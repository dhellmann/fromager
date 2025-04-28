from __future__ import annotations

import logging
import os
import pathlib
import platform
import re
import subprocess
import sys
import typing
from io import TextIOWrapper

from packaging.requirements import Requirement

from . import dependencies, external_commands, metrics, resolver
from .requirements_file import RequirementType

if typing.TYPE_CHECKING:
    from . import context

logger = logging.getLogger(__name__)

# Pip has no API, so parse its output looking for what it couldn't install.
# Verbose regular expressions ignore blank spaces, so we have to escape those to
# have them recognized as being part of the pattern.
_pip_missing_dependency_pattern = re.compile(
    r"""(
    Could\ not\ find\ a\ version\ that\ satisfies\ the\ requirement\ (\w+)
    |
    No\ matching\ distribution\ found\ for\ (\w+)
    |
    ResolutionImpossible  # usually when constraints prevent a match
    )""",
    flags=re.VERBOSE,
)


class MissingDependency(Exception):  # noqa: N818
    def __init__(
        self,
        ctx: context.WorkContext,
        req_type: RequirementType,
        req: Requirement | str | None,
        all_reqs: typing.Iterable[Requirement],
    ):
        self.all_reqs = all_reqs
        resolutions = []
        for r in all_reqs:
            try:
                _, version = resolver.resolve(
                    ctx=ctx,
                    req=r,
                    sdist_server_url=resolver.PYPI_SERVER_URL,
                    req_type=req_type,
                )
            except Exception as err:
                resolutions.append(f"{r} -> {err}")
            else:
                resolutions.append(f"{r} -> {version}")
        formatted_reqs = "\n".join(resolutions)
        if req:
            msg = (
                f"Failed to install {req_type} dependency {req}. "
                f"Check all {req_type} dependencies:\n{formatted_reqs}"
            )
        else:
            msg = (
                f"Failed to install {req_type} dependency. "
                f"Check all {req_type} dependencies:\n{formatted_reqs}"
            )
        super().__init__(f"\n{'*' * 40}\n{msg}\n{'*' * 40}\n")


class BuildEnvironment:
    "Wrapper for a virtualenv used for build isolation."

    def __init__(
        self,
        ctx: context.WorkContext,
        parent_dir: pathlib.Path,
        build_requirements: typing.Iterable[Requirement] | None,
        req: Requirement,
    ):
        self._ctx = ctx
        self.path = parent_dir.absolute() / f"build-{platform.python_version()}"
        self._build_requirements = build_requirements
        self._req = req  # for logging
        self._createenv()

    @property
    def python(self) -> pathlib.Path:
        """Path to Python interpreter in virtual env"""
        return self.path / "bin" / "python3"

    def get_venv_environ(
        self, template_env: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Add virtual env to extra environ

        The method activates the virtualenv

        1. Put the build environment at the front of the :envvar:`PATH`
           to ensure any build tools are picked up from there and not global
           versions. If the caller has already set a path, start there.
        2. Set :envvar:`VIRTUAL_ENV` so tools looking for that (for example,
           maturin) find it.
        """
        venv_environ: dict[str, str] = {
            "VIRTUAL_ENV": str(self.path),
        }

        # pre-pend virtualenv's bin
        # 1) template_env PATH, 2) process' PATH, 3) default: "/usr/bin"
        envpath: str | None = None
        if template_env:
            envpath = template_env.get("PATH")
        if envpath is None:
            envpath = os.environ.get("PATH", "/usr/bin")
        envpath_list = envpath.split(os.pathsep)
        venv_bin = str(self.path / "bin")
        if envpath_list[0] != venv_bin:
            envpath_list.insert(0, venv_bin)
        venv_environ["PATH"] = os.pathsep.join(envpath_list)
        return venv_environ

    def run(
        self,
        cmd: typing.Sequence[str],
        *,
        cwd: str | None = None,
        extra_environ: dict[str, str] | None = None,
        network_isolation: bool | None = None,
        log_filename: str | None = None,
        stdin: TextIOWrapper | None = None,
    ) -> str:
        """Run command in a virtual environment

        `network_isolation` defaults to context setting.
        """
        extra_environ = extra_environ.copy() if extra_environ else {}
        extra_environ.update(self.get_venv_environ(template_env=extra_environ))

        # default from context
        if network_isolation is None:
            network_isolation = self._ctx.network_isolation
        if network_isolation:
            # Build Rust dependencies without network access
            extra_environ.setdefault("CARGO_NET_OFFLINE", "true")

        return external_commands.run(
            cmd,
            cwd=cwd,
            extra_environ=extra_environ,
            network_isolation=network_isolation,
            log_filename=log_filename,
            stdin=stdin,
        )

    def _createenv(self) -> None:
        if self.path.exists():
            logger.info("reusing build environment in %s", self.path)
            return

        logger.debug("creating build environment in %s", self.path)
        external_commands.run(
            [
                sys.executable,
                "-m",
                "virtualenv",
                "--python",
                sys.executable,
                "--pip=bundle",
                "--setuptools=none",
                "--no-periodic-update",
                "--no-download",
                str(self.path),
            ],
            network_isolation=self._ctx.network_isolation,
        )
        logger.info("created build environment in %s", self.path)
        if self._build_requirements:
            self.install(self._build_requirements)

    @property
    def _pip_cmd(self) -> list[str]:
        cmd = [
            str(self.python),
            "-m",
            "pip",
            "-vvv",
            "install",
            "--disable-pip-version-check",
            "--no-compile",  # don't compile byte code
            "--upgrade",
            "--only-binary",
            ":all:",
        ]
        cmd.extend(self._ctx.pip_constraint_args)
        cmd.extend(self._ctx.pip_wheel_server_args)
        return cmd

    def install(self, reqs: typing.Iterable[Requirement]) -> None:
        if not reqs:
            return
        req_filename = self.path / "requirements.txt"
        # FIXME: Ensure each requirement is pinned to a specific version.
        with open(req_filename, "w") as f:
            for r in reqs:
                f.write(f"{r}\n")
        cmd = self._pip_cmd + ["-r", str(req_filename.absolute())]
        self.run(
            cmd,
            cwd=str(self.path.parent),
            network_isolation=False,
        )
        logger.info(
            "installed dependencies %s into build environment in %s",
            reqs,
            self.path,
        )


@metrics.timeit(description="prepare build environment")
def prepare_build_environment(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
) -> BuildEnvironment:
    logger.info("preparing build environment")

    build_env = BuildEnvironment(
        ctx=ctx,
        parent_dir=sdist_root_dir.parent,
        build_requirements=None,
        req=req,
    )

    build_system_dependencies = dependencies.get_build_system_dependencies(
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
    )
    _safe_install(
        ctx=ctx,
        req=req,
        build_env=build_env,
        deps=build_system_dependencies,
        dep_req_type=RequirementType.BUILD_SYSTEM,
    )

    build_backend_dependencies = dependencies.get_build_backend_dependencies(
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
        build_env=build_env,
    )
    _safe_install(
        ctx=ctx,
        req=req,
        build_env=build_env,
        deps=build_backend_dependencies,
        dep_req_type=RequirementType.BUILD_BACKEND,
    )

    build_sdist_dependencies = dependencies.get_build_sdist_dependencies(
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
        build_env=build_env,
    )
    _safe_install(
        ctx=ctx,
        req=req,
        build_env=build_env,
        deps=build_sdist_dependencies,
        dep_req_type=RequirementType.BUILD_SDIST,
    )

    return build_env


def _safe_install(
    ctx: context.WorkContext,
    req: Requirement,
    build_env: BuildEnvironment,
    deps: typing.Iterable[Requirement],
    dep_req_type: RequirementType,
) -> None:
    if not deps:
        return
    logger.debug("installing %s %s", dep_req_type, deps)
    cmd = build_env._pip_cmd
    cmd.extend(str(dep) for dep in deps)

    try:
        external_commands.run(
            cmd,
            network_isolation=False,
        )
    except subprocess.CalledProcessError as err:
        logger.error(
            f"{req.name}: failed to install {dep_req_type} dependencies {deps}: {err}"
        )
        match = _pip_missing_dependency_pattern.search(err.output)
        if match is not None:
            req_info = match.groups()[0]
        else:
            req_info = None
        raise MissingDependency(
            ctx,
            dep_req_type,
            req_info,
            deps,
        ) from err

    logger.info("%s: installed %s requirements %s", req.name, dep_req_type, deps)
