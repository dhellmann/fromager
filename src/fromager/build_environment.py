from __future__ import annotations

import importlib.metadata
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
from .threading_utils import with_thread_lock

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

        req_filename = self.path / "requirements.txt"
        # FIXME: Ensure each requirement is pinned to a specific version.
        with open(req_filename, "w") as f:
            if self._build_requirements:
                for r in self._build_requirements:
                    f.write(f"{r}\n")
        if not self._build_requirements:
            return
        self.run(
            [
                str(self.python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-compile",  # don't compile byte code
                "--only-binary",
                ":all:",
            ]
            + self._ctx.pip_wheel_server_args
            + self._ctx.pip_constraint_args
            + [
                "-r",
                str(req_filename.absolute()),
            ],
            cwd=str(self.path.parent),
            network_isolation=False,
        )
        logger.info(
            "installed dependencies into build environment in %s",
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

    next_req_type = RequirementType.BUILD_SYSTEM
    build_system_dependencies = dependencies.get_build_system_dependencies(
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
    )

    for dep in build_system_dependencies:
        # We may need these dependencies installed in order to run build hooks
        # Example: frozenlist build-system.requires includes expandvars because
        # it is used by the packaging/pep517_backend/ build backend
        try:
            maybe_install(
                ctx=ctx, req=req, dep=dep, dep_version=None, dep_req_type=next_req_type
            )
        except Exception as err:
            logger.error(f"failed to install {next_req_type} dependency {dep}: {err}")
            raise MissingDependency(
                ctx,
                next_req_type,
                dep,
                build_system_dependencies,
            ) from err

    next_req_type = RequirementType.BUILD_BACKEND
    build_backend_dependencies = dependencies.get_build_backend_dependencies(
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
    )

    for dep in build_backend_dependencies:
        # Build backends are often used to package themselves, so in
        # order to determine their dependencies they may need to be
        # installed.
        try:
            maybe_install(
                ctx=ctx, req=req, dep=dep, dep_version=None, dep_req_type=next_req_type
            )
        except Exception as err:
            logger.error(f"failed to install {next_req_type} dependency {dep}: {err}")
            raise MissingDependency(
                ctx,
                next_req_type,
                dep,
                build_backend_dependencies,
            ) from err

    next_req_type = RequirementType.BUILD_SDIST
    build_sdist_dependencies = dependencies.get_build_sdist_dependencies(
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
    )

    for dep in build_sdist_dependencies:
        try:
            maybe_install(
                ctx=ctx, req=req, dep=dep, dep_version=None, dep_req_type=next_req_type
            )
        except Exception as err:
            logger.error(f"failed to install {next_req_type} dependency {dep}: {err}")
            raise MissingDependency(
                ctx,
                next_req_type,
                dep,
                build_sdist_dependencies,
            ) from err

    try:
        build_env = BuildEnvironment(
            ctx=ctx,
            parent_dir=sdist_root_dir.parent,
            build_requirements=(
                build_system_dependencies
                | build_backend_dependencies
                | build_sdist_dependencies
            ),
            req=req,
        )
    except subprocess.CalledProcessError as err:
        # Pip has no API, so parse its output looking for what it
        # couldn't install. If we don't find something, just re-raise
        # the exception we already have.
        logger.error(f"failed to create build environment for {dep}: {err}")
        logger.info(f"looking for pattern in {err.output!r}")
        match = _pip_missing_dependency_pattern.search(err.output)
        if match:
            raise MissingDependency(
                ctx,
                next_req_type,
                match.groups()[0],
                build_system_dependencies
                | build_backend_dependencies
                | build_sdist_dependencies,
            ) from err
        raise
    return build_env


@with_thread_lock()
def maybe_install(
    ctx: context.WorkContext,
    req: Requirement,
    dep: Requirement,
    dep_version: str | None,
    dep_req_type: RequirementType,
):
    "Install the package if it is not already installed."
    if dep_version is not None:
        try:
            actual_version = importlib.metadata.version(dep.name)
            if str(dep_version) == actual_version:
                logger.debug(f"already have {dep.name} version {dep_version} installed")
                return
            logger.info(
                f"found {dep.name} {actual_version} installed, updating to {dep_version}"
            )
            _safe_install(
                ctx, req, Requirement(f"{dep.name}=={dep_version}"), dep_req_type
            )
            return
        except importlib.metadata.PackageNotFoundError as err:
            logger.debug(
                f"{dep.name}: could not determine version of {dep.name}, will install: {err}"
            )
    _safe_install(ctx, req, dep, dep_req_type)


def _safe_install(
    ctx: context.WorkContext,
    req: Requirement,
    dep: Requirement,
    dep_req_type: RequirementType,
):
    logger.debug("installing %s %s", dep_req_type, dep)
    external_commands.run(
        [
            sys.executable,
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
        + ctx.pip_wheel_server_args
        + ctx.pip_constraint_args
        + [
            f"{dep}",
        ],
        network_isolation=False,
    )
    logger.info("installed %s requirement %s", dep_req_type, dep)
