from __future__ import annotations

import copy
import logging
import os
import pathlib
import tempfile
import typing

import pkginfo
import pyproject_hooks
import tomlkit
from packaging.metadata import Metadata
from packaging.requirements import Requirement

from . import build_environment, external_commands, overrides, requirements_file

if typing.TYPE_CHECKING:
    from . import context

logger = logging.getLogger(__name__)

BUILD_SYSTEM_REQ_FILE_NAME = "build-system-requirements.txt"
BUILD_BACKEND_REQ_FILE_NAME = "build-backend-requirements.txt"
BUILD_SDIST_REQ_FILE_NAME = "build-sdist-requirements.txt"


def get_build_system_dependencies(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
) -> set[Requirement]:
    logger.info(f"getting build system dependencies for {req} in {sdist_root_dir}")
    pbi = ctx.package_build_info(req)

    build_system_req_file = sdist_root_dir.parent / BUILD_SYSTEM_REQ_FILE_NAME
    if build_system_req_file.exists():
        logger.info(
            f"loading build system dependencies from {build_system_req_file.name}"
        )
        return _read_requirements_file(build_system_req_file)

    orig_deps = overrides.find_and_invoke(
        req.name,
        "get_build_system_dependencies",
        default_get_build_system_dependencies,
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
        build_dir=pbi.build_dir(sdist_root_dir),
    )
    deps = _filter_requirements(req, orig_deps)

    _write_requirements_file(
        deps,
        build_system_req_file,
    )
    return deps


def _filter_requirements(
    req: Requirement,
    requirements: typing.Iterable[Requirement | str],
) -> set[Requirement]:
    requires = set()
    for r in requirements:
        if not isinstance(r, Requirement):
            r = Requirement(r)
        if requirements_file.evaluate_marker(req, r, req.extras):
            requires.add(r)
        else:
            logger.debug(
                f"evaluated {r} in the context of {req} and ignored because the environment marker does not match"
            )
    return requires


def default_get_build_system_dependencies(
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
    build_dir: pathlib.Path,
) -> typing.Iterable[str]:
    """Get build system requirements

    Defaults to ``[build-system] requires`` from ``pyproject.toml``.
    """
    pyproject_toml = get_pyproject_contents(build_dir)
    return typing.cast(list[str], get_build_backend(pyproject_toml)["requires"])


def get_build_backend_dependencies(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
    build_env: build_environment.BuildEnvironment,
) -> set[Requirement]:
    logger.info(f"getting build backend dependencies for {req} in {sdist_root_dir}")
    pbi = ctx.package_build_info(req)

    build_backend_req_file = sdist_root_dir.parent / BUILD_BACKEND_REQ_FILE_NAME
    if build_backend_req_file.exists():
        logger.info(
            f"loading build backend dependencies from {build_backend_req_file.name}"
        )
        return _read_requirements_file(build_backend_req_file)

    extra_environ = pbi.get_extra_environ(build_env=build_env)
    orig_deps = overrides.find_and_invoke(
        req.name,
        "get_build_backend_dependencies",
        default_get_build_backend_dependencies,
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
        build_dir=pbi.build_dir(sdist_root_dir),
        extra_environ=extra_environ,
        build_env=build_env,
    )
    deps = _filter_requirements(req, orig_deps)

    _write_requirements_file(
        deps,
        build_backend_req_file,
    )
    return deps


def default_get_build_backend_dependencies(
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
    build_dir: pathlib.Path,
    extra_environ: dict[str, str],
    build_env: build_environment.BuildEnvironment,
) -> typing.Iterable[str]:
    """Get build backend dependencies

    Defaults to result of hook call
    :meth:`~pyproject_hooks.BuildBackendHookCaller.get_requires_for_build_wheel`
    """
    pbi = ctx.package_build_info(req)
    hook_caller = get_build_backend_hook_caller(
        ctx=ctx,
        req=req,
        build_dir=build_dir,
        override_environ=extra_environ,
        build_env=build_env,
    )
    return hook_caller.get_requires_for_build_wheel(
        config_settings=pbi.config_settings,
    )


def get_build_sdist_dependencies(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
    build_env: build_environment.BuildEnvironment,
) -> set[Requirement]:
    logger.info(f"getting build sdist dependencies for {req} in {sdist_root_dir}")
    pbi = ctx.package_build_info(req)

    build_sdist_req_file = sdist_root_dir.parent / BUILD_SDIST_REQ_FILE_NAME
    if build_sdist_req_file.exists():
        logger.info(
            f"loading build sdist dependencies from {build_sdist_req_file.name}"
        )
        return _read_requirements_file(build_sdist_req_file)

    extra_environ = pbi.get_extra_environ(build_env=build_env)
    orig_deps = overrides.find_and_invoke(
        req.name,
        "get_build_sdist_dependencies",
        default_get_build_sdist_dependencies,
        ctx=ctx,
        req=req,
        sdist_root_dir=sdist_root_dir,
        build_dir=pbi.build_dir(sdist_root_dir),
        extra_environ=extra_environ,
        build_env=build_env,
    )
    deps = _filter_requirements(req, orig_deps)

    _write_requirements_file(
        deps,
        build_sdist_req_file,
    )
    return deps


def default_get_build_sdist_dependencies(
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
    build_dir: pathlib.Path,
    extra_environ: dict[str, str],
    build_env: build_environment.BuildEnvironment,
) -> typing.Iterable[str]:
    """Get build sdist dependencies

    Defaults to result of hook call
    :meth:`~pyproject_hooks.BuildBackendHookCaller.get_requires_for_build_wheel`
    """
    pbi = ctx.package_build_info(req)
    hook_caller = get_build_backend_hook_caller(
        ctx=ctx,
        req=req,
        build_dir=build_dir,
        override_environ=extra_environ,
        build_env=build_env,
    )
    return hook_caller.get_requires_for_build_wheel(
        config_settings=pbi.config_settings,
    )


def get_install_dependencies_of_sdist(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
    build_env: build_environment.BuildEnvironment,
) -> set[Requirement]:
    """Get install requirements (Requires-Dist) from sources

    Uses PEP 517 prepare_metadata_for_build_wheel() API.
    """
    pbi = ctx.package_build_info(req)
    build_dir = pbi.build_dir(sdist_root_dir)
    logger.info(f"getting install requirements for {req} from sdist in {build_dir}")
    extra_environ = pbi.get_extra_environ(build_env=build_env)
    hook_caller = get_build_backend_hook_caller(
        ctx=ctx,
        req=req,
        build_dir=build_dir,
        override_environ=extra_environ,
        build_env=build_env,
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        distinfo_name = hook_caller.prepare_metadata_for_build_wheel(
            tmp_dir,
            config_settings=pbi.config_settings,
        )
        metadata_file = pathlib.Path(tmp_dir) / distinfo_name / "METADATA"
        # ignore minor metadata issues
        metadata = parse_metadata(metadata_file, validate=False)

    if metadata.requires_dist is None:
        return set()
    else:
        return _filter_requirements(req, metadata.requires_dist)


def parse_metadata(metadata_file: pathlib.Path, *, validate: bool = True) -> Metadata:
    """Parse a dist-info/METADATA file

    The default parse mode is 'strict'. It even fails for a mismatch of field
    and core metadata version, e.g. a package with metadata 2.2 and
    license-expression field (added in 2.4).
    """
    return Metadata.from_email(metadata_file.read_bytes(), validate=validate)


def get_install_dependencies_of_wheel(
    req: Requirement, wheel_filename: pathlib.Path, requirements_file_dir: pathlib.Path
) -> set[Requirement]:
    logger.info(f"getting installation dependencies from {wheel_filename}")
    wheel = pkginfo.Wheel(str(wheel_filename))
    deps = _filter_requirements(req, wheel.requires_dist)
    _write_requirements_file(
        deps,
        requirements_file_dir / "requirements.txt",
    )
    return deps


def get_pyproject_contents(sdist_root_dir: pathlib.Path) -> dict[str, typing.Any]:
    pyproject_toml_filename = sdist_root_dir / "pyproject.toml"
    if not os.path.exists(pyproject_toml_filename):
        return {}
    return tomlkit.loads(pyproject_toml_filename.read_text())


# From pypa/build/src/build/__main__.py
_DEFAULT_BACKEND = {
    "build-backend": "setuptools.build_meta:__legacy__",
    "backend-path": None,
    "requires": ["setuptools >= 40.8.0"],
}


def get_build_backend(pyproject_toml: dict[str, typing.Any]) -> dict[str, typing.Any]:
    # Build a set of defaults. Use a copy to ensure that if anything
    # modifies the values returned by this function our defaults are
    # not changed.
    backend_settings = copy.deepcopy(_DEFAULT_BACKEND)

    # Override it with local settings. This allows for some projects
    # like pyarrow, that don't have 'build-backend' set but *do* have
    # 'requires' set.
    for key in ["build-backend", "backend-path", "requires"]:
        if key in pyproject_toml.get("build-system", {}):
            backend_settings[key] = pyproject_toml["build-system"][key]

    return backend_settings


def get_build_backend_hook_caller(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    build_dir: pathlib.Path,
    override_environ: dict[str, typing.Any],
    build_env: build_environment.BuildEnvironment,
) -> pyproject_hooks.BuildBackendHookCaller:
    """Create pyproject_hooks build backend caller"""

    def _run_hook_with_extra_environ(
        cmd: typing.Sequence[str],
        cwd: str | None = None,
        extra_environ: typing.Mapping[str, str] | None = None,
    ) -> None:
        """The BuildBackendHookCaller is going to pass extra_environ
        and our build system may want to set some values, too. The hook
        also needs env vars from the build environment's virtualenv. Merge
        the 3 sets of values before calling the actual runner function.
        """
        extra_environ = dict(extra_environ) if extra_environ else {}
        extra_environ.update(override_environ)
        extra_environ.update(build_env.get_venv_environ(template_env=extra_environ))
        external_commands.run(
            cmd,
            cwd=cwd,
            extra_environ=extra_environ,
            network_isolation=ctx.network_isolation,
        )

    pyproject_toml = get_pyproject_contents(build_dir)
    backend = get_build_backend(pyproject_toml)

    return pyproject_hooks.BuildBackendHookCaller(
        source_dir=str(build_dir),
        build_backend=backend["build-backend"],
        backend_path=backend["backend-path"],
        runner=_run_hook_with_extra_environ,
        python_executable=str(build_env.python),
    )


def _write_requirements_file(
    requirements: typing.Iterable[Requirement],
    filename: pathlib.Path,
) -> None:
    with open(filename, "w") as f:
        for r in requirements:
            f.write(f"{r}\n")


def _read_requirements_file(
    filename: pathlib.Path,
) -> set[Requirement]:
    lines = requirements_file.parse_requirements_file(filename)
    return set([Requirement(line) for line in lines])
