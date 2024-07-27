import importlib.metadata
import logging
import operator
import os.path
import pathlib
import re
import shutil
import subprocess
import sys
import typing
import zipfile
from urllib.parse import urlparse

from packaging.requirements import Requirement

from . import (
    context,
    dependencies,
    external_commands,
    finders,
    overrides,
    requirements_file,
    server,
    sources,
    wheels,
)

logger = logging.getLogger(__name__)

# Pip has no API, so parse its output looking for what it couldn't
# install.
_pip_missing_dependency_pattern = re.compile(
    r"Could not find a version that satisfies the requirement (\w+)",
)


class MissingDependency(Exception):  # noqa: N818
    def __init__(
        self,
        ctx: context.WorkContext,
        req_type: str,
        req: Requirement,
        all_reqs: typing.Iterable[Requirement],
    ):
        self.missing_req = req
        self.all_reqs = all_reqs
        resolutions = []
        for r in all_reqs:
            try:
                url, version = sources.resolve_dist(ctx, r, sources.PYPI_SERVER_URL)
            except Exception as err:
                resolutions.append(f"{r} -> {err}")
            else:
                resolutions.append(f"{r} -> {version}")
        formatted_reqs = "\n".join(resolutions)
        msg = (
            f"Failed to install {req_type} dependency {req}. "
            f"Check all {req_type} dependencies:\n{formatted_reqs}"
        )
        super().__init__(f'\n{"*" * 40}\n{msg}\n{"*" * 40}\n')


def handle_requirement(
    ctx: context.WorkContext,
    req: Requirement,
    req_type: str = "toplevel",
    why: list | None = None,
) -> str:
    if why is None:
        why = []
    # If we're given a requirements file as input, we might be iterating over a
    # list of requirements with marker expressions that limit their use to
    # specific platforms or python versions. Evaluate the markers to filter out
    # anything we shouldn't build. Only apply the filter to toplevel
    # requirements (items without a why list leading up to them) because other
    # dependencies are already filtered based on their markers in the context of
    # their parent, so they include values like the parent's extras settings.
    if (not why) and (not requirements_file.evaluate_marker(req, req)):
        logger.info(
            f"{req.name}: ignoring {req_type} dependency {req} because of its marker expression"
        )
        return ""
    logger.info(
        f'{req.name}: {"*" * (len(why) + 1)} handling {req_type} requirement {req} {why}'
    )

    constraint = ctx.constraints.get_constraint(req.name)
    if constraint:
        logger.info(
            f"{req.name}: incoming requirement {req} matches constraint {constraint}. Will apply both."
        )

    pre_built = overrides.pkgname_to_override_module(
        req.name
    ) in ctx.settings.pre_built(ctx.variant)

    # Resolve the dependency and get either the pre-built wheel our
    # the source code.
    if not pre_built:
        source_filename, resolved_version, source_url, source_url_type = (
            sources.download_source(ctx, req, sources.DEFAULT_SDIST_SERVER_URLS)
        )

    else:
        logger.info(f"{req.name}: {req_type} requirement {req} uses a pre-built wheel")
        servers = [sources.PYPI_SERVER_URL]
        if ctx.wheel_server_url:
            servers.insert(0, ctx.wheel_server_url)
        wheel_filename, resolved_version, wheel_url = download_wheel(
            ctx, req, ctx.wheels_prebuilt, servers
        )
        # Remember that this is a prebuilt wheel, and where we got it.
        source_url = wheel_url
        source_url_type = "prebuilt"
        # Add the wheel to the mirror so it is available to anything
        # that needs to install it. We leave a copy in the prebuilt
        # directory to make it easy to remove the wheel from the
        # downloads directory before uploading to a proper package
        # index.
        dest_name = ctx.wheels_downloads / wheel_filename.name
        if not dest_name.exists():
            logger.info(f"{req.name}: updating temporary mirror with pre-built wheel")
            shutil.copy(wheel_filename, dest_name)
            server.update_wheel_mirror(ctx)
        unpack_dir = ctx.work_dir / f"{req.name}-{resolved_version}"
        if not unpack_dir.exists():
            unpack_dir.mkdir()

    # Avoid cyclic dependencies and redundant processing.
    if ctx.has_been_seen(req, resolved_version):
        logger.debug(
            f"{req.name}: redundant {req_type} requirement {why} -> {req} resolves to {resolved_version}"
        )
        return resolved_version
    ctx.mark_as_seen(req, resolved_version)

    logger.info(
        f"{req.name}: new {req_type} dependency {req} resolves to {resolved_version}"
    )

    # Build the dependency chain up to the point of this new
    # requirement using a new list so we can avoid modifying the list
    # we're given.
    why = why[:] + [(req_type, req, resolved_version)]

    # for cleanup
    build_env = None
    sdist_root_dir = None

    if not pre_built:
        find_sdist_result = finders.find_sdist(ctx.sdists_builds, req, resolved_version)
        try:
            sdist_root_dir = finders.find_source_dir(
                ctx.work_dir, req, resolved_version
            )
        except ValueError as err:
            logger.debug(
                f"{req.name}: did not find existing source dir, preparing source ({err})"
            )
            sdist_root_dir = None
        if find_sdist_result and sdist_root_dir:
            logger.debug(f"{req.name}: have source dir {sdist_root_dir}")
        else:
            sdist_root_dir = sources.prepare_source(
                ctx, req, source_filename, resolved_version
            )
        unpack_dir = sdist_root_dir.parent

        build_system_dependencies = _handle_build_system_requirements(
            ctx, req, why, sdist_root_dir
        )
        build_backend_dependencies = _handle_build_backend_requirements(
            ctx, req, why, sdist_root_dir
        )
        build_sdist_dependencies = _handle_build_sdist_requirements(
            ctx, req, why, sdist_root_dir
        )

    # Add the new package to the build order list before trying to
    # build it so we have a record of the dependency even if the build
    # fails.
    ctx.add_to_build_order(
        req_type=req_type,
        req=req,
        version=resolved_version,
        why=why,
        source_url=source_url,
        source_url_type=source_url_type,
        prebuilt=pre_built,
        constraint=constraint,
    )

    if not pre_built:
        # FIXME: This is a bit naive, but works for most wheels, including
        # our more expensive ones, and there's not a way to know the
        # actual name without doing most of the work to build the wheel.
        wheel_filename = finders.find_wheel(ctx.wheels_downloads, req, resolved_version)
        if wheel_filename:
            logger.info(
                f"{req.name}: have wheel version {resolved_version}: {wheel_filename}"
            )
        else:
            logger.info(
                f"{req.name}: preparing to build wheel for version {resolved_version}"
            )
            build_env = wheels.BuildEnvironment(
                ctx,
                sdist_root_dir.parent,
                build_system_dependencies
                | build_backend_dependencies
                | build_sdist_dependencies,
            )
            try:
                find_sdist_result = finders.find_sdist(
                    ctx.sdists_builds, req, resolved_version
                )
                if not find_sdist_result:
                    sources.build_sdist(
                        ctx=ctx,
                        req=req,
                        version=resolved_version,
                        sdist_root_dir=sdist_root_dir,
                        build_env=build_env,
                    )
                else:
                    logger.info(
                        f"{req.name} have sdist version {resolved_version}: {find_sdist_result}"
                    )
            except Exception as err:
                logger.warning(
                    f"{req.name}: failed to build source distribution: {err}"
                )
            built_filename = wheels.build_wheel(
                ctx=ctx,
                req=req,
                sdist_root_dir=sdist_root_dir,
                version=resolved_version,
                build_env=build_env,
            )
            server.update_wheel_mirror(ctx)
            # When we update the mirror, the built file moves to the
            # downloads directory.
            wheel_filename = ctx.wheels_downloads / built_filename.name
            logger.info(
                f"{req.name}: built wheel for version {resolved_version}: {wheel_filename}"
            )

    # Process installation dependencies for all wheels.
    next_req_type = "install"
    install_dependencies = dependencies.get_install_dependencies_of_wheel(
        req, wheel_filename, unpack_dir
    )

    for dep in _sort_requirements(install_dependencies):
        try:
            handle_requirement(ctx, dep, next_req_type, why)
        except Exception as err:
            raise ValueError(
                f"could not handle {next_req_type} dependency {dep} for {why}"
            ) from err

    # Cleanup the source tree and build environment, leaving any other
    # artifacts that were created.
    if ctx.cleanup:
        if sdist_root_dir and sdist_root_dir.exists():
            logger.debug(f"{req.name}: cleaning up source tree {sdist_root_dir}")
            shutil.rmtree(sdist_root_dir)
            # Recreate the directory as empty as a marker that we've processed
            # it once, to avoid processing it again.
            sdist_root_dir.mkdir(parents=True, exist_ok=True)
            logger.debug(f"{req.name}: cleaned up source tree {sdist_root_dir}")
        if build_env and build_env.path.exists():
            logger.debug(f"{req.name}: cleaning up build environment {build_env.path}")
            shutil.rmtree(build_env.path)
            logger.debug(f"{req.name}: cleaned up build environment {build_env.path}")

    return resolved_version


def _sort_requirements(
    requirements: typing.Iterable[Requirement],
) -> typing.Iterable[Requirement]:
    return sorted(requirements, key=operator.attrgetter("name"))


def download_wheel(
    ctx: context.WorkContext,
    req: Requirement,
    output_directory: pathlib.Path,
    wheel_server_urls: list[str],
) -> tuple[pathlib.Path, str, str]:
    wheel_url, resolved_version = _resolve_prebuilt_wheel(ctx, req, wheel_server_urls)
    wheel_filename = output_directory / os.path.basename(urlparse(wheel_url).path)
    if not wheel_filename.exists():
        logger.info(f"{req.name}: downloading pre-built wheel {wheel_url}")
        wheel_filename = _download_wheel_check(output_directory, wheel_url)
        logger.info(f"{req.name}: saved wheel to {wheel_filename}")
    else:
        logger.info(f"{req.name}: have existing wheel {wheel_filename}")

    return wheel_filename, resolved_version, wheel_url


# Helper method to check whether the .whl file is a zip file and has contents in it.
# It will throw BadZipFile exception if any other file is encountered. Eg: index.html
def _download_wheel_check(destination_dir, wheel_url):
    wheel_filename = sources.download_url(destination_dir, wheel_url)
    wheel_directory_contents = zipfile.ZipFile(wheel_filename).namelist()
    if not wheel_directory_contents:
        raise zipfile.BadZipFile(f"Empty zip file encountered: {wheel_filename}")

    return wheel_filename


def _resolve_prebuilt_wheel(
    ctx: context.WorkContext,
    req: Requirement,
    wheel_server_urls: list[str],
) -> tuple[str, str]:
    "Return URL to wheel and its version."
    for url in wheel_server_urls:
        try:
            wheel_url, resolved_version = sources.resolve_dist(
                ctx,
                req,
                url,
                include_sdists=False,
                include_wheels=True,
            )
        except Exception:
            continue
        if wheel_url and resolved_version:
            return (wheel_url, resolved_version)
    raise ValueError(
        f'Could not find a prebuilt wheel for {req} on {" or ".join(wheel_server_urls)}'
    )


def _handle_build_system_requirements(
    ctx: context.WorkContext,
    req: Requirement,
    why: list | None,
    sdist_root_dir: pathlib.Path,
) -> set[Requirement]:
    build_system_dependencies = dependencies.get_build_system_dependencies(
        ctx, req, sdist_root_dir
    )

    for dep in _sort_requirements(build_system_dependencies):
        try:
            resolved = handle_requirement(ctx, dep, "build-system", why)
        except Exception as err:
            raise ValueError(
                f"could not handle build-system dependency {dep} for {why}"
            ) from err
        # We may need these dependencies installed in order to run build hooks
        # Example: frozenlist build-system.requires includes expandvars because
        # it is used by the packaging/pep517_backend/ build backend
        _maybe_install(ctx, dep, "build-system", resolved)
    return build_system_dependencies


def _handle_build_backend_requirements(
    ctx: context.WorkContext,
    req: Requirement,
    why: list,
    sdist_root_dir: pathlib.Path,
) -> set[Requirement]:
    build_backend_dependencies = dependencies.get_build_backend_dependencies(
        ctx, req, sdist_root_dir
    )

    for dep in _sort_requirements(build_backend_dependencies):
        try:
            resolved = handle_requirement(ctx, dep, "build-backend", why)
        except Exception as err:
            raise ValueError(
                f"could not handle build-backend dependency {dep} for {why}"
            ) from err
        # Build backends are often used to package themselves, so in
        # order to determine their dependencies they may need to be
        # installed.
        _maybe_install(ctx, dep, "build-backend", resolved)
    return build_backend_dependencies


def _handle_build_sdist_requirements(
    ctx: context.WorkContext,
    req: Requirement,
    why: list | None,
    sdist_root_dir: pathlib.Path,
) -> set[Requirement]:
    build_sdist_dependencies = dependencies.get_build_sdist_dependencies(
        ctx, req, sdist_root_dir
    )

    for dep in _sort_requirements(build_sdist_dependencies):
        try:
            resolved = handle_requirement(ctx, dep, "build-sdist", why)
        except Exception as err:
            raise ValueError(
                f"could not handle build-sdist dependency {dep} for {why}"
            ) from err
        _maybe_install(ctx, dep, "build-sdist", resolved)
    return build_sdist_dependencies


def prepare_build_environment(
    ctx: context.WorkContext,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
) -> pathlib.Path:
    logger.info(f"{req.name}: preparing build environment")

    next_req_type = "build_system"
    build_system_dependencies = dependencies.get_build_system_dependencies(
        ctx, req, sdist_root_dir
    )

    for dep in build_system_dependencies:
        # We may need these dependencies installed in order to run build hooks
        # Example: frozenlist build-system.requires includes expandvars because
        # it is used by the packaging/pep517_backend/ build backend
        try:
            _maybe_install(ctx, dep, next_req_type, None)
        except Exception as err:
            logger.error(
                f"{req.name}: failed to install {next_req_type} dependency {dep}: {err}"
            )
            raise MissingDependency(
                ctx,
                next_req_type,
                dep,
                build_system_dependencies,
            ) from err

    next_req_type = "build_backend"
    build_backend_dependencies = dependencies.get_build_backend_dependencies(
        ctx, req, sdist_root_dir
    )

    for dep in build_backend_dependencies:
        # Build backends are often used to package themselves, so in
        # order to determine their dependencies they may need to be
        # installed.
        try:
            _maybe_install(ctx, dep, next_req_type, None)
        except Exception as err:
            logger.error(
                f"{req.name}: failed to install {next_req_type} dependency {dep}: {err}"
            )
            raise MissingDependency(
                ctx,
                next_req_type,
                dep,
                build_backend_dependencies,
            ) from err

    next_req_type = "build_sdist"
    build_sdist_dependencies = dependencies.get_build_sdist_dependencies(
        ctx, req, sdist_root_dir
    )

    for dep in build_sdist_dependencies:
        try:
            _maybe_install(ctx, dep, next_req_type, None)
        except Exception as err:
            logger.error(
                f"{req.name}: failed to install {next_req_type} dependency {dep}: {err}"
            )
            raise MissingDependency(
                ctx,
                next_req_type,
                dep,
                build_sdist_dependencies,
            ) from err

    try:
        build_env = wheels.BuildEnvironment(
            ctx,
            sdist_root_dir.parent,
            build_system_dependencies
            | build_backend_dependencies
            | build_sdist_dependencies,
        )
    except subprocess.CalledProcessError as err:
        # Pip has no API, so parse its output looking for what it
        # couldn't install. If we don't find something, just re-raise
        # the exception we already have.
        logger.error(f"{req.name}: failed to create build environment for {dep}: {err}")
        match = _pip_missing_dependency_pattern.search(err.output)
        if match:
            raise MissingDependency(
                ctx,
                "build",
                match.groups()[0],
                build_system_dependencies | build_backend_dependencies,
            ) from err
        raise
    return build_env.path


def _maybe_install(
    ctx: context.WorkContext,
    req: Requirement,
    req_type: str,
    resolved_version: str,
):
    "Install the package if it is not already installed."
    if resolved_version is not None:
        try:
            actual_version = importlib.metadata.version(req.name)
            if str(resolved_version) == actual_version:
                logger.debug(
                    f"{req.name}: already have {req.name} version {resolved_version} installed"
                )
                return
            logger.info(
                f"{req.name}: found {req.name} {actual_version} installed, updating to {resolved_version}"
            )
        except importlib.metadata.PackageNotFoundError as err:
            logger.debug(
                f"{req.name}: could not determine version of {req.name}, will install: {err}"
            )
    safe_install(ctx, req, req_type)


def safe_install(
    ctx: context.WorkContext,
    req: Requirement,
    req_type: str,
):
    logger.debug("installing %s %s", req_type, req)
    external_commands.run(
        [
            sys.executable,
            "-m",
            "pip",
            "-vvv",
            "install",
            "--disable-pip-version-check",
            "--upgrade",
            "--only-binary",
            ":all:",
        ]
        + ctx.pip_wheel_server_args
        + [
            f"{req}",
        ]
    )
    version = importlib.metadata.version(req.name)
    logger.info("installed %s %s using %s", req_type, req, version)
