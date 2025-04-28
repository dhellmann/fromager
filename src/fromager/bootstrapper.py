from __future__ import annotations

import json
import logging
import operator
import os
import pathlib
import shutil
import tempfile
import typing
import zipfile
from email.parser import BytesParser
from urllib.parse import urlparse

from packaging.requirements import Requirement
from packaging.utils import NormalizedName, canonicalize_name
from packaging.version import Version

from . import (
    build_environment,
    dependencies,
    finders,
    gitutils,
    hooks,
    progress,
    resolver,
    server,
    sources,
    wheels,
)
from .dependency_graph import DependencyGraph
from .log import requirement_ctxvar
from .requirements_file import RequirementType, SourceType

if typing.TYPE_CHECKING:
    from . import context

logger = logging.getLogger(__name__)

# package name, extras, version, sdist/wheel
SeenKey = tuple[NormalizedName, tuple[str, ...], str, typing.Literal["sdist", "wheel"]]


class Bootstrapper:
    def __init__(
        self,
        ctx: context.WorkContext,
        progressbar: progress.Progressbar | None = None,
        prev_graph: DependencyGraph | None = None,
        cache_wheel_server_url: str | None = None,
        sdist_only: bool = False,
    ) -> None:
        self.ctx = ctx
        self.progressbar = progressbar or progress.Progressbar(None)
        self.prev_graph = prev_graph
        self.cache_wheel_server_url = cache_wheel_server_url or ctx.wheel_server_url
        self.sdist_only = sdist_only
        self.why: list[tuple[RequirementType, Requirement, Version]] = []
        # Push items onto the stack as we start to resolve their
        # dependencies so at the end we have a list of items that need to
        # be built in order.
        self._build_stack: list[typing.Any] = []
        self._build_requirements: set[tuple[NormalizedName, str]] = set()

        # Track requirements we've seen before so we don't resolve the
        # same dependencies over and over and so we can break cycles in
        # the dependency list. The key is the requirements spec, rather
        # than the package, in case we do have multiple rules for the same
        # package.
        self._seen_requirements: set[SeenKey] = set()

        self._build_order_filename = self.ctx.work_dir / "build-order.json"

    def resolve_version(
        self,
        req: Requirement,
        req_type: RequirementType,
    ) -> tuple[str, Version]:
        """Resolve the version of a requirement.

        Returns the source URL and the version of the requirement.
        """
        pbi = self.ctx.package_build_info(req)
        if pbi.pre_built:
            wheel_url, resolved_version = self._resolve_prebuilt_with_history(
                req=req,
                req_type=req_type,
            )
            source_url = wheel_url
        else:
            source_url, resolved_version = self._resolve_source_with_history(
                req=req,
                req_type=req_type,
            )
        return source_url, resolved_version

    def bootstrap(self, req: Requirement, req_type: RequirementType) -> Version:
        constraint = self.ctx.constraints.get_constraint(req.name)
        if constraint:
            logger.info(
                f"incoming requirement {req} matches constraint {constraint}. Will apply both."
            )

        pbi = self.ctx.package_build_info(req)
        if pbi.pre_built:
            wheel_url, resolved_version = self.resolve_version(
                req=req,
                req_type=req_type,
            )
            source_url = wheel_url
        else:
            source_url, resolved_version = self.resolve_version(
                req=req,
                req_type=req_type,
            )

        self._add_to_graph(req, req_type, resolved_version, source_url)

        # Is bootstrap going to create a wheel or just an sdist?
        # Use fast sdist-only if flag is set, requirement is not a build
        # requirement, and wheel is not pre-built.
        build_sdist_only = (
            self.sdist_only and req_type.is_install_requirement and not pbi.pre_built
        )

        # Avoid cyclic dependencies and redundant processing.
        if self._has_been_seen(req, resolved_version, build_sdist_only):
            logger.debug(
                f"redundant {req_type} dependency {req} "
                f"({resolved_version}, sdist_only={build_sdist_only}) for {self._explain}"
            )
            return resolved_version
        self._mark_as_seen(req, resolved_version, build_sdist_only)

        logger.info(f"new {req_type} dependency {req} resolves to {resolved_version}")

        # Build the dependency chain up to the point of this new
        # requirement using a new list so we can avoid modifying the list
        # we're given.
        self.why.append((req_type, req, resolved_version))

        # for cleanup
        build_env: build_environment.BuildEnvironment | None = None
        sdist_root_dir: pathlib.Path | None = None
        cached_wheel_filename: pathlib.Path | None = None
        wheel_filename: pathlib.Path | None = None
        sdist_filename: pathlib.Path | None = None
        unpack_dir: pathlib.Path | None = None
        unpacked_cached_wheel: pathlib.Path | None = None

        source_url_type = sources.get_source_type(self.ctx, req)

        if pbi.pre_built:
            wheel_filename, unpack_dir = self._download_prebuilt(
                req=req,
                req_type=req_type,
                resolved_version=resolved_version,
                wheel_url=source_url,
            )
            # Remember that this is a prebuilt wheel, and where we got it.
            source_url_type = str(SourceType.PREBUILT)
        else:
            # Look a few places for an existing wheel that matches what we need,
            # using caches for locations where we might have built the wheel
            # before.

            # Check if we have previously built a wheel and still have it on the
            # local filesystem.
            if not wheel_filename and not cached_wheel_filename:
                cached_wheel_filename, unpacked_cached_wheel = (
                    self._look_for_existing_wheel(
                        req,
                        resolved_version,
                        self.ctx.wheels_build,
                    )
                )

            # Check if we have previously downloaded a wheel and still have it
            # on the local filesystem.
            if not wheel_filename and not cached_wheel_filename:
                cached_wheel_filename, unpacked_cached_wheel = (
                    self._look_for_existing_wheel(
                        req,
                        resolved_version,
                        self.ctx.wheels_downloads,
                    )
                )

            # Look for a wheel on the cache server and download it if there is
            # one.
            if not wheel_filename and not cached_wheel_filename:
                cached_wheel_filename, unpacked_cached_wheel = (
                    self._download_wheel_from_cache(req, resolved_version)
                )

            if not unpacked_cached_wheel:
                # We didn't find anything so we are going to have to build the
                # wheel in order to process its installation dependencies.
                source_filename = sources.download_source(
                    ctx=self.ctx,
                    req=req,
                    version=resolved_version,
                    download_url=source_url,
                )
                sdist_root_dir = sources.prepare_source(
                    ctx=self.ctx,
                    req=req,
                    source_filename=source_filename,
                    version=resolved_version,
                )
            else:
                sdist_root_dir = unpacked_cached_wheel

            assert sdist_root_dir is not None
            unpack_dir = sdist_root_dir.parent

            build_env = build_environment.BuildEnvironment(
                ctx=self.ctx,
                parent_dir=sdist_root_dir.parent,
                build_requirements=None,
                req=req,
            )

            # need to call this function irrespective of whether we had the wheel cached
            # so that the build dependencies can be bootstrapped
            self._prepare_build_dependencies(req, sdist_root_dir, build_env)

            if cached_wheel_filename:
                logger.debug(
                    f"getting install requirements from cached "
                    f"wheel {cached_wheel_filename.name}"
                )
                # prefer existing wheel even in sdist_only mode
                # skip building even if it is a non-fromager built wheel
                wheel_filename = cached_wheel_filename
                build_sdist_only = False
            elif build_sdist_only:
                # get install dependencies from sdist and pyproject_hooks (only top-level and install)
                logger.debug(
                    f"getting install requirements from sdist "
                    f"{req.name}=={resolved_version} ({req_type})"
                )
                wheel_filename = None
                sdist_filename = self._build_sdist(
                    req, resolved_version, sdist_root_dir, build_env
                )
            else:
                # build wheel (build requirements, full build mode)
                logger.debug(
                    f"building wheel {req.name}=={resolved_version} "
                    f"to get install requirements ({req_type})"
                )
                wheel_filename, sdist_filename = self._build_wheel(
                    req, resolved_version, sdist_root_dir, build_env
                )

        hooks.run_post_bootstrap_hooks(
            ctx=self.ctx,
            req=req,
            dist_name=canonicalize_name(req.name),
            dist_version=str(resolved_version),
            sdist_filename=sdist_filename,
            wheel_filename=wheel_filename,
        )

        if build_sdist_only:
            if typing.TYPE_CHECKING:
                assert build_env is not None
                assert sdist_root_dir is not None
                assert wheel_filename is None

            install_dependencies = dependencies.get_install_dependencies_of_sdist(
                ctx=self.ctx,
                req=req,
                sdist_root_dir=sdist_root_dir,
                build_env=build_env,
            )
        else:
            if typing.TYPE_CHECKING:
                assert wheel_filename is not None
                assert unpack_dir is not None

            install_dependencies = dependencies.get_install_dependencies_of_wheel(
                req=req,
                wheel_filename=wheel_filename,
                requirements_file_dir=unpack_dir,
            )

        self._add_to_build_order(
            req=req,
            version=resolved_version,
            source_url=source_url,
            source_url_type=source_url_type,
            prebuilt=pbi.pre_built,
            constraint=constraint,
        )
        if req_type.is_build_requirement:
            # install dependencies of build requirements are also build
            # system requirements.
            child_req_type = RequirementType.BUILD_SYSTEM
        else:
            # top-level and install requirements
            child_req_type = RequirementType.INSTALL

        self.progressbar.update_total(len(install_dependencies))
        for dep in self._sort_requirements(install_dependencies):
            token = requirement_ctxvar.set(dep)
            try:
                self.bootstrap(req=dep, req_type=child_req_type)
            except Exception as err:
                raise ValueError(f"could not handle {self._explain}") from err
            finally:
                requirement_ctxvar.reset(token)
            self.progressbar.update()

        # we are done processing this req, so lets remove it from the why chain
        self.why.pop()
        self._cleanup(req, sdist_root_dir, build_env)
        return resolved_version

    @property
    def _explain(self) -> str:
        """Return message formatting current version of why stack."""
        return " for ".join(
            f"{req_type} dependency {req} ({resolved_version})"
            for req_type, req, resolved_version in reversed(self.why)
        )

    def _build_sdist(
        self,
        req: Requirement,
        resolved_version: Version,
        sdist_root_dir: pathlib.Path,
        build_env: build_environment.BuildEnvironment,
    ) -> pathlib.Path:
        try:
            find_sdist_result = finders.find_sdist(
                self.ctx, self.ctx.sdists_builds, req, str(resolved_version)
            )
            if not find_sdist_result:
                sdist_filename = sources.build_sdist(
                    ctx=self.ctx,
                    req=req,
                    version=resolved_version,
                    sdist_root_dir=sdist_root_dir,
                    build_env=build_env,
                )
            else:
                sdist_filename = find_sdist_result
                logger.info(
                    f"have sdist version {resolved_version}: {find_sdist_result}"
                )
        except Exception as err:
            logger.warning(f"failed to build source distribution: {err}")
            raise
        return sdist_filename

    def _build_wheel(
        self,
        req: Requirement,
        resolved_version: Version,
        sdist_root_dir: pathlib.Path,
        build_env: build_environment.BuildEnvironment,
    ) -> tuple[pathlib.Path, pathlib.Path]:
        sdist_filename = self._build_sdist(
            req, resolved_version, sdist_root_dir, build_env
        )

        logger.info(f"starting build of {self._explain} for {self.ctx.variant}")
        built_filename = wheels.build_wheel(
            ctx=self.ctx,
            req=req,
            sdist_root_dir=sdist_root_dir,
            version=resolved_version,
            build_env=build_env,
        )
        server.update_wheel_mirror(self.ctx)
        # When we update the mirror, the built file moves to the
        # downloads directory.
        wheel_filename = self.ctx.wheels_downloads / built_filename.name
        logger.info(f"built wheel for version {resolved_version}: {wheel_filename}")
        return wheel_filename, sdist_filename

    def _prepare_build_dependencies(
        self,
        req: Requirement,
        sdist_root_dir: pathlib.Path,
        build_env: build_environment.BuildEnvironment,
    ) -> set[Requirement]:
        # build system
        build_system_dependencies = dependencies.get_build_system_dependencies(
            ctx=self.ctx,
            req=req,
            sdist_root_dir=sdist_root_dir,
        )
        self._handle_build_requirements(
            req,
            RequirementType.BUILD_SYSTEM,
            build_system_dependencies,
        )
        # The next hooks need build system requirements.
        build_env.install(build_system_dependencies)

        # build backend
        build_backend_dependencies = dependencies.get_build_backend_dependencies(
            ctx=self.ctx,
            req=req,
            sdist_root_dir=sdist_root_dir,
            build_env=build_env,
        )
        self._handle_build_requirements(
            req,
            RequirementType.BUILD_BACKEND,
            build_backend_dependencies,
        )

        # build sdist
        build_sdist_dependencies = dependencies.get_build_sdist_dependencies(
            ctx=self.ctx,
            req=req,
            sdist_root_dir=sdist_root_dir,
            build_env=build_env,
        )
        self._handle_build_requirements(
            req,
            RequirementType.BUILD_SDIST,
            build_sdist_dependencies,
        )

        build_dependencies = build_sdist_dependencies | build_backend_dependencies
        if build_dependencies.isdisjoint(build_system_dependencies):
            build_env.install(build_dependencies)

        return (
            build_system_dependencies
            | build_backend_dependencies
            | build_sdist_dependencies
        )

    def _handle_build_requirements(
        self,
        req: Requirement,
        build_type: RequirementType,
        build_dependencies: set[Requirement],
    ) -> None:
        self.progressbar.update_total(len(build_dependencies))

        for dep in self._sort_requirements(build_dependencies):
            token = requirement_ctxvar.set(dep)
            try:
                self.bootstrap(req=dep, req_type=build_type)
            except Exception as err:
                requirement_ctxvar.reset(token)
                raise ValueError(f"could not handle {self._explain}") from err
            self.progressbar.update()
            requirement_ctxvar.reset(token)

    def _download_prebuilt(
        self,
        req: Requirement,
        req_type: RequirementType,
        resolved_version: Version,
        wheel_url: str,
    ) -> tuple[pathlib.Path, pathlib.Path]:
        logger.info(f"{req_type} requirement {req} uses a pre-built wheel")

        wheel_filename = wheels.download_wheel(req, wheel_url, self.ctx.wheels_prebuilt)

        # Add the wheel to the mirror so it is available to anything
        # that needs to install it. We leave a copy in the prebuilt
        # directory to make it easy to remove the wheel from the
        # downloads directory before uploading to a proper package
        # index.
        dest_name = self.ctx.wheels_downloads / wheel_filename.name
        if not dest_name.exists():
            logger.info("updating temporary mirror with pre-built wheel")
            shutil.copy(wheel_filename, dest_name)
            server.update_wheel_mirror(self.ctx)
        unpack_dir = self._create_unpack_dir(req, resolved_version)
        return (wheel_filename, unpack_dir)

    def _look_for_existing_wheel(
        self,
        req: Requirement,
        resolved_version: Version,
        search_in: pathlib.Path,
    ) -> tuple[pathlib.Path | None, pathlib.Path | None]:
        pbi = self.ctx.package_build_info(req)
        expected_build_tag = pbi.build_tag(resolved_version)
        logger.info(
            f"looking for existing wheel for version {resolved_version} with build tag {expected_build_tag} in {search_in}"
        )
        wheel_filename = finders.find_wheel(
            downloads_dir=search_in,
            req=req,
            dist_version=str(resolved_version),
            build_tag=expected_build_tag,
        )
        if not wheel_filename:
            return None, None

        _, _, build_tag, _ = wheels.extract_info_from_wheel_file(req, wheel_filename)
        if expected_build_tag and expected_build_tag != build_tag:
            logger.info(
                f"found wheel for {resolved_version} in {wheel_filename} but build tag does not match. Got {build_tag} but expected {expected_build_tag}"
            )
            return None, None

        logger.info(f"found existing wheel {wheel_filename}")
        metadata_dir = self._unpack_metadata_from_wheel(
            req, resolved_version, wheel_filename
        )
        return wheel_filename, metadata_dir

    def _download_wheel_from_cache(
        self, req: Requirement, resolved_version: Version
    ) -> tuple[pathlib.Path | None, pathlib.Path | None]:
        if not self.cache_wheel_server_url:
            return None, None
        logger.info(
            f"checking if wheel was already uploaded to {self.cache_wheel_server_url}"
        )
        try:
            wheel_url, _ = resolver.resolve(
                ctx=self.ctx,
                req=Requirement(f"{req.name}=={resolved_version}"),
                sdist_server_url=self.cache_wheel_server_url,
                include_sdists=False,
                include_wheels=True,
            )
            wheelfile_name = pathlib.Path(urlparse(wheel_url).path)
            pbi = self.ctx.package_build_info(req)
            expected_build_tag = pbi.build_tag(resolved_version)
            # Log the expected build tag for debugging
            logger.info(f"has expected build tag {expected_build_tag}")
            # Get changelogs for debug info
            changelogs = pbi.get_changelog(resolved_version)
            logger.debug(f"{req.name} has change logs {changelogs}")

            dist_name, dist_version, build_tag, _ = wheels.extract_info_from_wheel_file(
                req, wheelfile_name
            )
            if expected_build_tag and expected_build_tag != build_tag:
                logger.info(
                    f"found wheel for {resolved_version} in cache but build tag does not match. Got {build_tag} but expected {expected_build_tag}"
                )
                return None, None

            cached_wheel = wheels.download_wheel(
                req=req, wheel_url=wheel_url, output_directory=self.ctx.wheels_downloads
            )
            if self.cache_wheel_server_url != self.ctx.wheel_server_url:
                # Only update the local server if we actually downloaded
                # something from a different server.
                server.update_wheel_mirror(self.ctx)
            logger.info("found built wheel on cache server")
            unpack_dir = self._unpack_metadata_from_wheel(
                req, resolved_version, cached_wheel
            )
            return cached_wheel, unpack_dir
        except Exception:
            logger.info(
                f"did not find wheel for {resolved_version} in {self.cache_wheel_server_url}"
            )
            return None, None

    def _unpack_metadata_from_wheel(
        self, req: Requirement, resolved_version: Version, wheel_filename: pathlib.Path
    ) -> pathlib.Path | None:
        dist_name, dist_version, build_tag, _ = wheels.extract_info_from_wheel_file(
            req,
            wheel_filename,
        )
        unpack_dir = self._create_unpack_dir(req, resolved_version)
        dist_filename = f"{dist_name}-{dist_version}"
        metadata_dir = pathlib.Path(f"{dist_filename}.dist-info")
        try:
            archive = zipfile.ZipFile(wheel_filename)
            for filename in [
                dependencies.BUILD_BACKEND_REQ_FILE_NAME,
                dependencies.BUILD_SDIST_REQ_FILE_NAME,
                dependencies.BUILD_SYSTEM_REQ_FILE_NAME,
            ]:
                zipinfo = archive.getinfo(
                    str(metadata_dir / f"{wheels.FROMAGER_BUILD_REQ_PREFIX}-{filename}")
                )
                # Check for path traversal attempts
                if os.path.isabs(zipinfo.filename) or ".." in zipinfo.filename:
                    raise ValueError(f"Unsafe path in wheel: {zipinfo.filename}")
                zipinfo.filename = filename
                output_file = archive.extract(zipinfo, unpack_dir)
                logger.info(f"extracted {output_file}")

            logger.info(f"extracted build requirements from wheel into {unpack_dir}")
            return unpack_dir
        except Exception as e:
            # implies that the wheel server hosted non-fromager built wheels
            logger.info(f"could not extract build requirements from wheel: {e}")
            shutil.rmtree(unpack_dir)
            return None

    def _resolve_source_with_history(
        self,
        req: Requirement,
        req_type: RequirementType,
    ) -> tuple[str, Version]:
        if req.url:
            # If we have a URL, we should use that source. For now we only
            # support git clone URLs of some sort. We are given the directory
            # where the cloned repo resides, and return that as the URL for the
            # source code so the next step in the process can find it and
            # operate on it. However, we only support that if the package is a
            # top-level dependency.
            if req_type != RequirementType.TOP_LEVEL:
                raise ValueError(
                    f"{req} includes a URL, but is not a top-level dependency"
                )
            logger.info("resolving source via URL, ignoring any plugins")
            return self._resolve_version_from_git_url(req=req)

        cached_resolution = self._resolve_from_graph(
            req=req,
            req_type=req_type,
            pre_built=False,
        )
        if cached_resolution:
            source_url, resolved_version = cached_resolution
            logger.debug(f"resolved from previous bootstrap to {resolved_version}")
        else:
            source_url, resolved_version = sources.resolve_source(
                ctx=self.ctx,
                req=req,
                sdist_server_url=resolver.PYPI_SERVER_URL,
                req_type=req_type,
            )
        return (source_url, resolved_version)

    def _resolve_version_from_git_url(self, req: Requirement) -> tuple[str, Version]:
        "Return path to the cloned git repository and the package version."

        if not req.url:
            raise ValueError(f"unable to resolve from URL with no URL in {req}")

        if not req.url.startswith("git+"):
            raise ValueError(f"unable to handle URL scheme in {req.url} from {req}")

        # We start by not knowing where we would put the source because we don't
        # know the version.
        working_src_dir: pathlib.Path | None = None
        version: Version | None = None

        # Clean up the URL so we can parse it
        reduced_url = req.url[len("git+") :]
        parsed_url = urlparse(reduced_url)

        # Save the URL that we think we will use for cloning. This might change
        # later if the path has a tag or branch in it.
        url_to_clone = reduced_url
        need_to_clone = False

        # If the URL includes an @ with text after it, we use that as the reference
        # to clone, but by default we take the default branch.
        git_ref: str | None = None

        if "@" not in parsed_url.path:
            # If we have no reference, we know we are going to have to clone the
            # repository to figure out the version to use.
            logger.debug("no reference in URL, will clone")
            need_to_clone = True
        else:
            # If we have a reference, it might be a valid python version string, or
            # not. It _must_ be a valid git reference. If it can be parsed as a
            # valid python version, we assume the tag points to source that will
            # think that is its version, so we allow reusing an existing cloned repo
            # if there is one.
            new_path, _, git_ref = parsed_url.path.rpartition("@")
            url_to_clone = parsed_url._replace(path=new_path).geturl()
            try:
                version = Version(git_ref)
            except ValueError:
                logger.info(
                    "could not parse %r as a version, cloning to get the version",
                    git_ref,
                )
                need_to_clone = True
            else:
                logger.info("URL %s includes version %s", req.url, version)
                working_src_dir = (
                    self.ctx.work_dir
                    / f"{req.name}-{version}"
                    / f"{req.name}-{version}"
                )
                if not working_src_dir.exists():
                    need_to_clone = True
                else:
                    if self.ctx.cleanup:
                        logger.debug("cleaning up %s to reclone", working_src_dir)
                        shutil.rmtree(working_src_dir)
                        need_to_clone = True
                    else:
                        logger.info("reusing %s", working_src_dir)

        if need_to_clone:
            with tempfile.TemporaryDirectory() as tmpdir:
                clone_dir = pathlib.Path(tmpdir) / "src"
                gitutils.git_clone(
                    ctx=self.ctx,
                    req=req,
                    output_dir=clone_dir,
                    repo_url=url_to_clone,
                    ref=git_ref,
                )
                if not version:
                    # If we still do not have a version, get it from the package
                    # metadata.
                    version = self._get_version_from_package_metadata(req, clone_dir)
                    logger.info("found version %s", version)
                    working_src_dir = (
                        self.ctx.work_dir
                        / f"{req.name}-{version}"
                        / f"{req.name}-{version}"
                    )
                    if working_src_dir.exists():
                        # We have to check if the destination directory exists
                        # because if we were not given a version we did not
                        # clean it up earlier. We do not use ctx.cleanup to
                        # control this action because we cannot trust that the
                        # destination directory is reusable because we have had
                        # to compute the version and we cannot be sure that the
                        # version is dynamic. Two different commits in the repo
                        # could have the same version if that version is set
                        # with static data in the repo instead of via a tag or
                        # dynamically computed by something like setuptools-scm.
                        logger.debug("cleaning up %s", working_src_dir)
                        shutil.rmtree(working_src_dir)
                        need_to_clone = True
                    working_src_dir.parent.mkdir(parents=True, exist_ok=True)
                logger.info("moving cloned repo to %s", working_src_dir)
                shutil.move(clone_dir, str(working_src_dir))

        if not version:
            raise ValueError(f"unable to determine version for {req}")

        if not working_src_dir:
            raise ValueError(f"unable to determine working source directory for {req}")

        logging.info("resolved from git URL to %s, %s", working_src_dir, version)
        return (str(working_src_dir), version)

    def _get_version_from_package_metadata(
        self,
        req: Requirement,
        source_dir: pathlib.Path,
    ) -> Version:
        pbi = self.ctx.package_build_info(req)
        build_dir = pbi.build_dir(source_dir)

        logger.info(
            "preparing build dependencies so we can access the metadata to get the version"
        )
        build_dependencies = self._prepare_build_dependencies(req, source_dir)
        build_environment.BuildEnvironment(
            ctx=self.ctx,
            parent_dir=source_dir.parent,
            build_requirements=build_dependencies,
            req=req,
        )

        logger.info("generating metadata to get version")
        hook_caller = dependencies.get_build_backend_hook_caller(
            ctx=self.ctx,
            req=req,
            build_dir=build_dir,
            override_environ={},
        )
        metadata_dir_base = hook_caller.prepare_metadata_for_build_wheel(
            metadata_directory=str(source_dir.parent),
            config_settings=pbi.config_settings,
        )
        metadata_filename = source_dir.parent / metadata_dir_base / "METADATA"
        with open(metadata_filename, "rb") as f:
            p = BytesParser()
            metadata = p.parse(f, headersonly=True)
        return Version(metadata["Version"])

    def _resolve_prebuilt_with_history(
        self,
        req: Requirement,
        req_type: RequirementType,
    ) -> tuple[str, Version]:
        cached_resolution = self._resolve_from_graph(
            req=req,
            req_type=req_type,
            pre_built=True,
        )

        if cached_resolution and not req.url:
            wheel_url, resolved_version = cached_resolution
            logger.debug(f"resolved from previous bootstrap to {resolved_version}")
        else:
            servers = wheels.get_wheel_server_urls(self.ctx, req)
            wheel_url, resolved_version = wheels.resolve_prebuilt_wheel(
                ctx=self.ctx, req=req, wheel_server_urls=servers, req_type=req_type
            )
        return (wheel_url, resolved_version)

    def _resolve_from_graph(
        self,
        req: Requirement,
        req_type: RequirementType,
        pre_built: bool,
    ) -> tuple[str, Version] | None:
        _, parent_req, _ = self.why[-1] if self.why else (None, None, None)

        if not self.prev_graph:
            return None

        seen_version: set[str] = set()

        # first perform resolution using the top level reqs before looking at history
        possible_versions_in_top_level: list[tuple[str, Version]] = []
        for (
            top_level_edge
        ) in self.ctx.dependency_graph.get_root_node().get_outgoing_edges(
            req.name, RequirementType.TOP_LEVEL
        ):
            possible_versions_in_top_level.append(
                (
                    top_level_edge.destination_node.download_url,
                    top_level_edge.destination_node.version,
                )
            )
            seen_version.add(str(top_level_edge.destination_node.version))

        resolver_result = self._resolve_from_version_source(
            possible_versions_in_top_level, req
        )
        if resolver_result:
            return resolver_result

        # only if there is nothing in top level reqs, resolve using history
        possible_versions_from_graph: list[tuple[str, Version]] = []
        # check all nodes which have the same parent name irrespective of the parent's version
        for parent_node in self.prev_graph.get_nodes_by_name(
            parent_req.name if parent_req else None
        ):
            # if the edge matches the current req and type then it is a possible candidate
            # filtering on type might not be necessary, but we are being safe here. This will
            # for sure ensure that bootstrap takes the same route as it did in the previous one.
            # If we don't filter by type then it might pick up a different version from a different
            # type that should have appeared much later in the resolution process.
            for edge in parent_node.get_outgoing_edges(req.name, req_type):
                if (
                    edge.destination_node.pre_built == pre_built
                    and str(edge.destination_node.version) not in seen_version
                ):
                    possible_versions_from_graph.append(
                        (
                            edge.destination_node.download_url,
                            edge.destination_node.version,
                        )
                    )
                    seen_version.add(str(edge.destination_node.version))

        return self._resolve_from_version_source(possible_versions_from_graph, req)

    def _resolve_from_version_source(
        self,
        version_source: list[tuple[str, Version]],
        req: Requirement,
    ) -> tuple[str, Version] | None:
        if not version_source:
            return None
        try:
            # no need to pass req type to enable caching since we are already using the graph as our cache
            provider = resolver.GenericProvider(
                version_source=lambda x, y, z: version_source,
                constraints=self.ctx.constraints,
            )
            return resolver.resolve_from_provider(provider, req)
        except Exception as err:
            logger.debug(f"could not resolve {req} from {version_source}: {err}")
            return None

    def _create_unpack_dir(self, req: Requirement, resolved_version: Version):
        unpack_dir = self.ctx.work_dir / f"{req.name}-{resolved_version}"
        if not unpack_dir.exists():
            unpack_dir.mkdir()
        return unpack_dir

    def _cleanup(
        self,
        req: Requirement,
        sdist_root_dir: pathlib.Path | None,
        build_env: build_environment.BuildEnvironment | None,
    ) -> None:
        if not self.ctx.cleanup:
            return

        # Cleanup the source tree and build environment, leaving any other
        # artifacts that were created.
        if sdist_root_dir and sdist_root_dir.exists():
            logger.debug(f"cleaning up source tree {sdist_root_dir}")
            shutil.rmtree(sdist_root_dir)
            logger.debug(f"cleaned up source tree {sdist_root_dir}")
        if build_env and build_env.path.exists():
            logger.debug(f"cleaning up build environment {build_env.path}")
            shutil.rmtree(build_env.path)
            logger.debug(f"cleaned up build environment {build_env.path}")

    def _add_to_graph(
        self,
        req: Requirement,
        req_type: RequirementType,
        req_version: Version,
        download_url: str,
    ) -> None:
        if req_type == RequirementType.TOP_LEVEL:
            return

        _, parent_req, parent_version = self.why[-1] if self.why else (None, None, None)
        pbi = self.ctx.package_build_info(req)
        # Update the dependency graph after we determine that this requirement is
        # useful but before we determine if it is redundant so that we capture all
        # edges to use for building a valid constraints file.
        self.ctx.dependency_graph.add_dependency(
            parent_name=canonicalize_name(parent_req.name) if parent_req else None,
            parent_version=parent_version,
            req_type=req_type,
            req=req,
            req_version=req_version,
            download_url=download_url,
            pre_built=pbi.pre_built,
        )
        self.ctx.write_to_graph_to_file()

    def _sort_requirements(
        self,
        requirements: typing.Iterable[Requirement],
    ) -> typing.Iterable[Requirement]:
        return sorted(requirements, key=operator.attrgetter("name"))

    def _resolved_key(
        self, req: Requirement, version: Version, typ: typing.Literal["sdist", "wheel"]
    ) -> SeenKey:
        return (
            canonicalize_name(req.name),
            tuple(sorted(req.extras)),
            str(version),
            typ,
        )

    def _mark_as_seen(
        self,
        req: Requirement,
        version: Version,
        sdist_only: bool = False,
    ) -> None:
        """Track sdist and wheel builds

        A sdist-only build just contains as an sdist.
        A wheel build counts as wheel and sdist, because the presence of a
        either implies we have built a wheel from an sdist or we have a
        prebuilt wheel that will never have an sdist.
        """
        # Mark sdist seen for sdist-only build and wheel build
        self._seen_requirements.add(self._resolved_key(req, version, "sdist"))
        if not sdist_only:
            # Mark wheel seen only for wheel build
            self._seen_requirements.add(self._resolved_key(req, version, "wheel"))

    def _has_been_seen(
        self,
        req: Requirement,
        version: Version,
        sdist_only: bool = False,
    ) -> bool:
        typ: typing.Literal["sdist", "wheel"] = "sdist" if sdist_only else "wheel"
        return self._resolved_key(req, version, typ) in self._seen_requirements

    def _add_to_build_order(
        self,
        req: Requirement,
        version: Version,
        source_url: str,
        source_url_type: str,
        prebuilt: bool = False,
        constraint: Requirement | None = None,
    ) -> None:
        # We only care if this version of this package has been built,
        # and don't want to trigger building it twice. The "extras"
        # value, included in the _resolved_key() output, can confuse
        # that so we ignore itand build our own key using just the
        # name and version.
        key = (canonicalize_name(req.name), str(version))
        if key in self._build_requirements:
            return
        logger.info(f"adding {key} to build order")
        self._build_requirements.add(key)
        info = {
            "req": str(req),
            "constraint": str(constraint) if constraint else "",
            "dist": canonicalize_name(req.name),
            "version": str(version),
            "prebuilt": prebuilt,
            "source_url": source_url,
            "source_url_type": source_url_type,
        }
        self._build_stack.append(info)
        with open(self._build_order_filename, "w") as f:
            # Set default=str because the why value includes
            # Requirement and Version instances that can't be
            # converted to JSON without help.
            json.dump(self._build_stack, f, indent=2, default=str)
