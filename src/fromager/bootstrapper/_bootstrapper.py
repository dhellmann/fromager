from __future__ import annotations

import concurrent.futures
import contextlib
import datetime
import io
import json
import logging
import operator
import pathlib
import shutil
import tempfile
import time
import typing

from packaging.requirements import Requirement
from packaging.utils import NormalizedName, canonicalize_name
from packaging.version import Version

from .. import (
    bootstrap_requirement_resolver,
    build_environment,
    dependencies,
    gitutils,
    progress,
    sources,
)
from ..dependency_graph import DependencyGraph
from ..log import req_ctxvar_context, requirement_ctxvar
from ..requirements_file import RequirementType, SourceType
from . import _cache
from ._build import Build
from ._phase import Phase
from ._prepare_build import PrepareBuild
from ._prepare_source import PrepareSource
from ._process_install_deps import ProcessInstallDeps
from ._resolve import Resolve
from ._types import (
    _DEFAULT_BG_THREADS,
    FailureRecord,
    FailureType,
    SeenKey,
    SourceBuildResult,
)
from ._work_item import WorkItem

if typing.TYPE_CHECKING:
    from .. import context

logger = logging.getLogger(__name__)


class Bootstrapper:
    """Iterative DFS bootstrap engine for resolving and building packages.

    Use as a context manager. Call ``bootstrap(requirements)`` to process
    the dependency tree, then ``finalize()`` for cleanup and exit code.
    """

    def __init__(
        self,
        ctx: context.WorkContext,
        progressbar: progress.Progressbar | None = None,
        prev_graph: DependencyGraph | None = None,
        cache_wheel_server_url: str | None = None,
        sdist_only: bool = False,
        test_mode: bool = False,
        multiple_versions: bool = False,
        num_bg_threads: int = _DEFAULT_BG_THREADS,
    ) -> None:
        if test_mode and sdist_only:
            raise ValueError(
                "--test-mode requires full wheel builds; incompatible with --sdist-only"
            )

        self.ctx = ctx
        self.progressbar = progressbar or progress.Progressbar(None)
        self.prev_graph = prev_graph
        self.cache_wheel_server_url = cache_wheel_server_url or ctx.wheel_server_url
        self.sdist_only = sdist_only
        self.test_mode = test_mode
        self.multiple_versions = multiple_versions
        self.why: list[tuple[RequirementType, Requirement, Version]] = []
        self._num_bg_threads = max(1, num_bg_threads)
        self._bg_pool: concurrent.futures.ThreadPoolExecutor | None = (
            concurrent.futures.ThreadPoolExecutor(
                max_workers=self._num_bg_threads, thread_name_prefix="fromager-bg"
            )
        )

        # Delegate resolution to BootstrapRequirementResolver
        self._resolver = bootstrap_requirement_resolver.BootstrapRequirementResolver(
            ctx=ctx,
            prev_graph=prev_graph,
            multiple_versions=multiple_versions,
            cache_wheel_server_url=self.cache_wheel_server_url,
        )
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
        self._stack_filename = self.ctx.work_dir / "bootstrap-stack.json"
        logger.info("recording bootstrap stack state to %s", self._stack_filename)

        # Single-threaded pool for background file writes (serialized, never interleave)
        self._write_pool: concurrent.futures.ThreadPoolExecutor | None = (
            concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="fromager-writes"
            )
        )
        # Throttle stack-state writes to at most once per interval
        self._last_stack_write: float = 0.0
        self._STACK_WRITE_INTERVAL: float = 5.0

        # Track failed packages in test mode (list of typed dicts for JSON export)
        self.failed_packages: list[FailureRecord] = []

        # Track failed versions in multiple_versions mode
        self._failed_versions: dict[tuple[str, str], Exception] = {}

    @property
    def resolver(self) -> bootstrap_requirement_resolver.BootstrapRequirementResolver:
        """Public accessor for the version resolver."""
        return self._resolver

    def has_failed_version(self, pkg_name: NormalizedName, version: Version) -> bool:
        """Return True if this (pkg_name, version) has previously failed resolution."""
        return (pkg_name, str(version)) in self._failed_versions

    def _resolve_and_add_top_level(
        self,
        req: Requirement,
    ) -> tuple[str, Version] | None:
        """Resolve a top-level requirement and add it to the dependency graph.

        Private method called only by ``bootstrap()``.

        This is the pre-resolution phase before recursive bootstrapping begins.
        In test mode, catches resolution errors and records them as failures.

        When multiple_versions is enabled, resolves and adds all matching versions
        to the graph, but still returns only the first (highest) version for
        backward compatibility.

        Args:
            req: The top-level requirement to resolve.

        Returns:
            Tuple of (source_url, version) if resolution succeeded, None if it
            failed in test mode.

        Raises:
            Exception: In normal mode, re-raises any resolution error.
        """
        try:
            pbi = self.ctx.package_build_info(req)
            results = self.resolve_versions(
                req=req,
                req_type=RequirementType.TOP_LEVEL,
                parent_req=None,
                return_all_versions=self.multiple_versions,
            )
            if self.multiple_versions:
                logger.info(f"resolved {len(results)} version(s) for {req}")

            # Add all resolved versions to the graph
            for source_url, version in results:
                logger.info("%s resolves to %s", req, version)
                self.ctx.dependency_graph.add_dependency(
                    parent_name=None,
                    parent_version=None,
                    req_type=RequirementType.TOP_LEVEL,
                    req=req,
                    req_version=version,
                    download_url=source_url,
                    pre_built=pbi.pre_built,
                    constraint=self.ctx.constraints.get_constraint(req.name),
                )

            if not results:
                if self.multiple_versions:
                    err = RuntimeError(f"no versions found for {req}")
                    self._record_failed_version(
                        req, "unresolved", err, "no versions resolved, skipping"
                    )
                    if self.test_mode:
                        self.record_test_mode_failure(req, None, err, "resolution")
                    return None
                raise RuntimeError(f"Could not resolve any versions for {req}")

            # Return first result for backward compatibility
            return results[0]
        except Exception as err:
            if self.multiple_versions:
                self._record_failed_version(req, "unresolved", err, "failed to resolve")
                if self.test_mode:
                    self.record_test_mode_failure(req, None, err, "resolution")
                return None
            if not self.test_mode:
                raise
            self.record_test_mode_failure(req, None, err, "resolution")
            return None

    def resolve_versions(
        self,
        req: Requirement,
        req_type: RequirementType,
        parent_req: Requirement | None = None,
        return_all_versions: bool = False,
    ) -> list[tuple[str, Version]]:
        """Resolve version(s) of a requirement.

        Returns list of (source URL, version) tuples, sorted by version (highest first).

        Git URL resolution stays in Bootstrapper because it requires
        build orchestration (BuildEnvironment, build dependencies).
        Delegates PyPI/graph resolution to BootstrapRequirementResolver.

        Args:
            req: Package requirement to resolve
            req_type: Type of requirement
            parent_req: Explicit parent requirement from dependency chain.
                Callers must pass this explicitly; do not read ``self.why`` here.
            return_all_versions: If True, return all matching versions.
                If False, return only highest version.

        Returns:
            List of (url, version) tuples. Contains one item when
            return_all_versions=False, or all matching versions when True.
        """
        if req.url:
            if req_type != RequirementType.TOP_LEVEL:
                raise ValueError(
                    f"{req} includes a URL, but is not a top-level dependency"
                )

            # Check cache first to avoid re-resolving
            # Git URLs are always source (not prebuilt)
            cached_result = self._resolver.get_matching_versions(req, pre_built=False)
            if cached_result:
                logger.debug(f"resolved {req} from cache")
                return cached_result if return_all_versions else [cached_result[0]]

            logger.info("resolving source via URL, ignoring any plugins")
            source_url, resolved_version = self._resolve_version_from_git_url(req=req)
            # Cache the git URL resolution (always source, not prebuilt)
            # Store as list for consistency with cache structure
            result = [(source_url, resolved_version)]
            self._resolver.extend_known_versions(req, pre_built=False, result=result)
            return result  # Git URLs always return single version

        # Delegate to RequirementResolver
        return self._resolver.resolve(
            req=req,
            req_type=req_type,
            parent_req=parent_req,
            return_all_versions=return_all_versions,
        )

    def bootstrap(self, requirements: list[Requirement]) -> None:
        """Bootstrap all top-level requirements and their transitive dependencies.

        .. versionadded:: 0.89
           Replaces the former ``bootstrap(req, req_type)`` signature.

        Resolves each requirement, adds it to the dependency graph, and processes
        the full dependency tree using an iterative DFS loop. Handles
        ``requirement_ctxvar`` context internally; callers do not need to manage it.

        In test mode, records failures and continues instead of raising. In
        ``multiple_versions`` mode, processes all matching versions per requirement.

        Args:
            requirements: Top-level requirements to resolve and bootstrap.
        """
        # Resolve all top-level reqs and build initial stack.
        # Use the token pattern (no try/finally) so that if resolution raises
        # in normal mode, the context var stays set for the top-level error
        # handler in __main__.py to include the package name in its log message.
        stack: list[Phase] = []
        initial_items: list[Phase] = []
        for req in requirements:
            token = requirement_ctxvar.set(req)
            result = self._resolve_and_add_top_level(req)
            requirement_ctxvar.reset(token)
            if result is not None:
                initial_items.append(
                    Resolve(
                        WorkItem(
                            req=req,
                            req_type=RequirementType.TOP_LEVEL,
                            why_snapshot=[],
                            parent=None,
                        )
                    )
                )
        self._push_items(stack, initial_items)

        self._run_bootstrap_loop(stack)

    def _run_bootstrap_loop(self, stack: list[Phase]) -> None:
        """Run the iterative DFS bootstrap loop over a pre-built work stack.

        Pops items one at a time, dispatches each phase, and pushes any
        follow-on items (continuations and new dependencies) back onto the
        stack. Updates the progress bar as items complete.

        Args:
            stack: Initial list of ``Phase`` objects to process. Modified
                in-place; empty on return.
        """
        while stack:
            self._record_stack_state(stack)
            item = stack.pop()
            self.why = list(item.work_item.why_snapshot)

            with (
                req_ctxvar_context(item.work_item.req, item.work_item.resolved_version),
                self._track_why(item),
            ):
                if item.requires_exclusive_run:
                    logger.info(
                        "%s requires exclusive build, draining background pool",
                        item.work_item.req,
                    )
                    self._drain_background_pool()
                try:
                    new_items = item.run(self)
                except Exception as err:
                    new_items = self._handle_phase_error(item, err)

            new_dep_count = sum(1 for it in new_items if isinstance(it, Resolve))
            if new_dep_count > 0:
                self.progressbar.update_total(new_dep_count)
            if not new_items:
                self.progressbar.update()

            self._push_items(stack, new_items)

    def _bootstrap_one(self, req: Requirement, req_type: RequirementType) -> None:
        """Bootstrap a single requirement using an iterative DFS loop.

        Internal method used only by the git URL resolution path
        (``_handle_build_requirements``). All other callers should use
        ``bootstrap(requirements)`` instead.

        Uses an explicit LIFO stack instead of recursion to handle arbitrarily
        deep dependency graphs without hitting Python's recursion limit.

        In test mode, catches build exceptions, records package name, and continues.
        In normal mode, raises exceptions immediately (fail-fast).

        When multiple_versions is enabled, bootstraps all matching versions instead
        of just the highest version.
        """
        logger.info(f"bootstrapping {req} as {req_type} dependency of {self.why[-1:]}")

        # Capture parent from current why stack before creating work items
        parent: tuple[Requirement, Version] | None = None
        if self.why:
            _, parent_req, parent_version = self.why[-1]
            parent = (parent_req, parent_version)

        # Save the why stack so we can restore it after the iterative loop
        # (the loop modifies self.why for each work item)
        saved_why = list(self.why)

        # Single RESOLVE item — resolution, version expansion, and error
        # handling all happen inside the loop via Resolve.run().
        initial_item = Resolve(
            WorkItem(
                req=req,
                req_type=req_type,
                why_snapshot=list(self.why),
                parent=parent,
            )
        )
        stack: list[Phase] = []
        self._push_items(stack, [initial_item])

        self._run_bootstrap_loop(stack)

        # empty the stack state file to show watchers are are done
        # with bootstrapping
        self._record_stack_state([])

        # Restore why stack for the caller
        self.why = saved_why

    @contextlib.contextmanager
    def _track_why(
        self,
        item: Phase,
    ) -> typing.Generator[None, None, None]:
        """Context manager to track dependency chain in self.why stack.

        No-op for phases where tracks_why is False (RESOLVE and START).
        For all other phases, pushes the item onto the why stack and
        ensures it is popped even if an exception occurs.
        """
        if not item.tracks_why:
            yield
            return
        wi = item.work_item
        assert wi.resolved_version is not None
        self.why.append((wi.req_type, wi.req, wi.resolved_version))
        try:
            yield
        finally:
            self.why.pop()

    def record_test_mode_failure(
        self,
        req: Requirement,
        version: str | None,
        err: Exception,
        failure_type: FailureType,
        log_level: typing.Literal["error", "warning"] = "error",
    ) -> None:
        """Record a failure in test mode. Call this after checking test_mode.

        Args:
            req: The requirement that failed.
            version: The version being processed (None if not yet resolved).
            err: The exception that was raised.
            failure_type: Category of failure for analysis.
            log_level: Log at error (fatal) or warning (non-fatal, continuing).
        """
        version_str = f"=={version}" if version else ""
        msg = f"test mode: {failure_type} failed for {req.name}{version_str}"
        if log_level == "warning":
            logger.warning("%s: %s (continuing)", msg, err)
        else:
            logger.error("%s: %s", msg, err, exc_info=True)

        self.failed_packages.append(
            {
                "package": str(req.name),
                "version": version,
                "exception_type": err.__class__.__name__,
                "exception_message": str(err),
                "failure_type": failure_type,
            }
        )

    @property
    def explain(self) -> str:
        """Return message formatting current version of why stack."""
        return " for ".join(
            f"{req_type} dependency {req} ({resolved_version})"
            for req_type, req, resolved_version in reversed(self.why)
        )

    def _prepare_build_dependencies(
        self,
        req: Requirement,
        resolved_version: Version | None,
        sdist_root_dir: pathlib.Path,
        build_env: build_environment.BuildEnvironment,
    ) -> set[Requirement]:
        """Prepare build dependencies for a package.

        Only used by the git URL resolution path
        (_resolve_version_from_git_url -> _get_version_from_package_metadata).
        The main iterative bootstrap loop handles build deps via phase handlers.
        """
        # build system
        build_system_dependencies = dependencies.get_build_system_dependencies(
            ctx=self.ctx,
            req=req,
            version=resolved_version,
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
            version=resolved_version,
            sdist_root_dir=sdist_root_dir,
            build_env=build_env,
        )

        # build sdist
        build_sdist_dependencies = dependencies.get_build_sdist_dependencies(
            ctx=self.ctx,
            req=req,
            version=resolved_version,
            sdist_root_dir=sdist_root_dir,
            build_env=build_env,
        )

        # Filter out deps already satisfied by build-system dependencies
        resolved_build_sys = self._resolve_build_system_versions_by_name(
            build_system_dependencies,
        )
        build_backend_dependencies = self.filter_deps_satisfied_by_build_system(
            build_backend_dependencies,
            resolved_build_sys,
            RequirementType.BUILD_BACKEND,
        )
        build_sdist_dependencies = self.filter_deps_satisfied_by_build_system(
            build_sdist_dependencies,
            resolved_build_sys,
            RequirementType.BUILD_SDIST,
        )

        self._handle_build_requirements(
            req,
            RequirementType.BUILD_BACKEND,
            build_backend_dependencies,
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
        """Bootstrap build dependencies.

        Only used by the git URL resolution path
        (_resolve_version_from_git_url -> _get_version_from_package_metadata).
        The main iterative bootstrap loop handles build deps via phase handlers.
        """
        self.progressbar.update_total(len(build_dependencies))

        for dep in self._sort_requirements(build_dependencies):
            with req_ctxvar_context(dep):
                # Save/restore self.why because the iterative bootstrap()
                # modifies it internally for each work item.
                saved_why = list(self.why)
                self._bootstrap_one(req=dep, req_type=build_type)
                self.why = saved_why
            self.progressbar.update()

    def _download_prebuilt(
        self,
        req: Requirement,
        req_type: RequirementType,
        resolved_version: Version,
        wheel_url: str,
    ) -> tuple[pathlib.Path, pathlib.Path]:
        result = _cache.bg_prepare_prebuilt(
            self.ctx, req, req_type, resolved_version, wheel_url
        )
        assert result.wheel_filename is not None
        assert result.unpack_dir is not None
        return (result.wheel_filename, result.unpack_dir)

    def _download_source(
        self,
        req: Requirement,
        resolved_version: Version,
        source_url: str,
    ) -> pathlib.Path:
        """Download source for a package."""
        result: pathlib.Path = sources.download_source(
            ctx=self.ctx,
            req=req,
            version=resolved_version,
            download_url=source_url,
        )
        return result

    def _prepare_source(
        self,
        req: Requirement,
        resolved_version: Version,
        source_filename: pathlib.Path,
    ) -> pathlib.Path:
        """Prepare (unpack/patch) source for building."""
        result: pathlib.Path = sources.prepare_source(
            ctx=self.ctx,
            req=req,
            source_filename=source_filename,
            version=resolved_version,
        )
        return result

    def _handle_test_mode_failure(
        self,
        req: Requirement,
        resolved_version: Version,
        req_type: RequirementType,
        build_error: Exception,
    ) -> SourceBuildResult | None:
        """Handle build failure in test mode by attempting pre-built fallback.

        Args:
            req: The requirement that failed to build.
            resolved_version: The version that was attempted.
            req_type: The type of requirement (for fallback resolution).
            build_error: The original exception from the build attempt.

        Returns:
            SourceBuildResult if fallback succeeded, None if fallback also failed.
        """
        logger.warning(
            "test mode: build failed for %s==%s, attempting pre-built fallback: %s",
            req.name,
            resolved_version,
            build_error,
        )

        try:
            parent_req = self.why[-1][1] if self.why else None
            results = self._resolver.resolve(
                req=req,
                req_type=req_type,
                parent_req=parent_req,
                pre_built=True,  # Force prebuilt for test mode fallback
            )
            wheel_url, fallback_version = results[0]

            if fallback_version != resolved_version:
                logger.warning(
                    "test mode: version mismatch for %s - requested %s, fallback %s",
                    req.name,
                    resolved_version,
                    fallback_version,
                )

            wheel_filename, unpack_dir = self._download_prebuilt(
                req=req,
                req_type=req_type,
                resolved_version=fallback_version,
                wheel_url=wheel_url,
            )

            logger.info(
                "test mode: successfully used pre-built wheel for %s==%s",
                req.name,
                fallback_version,
            )

            # Package succeeded via fallback - no failure to record

            return SourceBuildResult(
                wheel_filename=wheel_filename,
                sdist_filename=None,
                unpack_dir=unpack_dir,
                sdist_root_dir=None,
                build_env=None,
                source_type=SourceType.PREBUILT,
            )

        except Exception as fallback_error:
            logger.error(
                "test mode: pre-built fallback also failed for %s: %s",
                req.name,
                fallback_error,
                exc_info=True,
            )
            # Return None to signal failure; bootstrap() will record via re-raised exception
            return None

    def _resolve_version_from_git_url(self, req: Requirement) -> tuple[str, Version]:
        """Resolve source path and version from a ``git+`` URL.

        Parses the URL for an ``@ref`` version hint. If the ref is a valid
        version, reuses an existing clone when possible. Otherwise, clones
        the repo and extracts the version from package metadata.
        """

        if not req.url:
            raise ValueError(f"unable to resolve from URL with no URL in {req}")

        # We start by not knowing where we would put the source because we don't
        # know the version.
        working_src_dir: pathlib.Path | None = None
        version: Version | None = None

        url_to_clone, git_ref = gitutils.parse_vcs_url(req.url, require_ref=False)
        need_to_clone = False

        if git_ref == gitutils.GIT_HEAD:
            # No ref in URL, clone to discover the version.
            logger.debug("no reference in URL, will clone")
            need_to_clone = True
        else:
            # If we have a reference, it might be a valid python version
            # string, or not. It _must_ be a valid git reference. If it can
            # be parsed as a valid python version, we assume the tag points
            # to source that will think that is its version, so we allow
            # reusing an existing cloned repo if there is one.
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
                sources.download_git_source(
                    ctx=self.ctx,
                    req=req,
                    url_to_clone=url_to_clone,
                    destination_dir=clone_dir,
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
                assert working_src_dir is not None
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
        build_env = build_environment.BuildEnvironment(
            ctx=self.ctx,
            parent_dir=source_dir.parent,
        )
        build_dependencies = self._prepare_build_dependencies(
            req=req,
            resolved_version=None,
            sdist_root_dir=source_dir,
            build_env=build_env,
        )
        build_env.install(build_dependencies)

        logger.info("generating metadata to get version")
        hook_caller = dependencies.get_build_backend_hook_caller(
            ctx=self.ctx,
            req=req,
            build_dir=build_dir,
            override_environ={},
            build_env=build_env,
        )
        metadata_dir_base = hook_caller.prepare_metadata_for_build_wheel(
            metadata_directory=str(source_dir.parent),
            config_settings=pbi.config_settings,
        )
        metadata_filename = source_dir.parent / metadata_dir_base / "METADATA"
        # Disable validation because some packages have metadata version mismatches
        # (e.g., declaring Metadata-Version: 2.2 but using fields from 2.4).
        metadata = dependencies.parse_metadata(metadata_filename, validate=False)
        return metadata.version

    def add_to_graph(
        self,
        req: Requirement,
        req_type: RequirementType,
        req_version: Version,
        download_url: str,
        parent: tuple[Requirement, Version] | None,
    ) -> None:
        """Add a resolved requirement as a node/edge in the dependency graph.

        Records the dependency relationship between ``parent`` and ``req`` in
        the in-memory graph, then writes the updated graph to disk. Must be
        called after resolution but before the seen-check so that every edge
        is captured even for packages that are not built again.

        Args:
            req: The requirement being added.
            req_type: Whether this is a build-system, build-backend, install,
                or other dependency kind.
            req_version: The resolved version of ``req``.
            download_url: Source URL used to obtain the distribution.
            parent: ``(req, version)`` of the package that introduced ``req``,
                or ``None`` for top-level requirements.
        """
        parent_req, parent_version = parent if parent else (None, None)
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
            constraint=self.ctx.constraints.get_constraint(req.name),
        )
        self._write_graph_async()

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

    def mark_as_seen(
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

    def has_been_seen(
        self,
        req: Requirement,
        version: Version,
        sdist_only: bool = False,
    ) -> bool:
        """Return True if this requirement/version has already been processed.

        Uses the same sdist-vs-wheel distinction as ``mark_as_seen``:
        when ``sdist_only`` is True, checks only for a prior sdist build;
        otherwise checks for a prior wheel build.

        Args:
            req: The requirement to check.
            version: The resolved version to check.
            sdist_only: If True, check for a sdist-only build instead of a
                full wheel build.
        """
        typ: typing.Literal["sdist", "wheel"] = "sdist" if sdist_only else "wheel"
        return self._resolved_key(req, version, typ) in self._seen_requirements

    def add_to_build_order(
        self,
        req: Requirement,
        version: Version,
        source_url: str,
        source_type: SourceType,
        prebuilt: bool = False,
        constraint: Requirement | None = None,
    ) -> None:
        """Append a package to the build-order list if not already present.

        Deduplicates by ``(canonicalized_name, version)`` so the same package
        is never written twice, regardless of extras. Data is buffered in
        memory and written to ``self._build_order_filename`` once during
        ``finalize()``.

        Args:
            req: The requirement being added to the build order.
            version: The resolved version.
            source_url: URL from which the source distribution was obtained.
            source_type: The kind of source (sdist, wheel, git, etc.).
            prebuilt: True if the wheel was taken from a prebuilt cache rather
                than compiled locally.
            constraint: The version constraint active for this package, if any.
        """
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
            "source_url_type": str(source_type),
        }
        if req.url:
            info["source_url"] = req.url
        self._build_stack.append(info)

    def _check_write_error(self, fut: concurrent.futures.Future[int]) -> None:
        """Log errors from background file writes."""
        if exc := fut.exception():
            logger.error("background file write failed: %s", exc)

    def _schedule_write(self, path: pathlib.Path, content: str) -> None:
        """Submit a pre-serialized write to the background write pool."""
        if self._write_pool is not None:
            fut = self._write_pool.submit(path.write_text, content, "utf-8")
            fut.add_done_callback(self._check_write_error)

    def _write_graph_async(self) -> None:
        """Serialize the dependency graph on the main thread, write it in background.

        Must only be called from the main thread. Serialization happens
        synchronously here so the snapshot is consistent with the caller's
        view of the graph; only the file I/O is offloaded to the write pool.
        """
        buf = io.StringIO()
        self.ctx.dependency_graph.serialize(buf)
        self._schedule_write(self.ctx.graph_file, buf.getvalue())

    def _record_stack_state(self, stack: list[Phase]) -> None:
        """Write the current bootstrap stack to `self._stack_filename`.

        Index 0 in the output corresponds to `stack[-1]`, the next item to be
        processed. Throttled to at most once every ``_STACK_WRITE_INTERVAL`` seconds.
        """
        now = time.monotonic()
        if now - self._last_stack_write < self._STACK_WRITE_INTERVAL:
            return
        records = [item.as_json() for item in reversed(stack)]
        with open(self._stack_filename, "w") as f:
            json.dump(records, f, indent=2, default=str)
        self._last_stack_write = now

    # ---- Iterative bootstrap: phase handlers and helpers ----

    def _push_items(self, stack: list[Phase], items: list[Phase]) -> None:
        """Push items onto the stack and submit background tasks in LIFO order.

        Submits the item that will be processed first (top of stack) to the
        background pool first, maximising overlap between background I/O and
        main-thread serial work.
        """
        stack.extend(items)
        if self._bg_pool is not None:
            for item in reversed(items):
                bg_work = item.background_work(self)
                if bg_work is not None:
                    item.bg_future = self._bg_pool.submit(bg_work)

    def _drain_background_pool(self) -> None:
        """Drain all in-flight background tasks and recreate the pool.

        Used as an exclusive-build barrier: ensures all background I/O completes
        before an exclusive build starts. ``cancel_futures=False`` guarantees
        every submitted task runs to completion.
        """
        if self._bg_pool is not None:
            self._bg_pool.shutdown(wait=True, cancel_futures=False)
            self._bg_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=self._num_bg_threads, thread_name_prefix="fromager-bg"
            )

    def create_unresolved_work_items(
        self,
        deps: typing.Iterable[Requirement],
        dep_req_type: RequirementType,
        parent_req: Requirement,
        parent_version: Version,
    ) -> list[Phase]:
        """Create RESOLVE-phase work items for dependencies.

        Called inside a parent's _track_why context so that why_snapshot
        captures the parent's dependency chain. Resolution and error
        handling happen later when each item's RESOLVE phase runs.
        """
        return [
            Resolve(
                WorkItem(
                    req=dep,
                    req_type=dep_req_type,
                    why_snapshot=list(self.why),
                    parent=(parent_req, parent_version),
                )
            )
            for dep in self._sort_requirements(deps)
        ]

    def _resolve_build_system_versions_by_name(
        self,
        build_system_deps: set[Requirement],
    ) -> dict[NormalizedName, tuple[Version, str]]:
        """Build a mapping of resolved build-system versions by looking up graph nodes.

        Used by the git URL path where the parent node may not yet exist
        in the dependency graph.
        """
        resolved: dict[NormalizedName, tuple[Version, str]] = {}
        for dep in build_system_deps:
            dep_name = canonicalize_name(dep.name)
            nodes = self.ctx.dependency_graph.get_nodes_by_name(str(dep_name))
            for node in nodes:
                if node.version in dep.specifier:
                    resolved[dep_name] = (node.version, node.download_url)
                    break
        return resolved

    def get_resolved_build_system_versions(
        self,
        item: WorkItem,
    ) -> dict[NormalizedName, tuple[Version, str]]:
        """Build a mapping of resolved build-system dependency versions.

        Looks up the parent node's ``BUILD_SYSTEM`` edges in the dependency
        graph to find what versions were resolved for each build-system
        dependency.

        Returns:
            Mapping of canonicalized package name to ``(version, download_url)``.
        """
        assert item.resolved_version is not None
        parent_key = f"{canonicalize_name(item.req.name)}=={item.resolved_version}"
        parent_node = self.ctx.dependency_graph.nodes.get(parent_key)
        if parent_node is None:
            return {}
        resolved: dict[NormalizedName, tuple[Version, str]] = {}
        for edge in parent_node.children:
            if edge.req_type == RequirementType.BUILD_SYSTEM:
                child = edge.destination_node
                resolved[child.canonicalized_name] = (
                    child.version,
                    child.download_url,
                )
        return resolved

    def filter_deps_satisfied_by_build_system(
        self,
        deps: set[Requirement],
        resolved_build_sys: dict[NormalizedName, tuple[Version, str]],
        dep_req_type: RequirementType,
        parent: tuple[Requirement, Version] | None = None,
    ) -> set[Requirement]:
        """Filter out deps already satisfied by build-system dependencies.

        For each dep whose resolved build-system version satisfies the
        requirement specifier, excludes the dep from the returned set.
        When *parent* is provided, also adds a graph edge reusing that
        version.  Remaining deps need independent resolution.

        Logs a warning when the same package appears in both build-system
        and build-backend/sdist with incompatible version specifiers.
        """
        unsatisfied: set[Requirement] = set()
        for dep in deps:
            if dep.extras:
                unsatisfied.add(dep)
                continue
            dep_name = canonicalize_name(dep.name)
            if dep_name in resolved_build_sys:
                version, download_url = resolved_build_sys[dep_name]
                if version in dep.specifier:
                    logger.info(
                        "%s dependency %s is already satisfied by "
                        "build-system dependency %s==%s",
                        dep_req_type,
                        dep,
                        dep_name,
                        version,
                    )
                    if parent is not None:
                        self.add_to_graph(
                            req=dep,
                            req_type=dep_req_type,
                            req_version=version,
                            download_url=download_url,
                            parent=parent,
                        )
                    continue
                else:
                    logger.warning(
                        "%s dependency %s conflicts with "
                        "build-system dependency %s==%s; "
                        "resolving independently",
                        dep_req_type,
                        dep,
                        dep_name,
                        version,
                    )
            unsatisfied.add(dep)
        return unsatisfied

    def _handle_phase_error(
        self,
        item: Phase,
        err: Exception,
    ) -> list[Phase]:
        """Handle errors from phase processing.

        Returns work items to continue processing (e.g. prebuilt fallback),
        or empty list to skip this item. Raises in normal mode (fail-fast).
        """
        wi = item.work_item
        # Resolution failures: recoverable in test mode and multiple versions mode
        if isinstance(item, Resolve):
            if self.test_mode:
                self.record_test_mode_failure(wi.req, None, err, "resolution")
                if self.multiple_versions:
                    self._record_failed_version(
                        wi.req,
                        "unresolved",
                        err,
                        f"failed during {type(item).phase} phase",
                    )
                return []
            if self.multiple_versions:
                self._record_failed_version(
                    wi.req,
                    "unresolved",
                    err,
                    f"failed during {type(item).phase} phase",
                )
                return []
            raise

        # Test mode: try prebuilt fallback for build-related phases
        if self.test_mode:
            if (
                isinstance(item, PrepareSource | PrepareBuild | Build)
                and not wi.pbi_pre_built
            ):
                assert wi.resolved_version is not None
                fallback = self._handle_test_mode_failure(
                    req=wi.req,
                    resolved_version=wi.resolved_version,
                    req_type=wi.req_type,
                    build_error=err,
                )
                if fallback is not None:
                    wi.build_result = fallback
                    return [ProcessInstallDeps(wi)]
            self.record_test_mode_failure(
                wi.req, str(wi.resolved_version), err, "bootstrap"
            )
            return []

        # Multiple versions mode: record failure, remove from graph, continue
        if self.multiple_versions:
            assert wi.resolved_version is not None
            pkg_name = canonicalize_name(wi.req.name)
            self._record_failed_version(
                wi.req,
                str(wi.resolved_version),
                err,
                f"failed during {type(item).phase} phase",
            )
            self.ctx.dependency_graph.remove_dependency(pkg_name, wi.resolved_version)
            self._seen_requirements.discard(
                self._resolved_key(wi.req, wi.resolved_version, "sdist")
            )
            self._seen_requirements.discard(
                self._resolved_key(wi.req, wi.resolved_version, "wheel")
            )
            self._write_graph_async()
            return []

        # Normal mode: fail-fast
        raise

    def _record_failed_version(
        self,
        req: Requirement,
        version: str,
        err: Exception,
        detail: str,
    ) -> None:
        """Record a version failure in multiple versions mode."""
        pkg_name = canonicalize_name(req.name)
        self._failed_versions[(pkg_name, version)] = err
        logger.warning(
            "%s==%s: %s: %s: %s",
            req.name,
            version,
            detail,
            type(err).__name__,
            err,
        )

    def _log_failed_versions_table(self) -> None:
        """Log a summary table of all failed versions."""
        logger.warning("%d version(s) failed to bootstrap:", len(self._failed_versions))
        for (name, ver), exc in self._failed_versions.items():
            logger.warning("  %s==%s: %s: %s", name, ver, type(exc).__name__, exc)

    def finalize(self) -> int:
        """Finalize bootstrap and return exit code.

        Reports failed versions in multiple versions mode.
        In test mode, writes failure report and returns non-zero if there were failures.

        Note: I/O errors from background graph writes are logged at ERROR level
        but are not raised. Check the log for ``"background file write failed"``
        if the on-disk graph file is suspected to be stale.

        Returns:
            0 if all packages built successfully (or not in test/multiple versions mode)
            1 if any packages failed in test mode
        """
        if self._bg_pool is not None:
            # cancel_futures=True cancels pending (not-yet-started) futures immediately
            # so we don't block waiting for work whose result will never be used.
            # Already-running futures still complete naturally.
            self._bg_pool.shutdown(wait=True, cancel_futures=True)
            self._bg_pool = None

        # Write build-order once at the end (data was buffered in _build_stack)
        with open(self._build_order_filename, "w") as f:
            # Set default=str because values may include Requirement and Version
            # instances that can't be converted to JSON without help.
            json.dump(self._build_stack, f, indent=2, default=str)

        # Final graph write and stack flush, then drain the write pool
        self._write_graph_async()
        records: list[typing.Any] = []
        with open(self._stack_filename, "w") as f:
            json.dump(records, f, indent=2)
        if self._write_pool is not None:
            self._write_pool.shutdown(wait=True, cancel_futures=False)
            self._write_pool = None

        if self.multiple_versions and self._failed_versions:
            self._log_failed_versions_table()

        if not self.test_mode:
            return 0

        if not self.failed_packages:
            logger.info("test mode: all packages processed successfully")
            return 0

        # Write JSON failure report with timestamp for uniqueness
        timestamp = datetime.datetime.now(tz=datetime.UTC).strftime("%Y%m%d-%H%M%S-%f")
        failures_file = self.ctx.work_dir / f"test-mode-failures-{timestamp}.json"
        with open(failures_file, "w") as f:
            json.dump({"failures": self.failed_packages}, f, indent=2)
        logger.info("test mode: wrote failure report to %s", failures_file)

        # Log summary
        failed_names = [f["package"] for f in self.failed_packages]
        logger.error(
            "test mode: %d package(s) failed: %s",
            len(self.failed_packages),
            ", ".join(failed_names),
        )
        return 1

    def __enter__(self) -> Bootstrapper:
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._bg_pool is not None:
            self._bg_pool.shutdown(wait=False, cancel_futures=True)
            self._bg_pool = None
        if self._write_pool is not None:
            # Intentionally abandon any pending graph writes on error exit.
            # A crashed or aborted run produces inconsistent state regardless;
            # waiting for writes would only delay propagation of the exception.
            self._write_pool.shutdown(wait=False, cancel_futures=True)
            self._write_pool = None
