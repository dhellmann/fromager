import collections
import logging
import os
import pathlib
import threading
from urllib.parse import urlparse

from packaging.requirements import Requirement
from packaging.utils import NormalizedName, canonicalize_name
from packaging.version import Version

from . import constraints, dependency_graph, packagesettings, request_session

logger = logging.getLogger(__name__)

# Map package names to (requirement type, dependency name, version)
BuildRequirements = dict[str, list[tuple[str, NormalizedName, Version, Requirement]]]
ROOT_BUILD_REQUIREMENT = canonicalize_name("", validate=False)


class WorkContext:
    def __init__(
        self,
        active_settings: packagesettings.Settings | None,
        constraints_file: str | None,
        patches_dir: pathlib.Path,
        sdists_repo: pathlib.Path,
        wheels_repo: pathlib.Path,
        work_dir: pathlib.Path,
        cleanup: bool = True,
        variant: str = "cpu",
        network_isolation: bool = False,
        max_jobs: int | None = None,
        settings_dir: pathlib.Path | None = None,
    ):
        if active_settings is None:
            active_settings = packagesettings.Settings(
                settings=packagesettings.SettingsFile(),
                package_settings=[],
                patches_dir=patches_dir,
                variant=variant,
                max_jobs=max_jobs,
            )
        self.settings = active_settings
        self.input_constraints_uri: str | None
        self.constraints = constraints.Constraints()
        if constraints_file is not None:
            self.input_constraints_uri = constraints_file
            self.constraints.load_constraints_file(constraints_file)
        else:
            self.input_constraints_uri = None
        self.sdists_repo = pathlib.Path(sdists_repo).absolute()
        self.sdists_downloads = self.sdists_repo / "downloads"
        self.sdists_builds = self.sdists_repo / "builds"
        self.wheels_repo = pathlib.Path(wheels_repo).absolute()
        self.wheels_build_base = self.wheels_repo / "build"
        self.wheels_downloads = self.wheels_repo / "downloads"
        self.wheels_prebuilt = self.wheels_repo / "prebuilt"
        self.wheel_server_dir = self.wheels_repo / "simple"
        self.work_dir = pathlib.Path(work_dir).absolute()
        self.wheel_server_url = ""
        self.logs_dir = self.work_dir / "logs"
        self.cleanup = cleanup
        self.variant = variant
        self.network_isolation = network_isolation
        self.settings_dir = settings_dir

        self._constraints_filename = self.work_dir / "constraints.txt"

        self.dependency_graph = dependency_graph.DependencyGraph()

        # storing metrics
        self.time_store: dict[str, dict[str, float]] = collections.defaultdict(
            dict[str, float]
        )
        self.time_description_store: dict[str, str] = {}

        self._parallel_builds = False

    def enable_parallel_builds(self) -> None:
        self._parallel_builds = True

    @property
    def wheels_build(self) -> pathlib.Path:
        # when parallel builds are enabled, return a path that is unique for the
        # current thread to avoid collisions when creating output files
        if self._parallel_builds:
            thread_path = self.wheels_build_base / f"{threading.get_native_id()}"
            thread_path.mkdir(parents=True, exist_ok=True)
            return thread_path
        else:
            return self.wheels_build_base

    @property
    def pip_wheel_server_args(self) -> list[str]:
        args = ["--index-url", self.wheel_server_url]
        parsed = urlparse(self.wheel_server_url)
        if parsed.scheme != "https" and parsed.hostname:
            args = args + ["--trusted-host", parsed.hostname]
        return args

    @property
    def pip_constraint_args(self) -> list[str]:
        if not self.input_constraints_uri:
            return []

        if self.input_constraints_uri.startswith(("https://", "http://", "file://")):
            path_to_constraints_file = self.work_dir / "input-constraints.txt"
            if not path_to_constraints_file.exists():
                response = request_session.session.get(self.input_constraints_uri)
                path_to_constraints_file.write_text(response.text)
        else:
            path_to_constraints_file = pathlib.Path(self.input_constraints_uri)

        path_to_constraints_file = path_to_constraints_file.absolute()
        return ["--constraint", os.fspath(path_to_constraints_file)]

    def write_to_graph_to_file(self):
        with open(self.work_dir / "graph.json", "w") as f:
            self.dependency_graph.serialize(f)

    def package_build_info(
        self, package: str | packagesettings.Package | Requirement
    ) -> packagesettings.PackageBuildInfo:
        if isinstance(package, Requirement):
            name = package.name
        else:
            name = package
        return self.settings.package_build_info(name)

    def setup(self) -> None:
        # The work dir must already exist, so don't try to create it.
        # Use os.makedirs() to create the others in case the paths
        # already exist.
        for p in [
            self.work_dir,
            self.sdists_repo,
            self.sdists_downloads,
            self.sdists_builds,
            self.wheels_repo,
            self.wheels_downloads,
            self.wheels_prebuilt,
            self.wheels_build,
            self.logs_dir,
        ]:
            if not p.exists():
                logger.debug("creating %s", p)
                p.mkdir(parents=True)
