import concurrent.futures
import dataclasses
import datetime
import functools
import json
import logging
import pathlib
import sys
import typing
from urllib.parse import urlparse

import click
import rich
import rich.box
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name, parse_wheel_filename
from packaging.version import Version
from rich.table import Table
from rich.text import Text

from fromager import (
    build_environment,
    clickext,
    context,
    dependency_graph,
    hooks,
    metrics,
    overrides,
    progress,
    read,
    server,
    sources,
    wheels,
)

from ..log import requirement_ctxvar

logger = logging.getLogger(__name__)


@dataclasses.dataclass()
@functools.total_ordering
class BuildSequenceEntry:
    name: str
    version: Version
    prebuilt: bool
    download_url: str
    wheel_filename: pathlib.Path
    skipped: bool = False

    @staticmethod
    def dict_factory(x):
        return {
            k: str(v) if isinstance(v, pathlib.Path | Version) else v for (k, v) in x
        }

    def __lt__(self, other):
        if not isinstance(other, BuildSequenceEntry):
            return NotImplemented
        # sort by lower name and version
        return (self.name.lower(), self.version) < (other.name.lower(), other.version)


@click.command()
@click.option(
    "--wheel-server-url",
    default="",
    type=str,
    help="URL for the wheel server for builds",
)
@click.argument("dist_name")
@click.argument("dist_version", type=clickext.PackageVersion())
@click.argument("sdist_server_url")
@click.pass_obj
def build(
    wkctx: context.WorkContext,
    wheel_server_url: str,
    dist_name: str,
    dist_version: Version,
    sdist_server_url: str,
) -> None:
    """Build a single version of a single wheel

    DIST_NAME is the name of a distribution

    DIST_VERSION is the version to process

    SDIST_SERVER_URL is the URL for a PyPI-compatible package index hosting sdists

    1. Downloads the source distribution.

    2. Unpacks it and prepares the source via patching, vendoring rust
       dependencies, etc.

    3. Prepares a build environment with the build dependencies.

    4. Builds the wheel.

    Refer to the 'step' commands for scripting these stages
    separately.

    """
    wkctx.wheel_server_url = wheel_server_url
    server.start_wheel_server(wkctx)
    req = Requirement(f"{dist_name}=={dist_version}")
    token = requirement_ctxvar.set(req)
    source_url, version = sources.resolve_source(
        ctx=wkctx,
        req=req,
        sdist_server_url=sdist_server_url,
    )
    wheel_filename = _build(wkctx, version, req, source_url)
    requirement_ctxvar.reset(token)
    print(wheel_filename)


build._fromager_show_build_settings = True  # type: ignore


@click.command()
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    help="rebuild wheels even if they have already been built",
)
@click.option(
    "-c",
    "--cache-wheel-server-url",
    "cache_wheel_server_url",
    help="url to a wheel server from where fromager can check if it had already built the wheel",
)
@click.argument("build_order_file")
@click.pass_obj
def build_sequence(
    wkctx: context.WorkContext,
    build_order_file: str,
    force: bool,
    cache_wheel_server_url: str | None,
) -> None:
    """Build a sequence of wheels in order

    BUILD_ORDER_FILE is the build-order.json files to build

    SDIST_SERVER_URL is the URL for a PyPI-compatible package index hosting sdists

    Performs the equivalent of the 'build' command for each item in
    the build order file.

    """
    server.start_wheel_server(wkctx)
    wheel_server_urls = [wkctx.wheel_server_url]
    if cache_wheel_server_url:
        # put after local server so we always check local server first
        wheel_server_urls.append(cache_wheel_server_url)

    if force:
        logger.info(f"rebuilding all wheels even if they exist in {wheel_server_urls}")
    else:
        logger.info(
            f"skipping builds for versions of packages available at {wheel_server_urls}"
        )

    entries: list[BuildSequenceEntry] = []

    logger.info("reading build order from %s", build_order_file)
    with read.open_file_or_url(build_order_file) as f:
        for entry in progress.progress(json.load(f)):
            dist_name = entry["dist"]
            resolved_version = Version(entry["version"])
            prebuilt = entry["prebuilt"]
            source_download_url = entry["source_url"]

            req = Requirement(f"{dist_name}=={resolved_version}")
            token = requirement_ctxvar.set(req)

            if not force:
                is_built, wheel_filename = _is_wheel_built(
                    wkctx, dist_name, resolved_version, wheel_server_urls
                )
                if is_built:
                    logger.info(
                        "%s: skipping building wheel for %s==%s since it already exists",
                        dist_name,
                        dist_name,
                        resolved_version,
                    )
                    entries.append(
                        BuildSequenceEntry(
                            dist_name,
                            resolved_version,
                            prebuilt,
                            source_download_url,
                            wheel_filename=wheel_filename,
                            skipped=True,
                        )
                    )
                    continue

            if prebuilt:
                logger.info(
                    "%s: downloading prebuilt wheel %s==%s",
                    dist_name,
                    dist_name,
                    resolved_version,
                )
                wheel_filename = wheels.download_wheel(
                    req=req,
                    wheel_url=source_download_url,
                    output_directory=wkctx.wheels_build,
                )
                hooks.run_prebuilt_wheel_hooks(
                    ctx=wkctx,
                    req=req,
                    dist_name=dist_name,
                    dist_version=str(resolved_version),
                    wheel_filename=wheel_filename,
                )
            else:
                logger.info(
                    "%s: building %s==%s", dist_name, dist_name, resolved_version
                )
                wheel_filename = _build(
                    wkctx, resolved_version, req, source_download_url
                )

            server.update_wheel_mirror(wkctx)
            # After we update the wheel mirror, the built file has
            # moved to a new directory.
            wheel_filename = wkctx.wheels_downloads / wheel_filename.name
            entries.append(
                BuildSequenceEntry(
                    dist_name,
                    resolved_version,
                    prebuilt,
                    source_download_url,
                    wheel_filename=wheel_filename,
                )
            )
            print(wheel_filename)
            requirement_ctxvar.reset(token)
    metrics.summarize(wkctx, "Building")

    _summary(wkctx, entries)


build_sequence._fromager_show_build_settings = True  # type: ignore


def _summary(ctx: context.WorkContext, entries: list[BuildSequenceEntry]) -> None:
    output: list[typing.Any] = []
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    output.append(Text(f"Build sequence summary {now}\n"))

    built_entries = [e for e in entries if not e.skipped and not e.prebuilt]
    if built_entries:
        output.append(
            _create_table(
                built_entries,
                title="New builds",
                box=rich.box.MARKDOWN,
                title_justify="left",
            )
        )
    else:
        output.append(Text("No new builds\n"))

    prebuilt_entries = [e for e in entries if e.prebuilt]
    if prebuilt_entries:
        output.append(
            _create_table(
                prebuilt_entries,
                title="Prebuilt wheels",
                box=rich.box.MARKDOWN,
                title_justify="left",
            )
        )
    else:
        output.append(Text("No pre-built wheels\n"))

    skipped_entries = [e for e in entries if e.skipped and not e.prebuilt]
    if skipped_entries:
        output.append(
            _create_table(
                skipped_entries,
                title="Skipped existing builds",
                box=rich.box.MARKDOWN,
                title_justify="left",
            )
        )
    else:
        output.append(Text("No skipped builds\n"))

    console = rich.get_console()
    console.print(*output, sep="\n\n")

    with open(ctx.work_dir / "build-sequence-summary.md", "w", encoding="utf-8") as f:
        console = rich.console.Console(file=f, width=sys.maxsize)
        console.print(*output, sep="\n\n")

    with open(ctx.work_dir / "build-sequence-summary.json", "w", encoding="utf-8") as f:
        json.dump(
            [
                dataclasses.asdict(e, dict_factory=BuildSequenceEntry.dict_factory)
                for e in entries
            ],
            f,
        )


def _create_table(entries: list[BuildSequenceEntry], **table_kwargs) -> Table:
    table = Table(**table_kwargs)
    table.add_column("Name", justify="right", no_wrap=True)
    table.add_column("Version", no_wrap=True)
    table.add_column("Wheel", no_wrap=True)
    table.add_column("Source URL")

    platlib_count = 0

    for info in sorted(entries):
        tags = parse_wheel_filename(info.wheel_filename.name)[3]
        if any(t.platform != "any" or t.abi != "none" for t in tags):
            platlib_count += 1
        source_filename = urlparse(info.download_url).path.rsplit("/", 1)[-1]
        table.add_row(
            info.name,
            str(info.version),
            info.wheel_filename.name,
            # escape Rich markup
            rf"\[{source_filename}]({info.download_url})",
        )

    # summary
    table.add_section()
    table.add_row(
        f"total: {len(entries)}",
        None,
        f"platlib: {platlib_count}",
        None,
    )
    return table


def _build(
    wkctx: context.WorkContext,
    resolved_version: Version,
    req: Requirement,
    source_download_url: str,
) -> pathlib.Path:
    per_wheel_logger = logging.getLogger("")

    module_name = overrides.pkgname_to_override_module(req.name)
    log_filename = module_name + "_current.log"

    wheel_log = wkctx.logs_dir / log_filename

    file_handler = logging.FileHandler(str(wheel_log), mode="w")
    per_wheel_logger.addHandler(file_handler)

    source_filename = sources.download_source(
        ctx=wkctx,
        req=req,
        version=resolved_version,
        download_url=source_download_url,
    )
    logger.debug(
        "%s: saved sdist of version %s from %s to %s",
        req.name,
        resolved_version,
        source_download_url,
        source_filename,
    )

    # Prepare source
    source_root_dir = sources.prepare_source(
        ctx=wkctx, req=req, source_filename=source_filename, version=resolved_version
    )

    # Build environment
    build_env = build_environment.prepare_build_environment(
        ctx=wkctx, req=req, sdist_root_dir=source_root_dir
    )

    # Make a new source distribution, in case we patched the code.
    sdist_filename = sources.build_sdist(
        ctx=wkctx,
        req=req,
        version=resolved_version,
        sdist_root_dir=source_root_dir,
        build_env=build_env,
    )

    # Build
    wheel_filename = wheels.build_wheel(
        ctx=wkctx,
        req=req,
        sdist_root_dir=source_root_dir,
        version=resolved_version,
        build_env=build_env,
    )

    per_wheel_logger.removeHandler(file_handler)
    file_handler.close()

    new_filename = wheel_log.with_name(wheel_filename.stem + ".log")
    wheel_log.rename(new_filename)

    hooks.run_post_build_hooks(
        ctx=wkctx,
        req=req,
        dist_name=canonicalize_name(req.name),
        dist_version=str(resolved_version),
        sdist_filename=sdist_filename,
        wheel_filename=wheel_filename,
    )

    return wheel_filename


def _is_wheel_built(
    wkctx: context.WorkContext,
    dist_name: str,
    resolved_version: Version,
    wheel_server_urls: list[str],
) -> tuple[True, pathlib.Path] | tuple[False, None]:
    req = Requirement(f"{dist_name}=={resolved_version}")

    try:
        logger.info(f"checking if {req} was already built")
        url, _ = wheels.resolve_prebuilt_wheel(
            ctx=wkctx,
            req=req,
            wheel_server_urls=wheel_server_urls,
        )
        pbi = wkctx.package_build_info(req)
        build_tag_from_settings = pbi.build_tag(resolved_version)
        build_tag = build_tag_from_settings if build_tag_from_settings else (0, "")
        wheel_filename = urlparse(url).path.rsplit("/", 1)[-1]
        _, _, build_tag_from_name, _ = parse_wheel_filename(wheel_filename)
        existing_build_tag = build_tag_from_name if build_tag_from_name else (0, "")
        if (
            existing_build_tag[0] > build_tag[0]
            and existing_build_tag[1] == build_tag[1]
        ):
            raise ValueError(
                f"{dist_name}: changelog for version {resolved_version} is inconsistent. Found build tag {existing_build_tag} but expected {build_tag}"
            )
        is_built = existing_build_tag == build_tag
        if is_built and wkctx.wheel_server_url not in url:
            # if the found wheel was on an external server, then download it
            wheels.download_wheel(req, url, wkctx.wheels_downloads)
            server.update_wheel_mirror(wkctx)

        return is_built, pathlib.Path(wheel_filename)
    except Exception:
        logger.info(f"could not locate prebuilt wheel. Will build {req}")
        return False, None


def _build_parallel(
    wkctx: context.WorkContext,
    resolved_version: Version,
    req: Requirement,
    source_download_url: str,
) -> pathlib.Path:
    try:
        token = requirement_ctxvar.set(req)
        return _build(wkctx, resolved_version, req, source_download_url)
    finally:
        requirement_ctxvar.reset(token)


@click.command()
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    help="rebuild wheels even if they have already been built",
)
@click.option(
    "-c",
    "--cache-wheel-server-url",
    "cache_wheel_server_url",
    help="url to a wheel server from where fromager can check if it had already built the wheel",
)
@click.option(
    "-j",
    "--jobs",
    type=int,
    default=None,
    help="number of parallel jobs to run (default: unlimited)",
)
@click.argument("graph_file")
@click.pass_obj
def build_parallel(
    wkctx: context.WorkContext,
    graph_file: str,
    force: bool,
    cache_wheel_server_url: str | None,
    jobs: int | None,
) -> None:
    """Build wheels in parallel based on a dependency graph

    GRAPH_FILE is a graph.json file containing the dependency relationships between packages

    Performs parallel builds of wheels based on their dependency relationships.
    Packages that have no dependencies or whose dependencies are already built
    can be built concurrently. By default, all possible packages are built in
    parallel. Use --jobs to limit the number of concurrent builds.

    """
    server.start_wheel_server(wkctx)
    wheel_server_urls = [wkctx.wheel_server_url]
    if cache_wheel_server_url:
        # put after local server so we always check local server first
        wheel_server_urls.append(cache_wheel_server_url)

    if force:
        logger.info(f"rebuilding all wheels even if they exist in {wheel_server_urls}")
    else:
        logger.info(
            f"skipping builds for versions of packages available at {wheel_server_urls}"
        )

    # Load the dependency graph
    logger.info("reading dependency graph from %s", graph_file)
    graph = dependency_graph.DependencyGraph.from_file(graph_file)

    # Get all nodes that need to be built (excluding prebuilt ones and the root node)
    nodes_to_build = []
    for node in graph.nodes.values():
        # Skip the root node and prebuilt nodes
        if node.key == dependency_graph.ROOT or node.pre_built:
            continue
        if not force:
            is_built, wheel_filename = _is_wheel_built(
                wkctx, node.canonicalized_name, node.version, wheel_server_urls
            )
            if is_built:
                logger.info(
                    "%s: skipping building wheel for %s==%s since it already exists",
                    node.canonicalized_name,
                    node.canonicalized_name,
                    node.version,
                )
                continue
        nodes_to_build.append(node)
    logger.info("found %d packages to build", len(nodes_to_build))

    # Sort nodes by their dependencies to ensure we build in the right order
    # A node can be built when all of its build dependencies are built
    built_nodes = set()
    entries: list[BuildSequenceEntry] = []

    with progress.progress_context(total=len(nodes_to_build)) as progressbar:
        while nodes_to_build:
            # Find nodes that can be built (all build dependencies are built)
            buildable_nodes = []
            for node in nodes_to_build:
                # Get all build dependencies (build-system, build-backend, build-sdist)
                build_deps = [
                    edge.destination_node
                    for edge in node.children
                    if edge.req_type.is_build_requirement
                ]
                # A node can be built when all of its build dependencies are built
                if all(dep.key in built_nodes for dep in build_deps):
                    buildable_nodes.append(node)

            if not buildable_nodes:
                # If we can't build anything but still have nodes, we have a cycle
                remaining = [n.key for n in nodes_to_build]
                raise ValueError(f"Circular dependency detected among: {remaining}")
            logger.info(
                "ready to build: %s",
                ", ".join(n.canonicalized_name for n in buildable_nodes),
            )

            # Build up to jobs nodes concurrently (or all if jobs is None)
            with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
                futures = []
                for node in buildable_nodes:
                    req = Requirement(f"{node.canonicalized_name}=={node.version}")
                    futures.append(
                        executor.submit(
                            _build_parallel,
                            wkctx,
                            node.version,
                            req,
                            node.download_url,
                        )
                    )

                # Wait for all builds to complete
                for node, future in zip(buildable_nodes, futures, strict=True):
                    try:
                        wheel_filename = future.result()
                        server.update_wheel_mirror(wkctx)
                        # After we update the wheel mirror, the built file has
                        # moved to a new directory.
                        wheel_filename = wkctx.wheels_downloads / wheel_filename.name
                        entries.append(
                            BuildSequenceEntry(
                                node.canonicalized_name,
                                node.version,
                                False,  # not prebuilt
                                node.download_url,
                                wheel_filename=wheel_filename,
                            )
                        )
                        built_nodes.add(node.key)
                        nodes_to_build.remove(node)
                        progressbar.update()
                    except Exception as e:
                        logger.error(f"Failed to build {node.key}: {e}")
                        raise

    metrics.summarize(wkctx, "Building in parallel")
    _summary(wkctx, entries)


build_parallel._fromager_show_build_settings = True  # type: ignore
