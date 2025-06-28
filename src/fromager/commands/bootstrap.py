import contextvars
import logging
import typing

import click
from packaging.requirements import Requirement
from packaging.utils import NormalizedName
from packaging.version import Version

from fromager.dependency_graph import DependencyEdge, DependencyNode

from .. import (
    bootstrapper,
    context,
    dependency_graph,
    metrics,
    progress,
    requirements_file,
    server,
)
from ..log import requirement_ctxvar
from ..requirements_file import RequirementType
from .graph import find_why

# Map child_name==child_version to list of (parent_name==parent_version, Requirement)
ReverseRequirements = dict[str, list[tuple[str, Requirement]]]

logger = logging.getLogger(__name__)


def _get_requirements_from_args(
    toplevel: typing.Iterable[str],
    req_files: typing.Iterable[str],
) -> list[Requirement]:
    parsed_req: list[str] = []
    parsed_req.extend(toplevel)
    for filename in req_files:
        parsed_req.extend(requirements_file.parse_requirements_file(filename))
    to_build: list[Requirement] = []
    for dep in parsed_req:
        req = Requirement(dep)
        # If we're given a requirements file as input, we might be iterating over a
        # list of requirements with marker expressions that limit their use to
        # specific platforms or python versions. Evaluate the markers to filter out
        # anything we shouldn't build. Only apply the filter to toplevel
        # requirements (items without a why list leading up to them) because other
        # dependencies are already filtered based on their markers in the context of
        # their parent, so they include values like the parent's extras settings.
        if not requirements_file.evaluate_marker(req, req):
            logger.info(
                f"ignoring {requirements_file.RequirementType.TOP_LEVEL} dependency {req} because of its marker expression"
            )
        else:
            to_build.append(req)
    return to_build


@click.command()
@click.option(
    "-r",
    "--requirements-file",
    "requirements_files",
    multiple=True,
    type=str,
    help="pip requirements file",
)
@click.option(
    "-p",
    "--previous-bootstrap-file",
    "previous_bootstrap_file",
    type=str,
    help="graph file produced from a previous bootstrap",
)
@click.option(
    "-c",
    "--cache-wheel-server-url",
    "cache_wheel_server_url",
    help="url to a wheel server from where fromager can download the wheels that it has built before",
)
@click.option(
    "--sdist-only/--full-build",
    "sdist_only",
    default=False,
    help=(
        "--sdist-only (fast mode) does not build missing wheels unless they "
        "are build requirements. --full-build (default) builds all missing "
        "wheels."
    ),
)
@click.option(
    "--skip-constraints",
    "skip_constraints",
    is_flag=True,
    default=False,
    help="Skip generating constraints.txt file to allow building collections with conflicting versions",
)
@click.argument("toplevel", nargs=-1)
@click.pass_obj
def bootstrap(
    wkctx: context.WorkContext,
    requirements_files: list[str],
    previous_bootstrap_file: str | None,
    cache_wheel_server_url: str | None,
    sdist_only: bool,
    skip_constraints: bool,
    toplevel: list[str],
) -> None:
    """Compute and build the dependencies of a set of requirements recursively

    TOPLEVEL is a requirements specification, including a package name
    and optional version constraints.

    """
    logger.info(f"cache wheel server url: {cache_wheel_server_url}")

    to_build = _get_requirements_from_args(toplevel, requirements_files)
    if not to_build:
        raise RuntimeError(
            "Pass a requirement specificiation or use -r to pass a requirements file"
        )
    logger.info("bootstrapping %r variant of %s", wkctx.variant, to_build)

    if previous_bootstrap_file:
        logger.info("reading previous bootstrap data from %s", previous_bootstrap_file)
        prev_graph = dependency_graph.DependencyGraph.from_file(previous_bootstrap_file)
    else:
        logger.info("no previous bootstrap data")
        prev_graph = None

    if sdist_only:
        logger.info("sdist-only (fast mode), getting metadata from sdists")
    else:
        logger.info("build all missing wheels")

    pre_built = wkctx.settings.list_pre_built()
    if pre_built:
        logger.info("treating %s as pre-built wheels", sorted(pre_built))

    server.start_wheel_server(wkctx)

    with progress.progress_context(total=len(to_build * 2)) as progressbar:
        bt = bootstrapper.Bootstrapper(
            wkctx,
            progressbar,
            prev_graph,
            cache_wheel_server_url,
            sdist_only=sdist_only,
        )

        # we need to resolve all the top level dependencies before we start bootstrapping.
        # this is to ensure that if we are using an older bootstrap to resolve packages
        # we are able to upgrade a package anywhere in the dependency tree if it is mentioned
        # in the toplevel without having to fall back to history
        logger.info("resolving top-level dependencies before building")
        for req in to_build:
            token = requirement_ctxvar.set(req)
            pbi = wkctx.package_build_info(req)
            if pbi.pre_built:
                source_url, version = bt.resolve_version(
                    req=req,
                    req_type=RequirementType.TOP_LEVEL,
                )
            else:
                source_url, version = bt.resolve_version(
                    req=req,
                    req_type=RequirementType.TOP_LEVEL,
                )
            logger.info("%s resolves to %s", req, version)
            wkctx.dependency_graph.add_dependency(
                parent_name=None,
                parent_version=None,
                req_type=requirements_file.RequirementType.TOP_LEVEL,
                req=req,
                req_version=version,
                download_url=source_url,
                pre_built=pbi.pre_built,
            )
            requirement_ctxvar.reset(token)

        for req in to_build:
            token = requirement_ctxvar.set(req)
            bt.bootstrap(req, requirements_file.RequirementType.TOP_LEVEL)
            progressbar.update()
            requirement_ctxvar.reset(token)

    # If we put pre-built wheels in the downloads directory, we should
    # remove them so we can treat that directory as a source of wheels
    # to upload to an index.
    for prebuilt_wheel in wkctx.wheels_prebuilt.glob("*.whl"):
        filename = wkctx.wheels_downloads / prebuilt_wheel.name
        if filename.exists():
            logger.info(
                f"removing prebuilt wheel {prebuilt_wheel.name} from download cache"
            )
            filename.unlink()

    constraints_filename = wkctx.work_dir / "constraints.txt"
    if skip_constraints:
        logger.info("skipping constraints.txt generation as requested")
    else:
        logger.info(f"writing installation dependencies to {constraints_filename}")
        with open(constraints_filename, "w") as f:
            if not write_constraints_file(graph=wkctx.dependency_graph, output=f):
                raise ValueError(
                    f"Could not produce a pip compatible constraints file. Please review {constraints_filename} for more details"
                )

    metrics.summarize(wkctx, "Bootstrapping")


def write_constraints_file(
    graph: dependency_graph.DependencyGraph,
    output: typing.TextIO,
) -> bool:
    # Look for potential conflicts by tracking how many different versions of
    # each package are needed.
    conflicts: dict[NormalizedName, list[DependencyNode]] = (
        graph.get_install_dependency_versions()
    )
    ret = True

    # Map for already resolved versions for a given dependency Eg: {"a": "0.4"}
    resolved: dict[NormalizedName, Version] = {}

    nodes_by_key: dict[str, DependencyNode] = {
        node.key: node for node in graph.get_all_nodes()
    }

    # List of unresolved dependencies
    unresolved_dependencies: list[tuple[NormalizedName, list[DependencyNode]]] = sorted(
        conflicts.items()
    )

    dep_name: NormalizedName
    token: contextvars.Token[Requirement] | None = None

    # Loop over dependencies and resolve dependencies with single version first.
    # This will shrink the unresolved_dependencies to begin with.
    for dep_name, nodes in unresolved_dependencies[:]:
        token = requirement_ctxvar.set(Requirement(dep_name))
        versions: list[Version] = [node.version for node in nodes]
        if len(versions) == 0:
            # This should never happen.
            raise ValueError(f"No versions of {dep_name} supported")

        if len(versions) == 1:
            logger.debug(f"using {versions[0]} for {dep_name}")
            # This is going to be the situation for most dependencies, where we
            # only have one version.
            resolved[dep_name] = versions[0]
            # Remove from unresolved dependencies list
            unresolved_dependencies.remove((dep_name, nodes))
        requirement_ctxvar.reset(token)

    multiple_versions: dict[NormalizedName, list[DependencyNode]] = dict(
        unresolved_dependencies
    )

    # Below this point we have built multiple versions of the same thing, so
    # we need to try to determine if any one of those versions meets all of
    # the requirements.

    # Track the names of packages that we can't resolve.
    failed_to_resolve: set[NormalizedName] = set()

    # Flag to see if something is resolved
    resolved_something: bool = True

    # Outer while loop to resolve remaining dependencies with multiple versions
    while unresolved_dependencies and resolved_something:
        resolved_something = False

        # Make copy of the original list and loop over unresolved dependencies
        token = None
        for dep_name, nodes in unresolved_dependencies[:]:
            # Set up the requirement context variable used by the logger to show
            # the name of the package we are trying to resolve.
            if token:
                requirement_ctxvar.reset(token)
            token = requirement_ctxvar.set(Requirement(dep_name))

            # Track all of the versions of the dependency that we have to choose from
            candidate_versions: list[Version] = [node.version for node in nodes]
            logger.debug(f"candidate versions of {dep_name} are {candidate_versions}")

            # Track which versions can be used by the users of the dependency.
            # This set is modified as we determine candidates that are not usable.
            usable_versions: set[Version] = set(candidate_versions)

            # Find a list of all of the packages that ask for any version of this dependency
            edges_to_dep: list[DependencyEdge] = []
            for node in nodes:
                edges_to_dep.extend(node.get_incoming_install_edges())
            logger.debug(f"{len(edges_to_dep)} users of {dep_name} are {edges_to_dep}")

            # Reduce the list of users to only those that are already resolved
            # because if there is an unresolved user we don't know which
            # requirement rule might be in effect.
            unresolved_users: set[NormalizedName] = set()
            resolved_users: dict[NormalizedName, DependencyNode] = {}
            for edge in edges_to_dep:
                if edge.destination_node.canonicalized_name in resolved:
                    resolved_users[edge.destination_node.canonicalized_name] = (
                        nodes_by_key[edge.destination_node.key]
                    )
                else:
                    unresolved_users.add(edge.destination_node.canonicalized_name)
            if unresolved_users:
                logger.debug(
                    f"skipping {dep_name} because it has unresolved users {sorted(unresolved_users)}"
                )
                continue
            logger.debug(f"resolved users of {dep_name} are {resolved_users}")

            # Loop over the users of the dependency and find the versions that
            # they can use.
            for _, node in resolved_users.items():
                for edge in node.children:
                    if edge.req.name != dep_name:
                        continue
                    logger.debug(f"{node} asks for {edge.req}")
                    can_use = set(edge.req.specifier.filter(candidate_versions))
                    logger.debug(f"matches candidates {can_use}")
                    unusable_versions = usable_versions - can_use
                    if unusable_versions:
                        logger.debug(f"ruled out candidates {unusable_versions}")
                    usable_versions = usable_versions - unusable_versions
                    logger.debug(f"remaining candidates {usable_versions}")
                    if not usable_versions:
                        logger.error(f"no version of {dep_name} met all requirements")
                        # We've run out of versions that can be used by all of
                        # the users of the dependency. Break out of the loop so
                        # that error handling later can report more details.
                        failed_to_resolve.add(dep_name)
                        break
                if not usable_versions:
                    # Break out of the outer loop.
                    break

            if usable_versions:
                # We have at least one version that can be used by all of the users
                # of the dependency. Pick the highest version, and remove the
                # package from the list of unresolved dependencies.
                version_to_use: Version = max(usable_versions)
                logger.debug(f"using {version_to_use} for {dep_name}")
                if len(usable_versions) > 1:
                    logger.info(
                        f"selecting {version_to_use} for {dep_name} from {usable_versions}"
                    )
                    multiple_versions[dep_name] = nodes
                resolved[dep_name] = version_to_use
                try:
                    unresolved_dependencies.remove((dep_name, nodes))
                except ValueError:
                    logger.debug(
                        f"{dep_name} not in unresolved dependencies list, ignoring"
                    )

                # We've resolved something, so when we finish this iteration of
                # the loop we should do one more if there are unresolved
                # dependencies.
                resolved_something = True

        # Reset the requirement context variable used by the logger.
        if token:
            requirement_ctxvar.reset(token)

    # Write resolved versions to constraints file. We do this regardless of
    # whether we could resolve all dependencies because we can at least show
    # what did work and that's useful when the output  is saved as part of a job
    # log.
    for dep_name, resolved_version in sorted(resolved.items()):  # type: ignore
        if dep_name in multiple_versions:
            version_strs = [
                str(node.version)
                for node in sorted(multiple_versions[dep_name], key=lambda n: n.version)
            ]
            output.write(
                f"# NOTE: fromager selected {dep_name}=={resolved_version} from: {version_strs}\n"
            )
        output.write(f"{dep_name}=={resolved_version}\n")

    # We differentiate between failed to resolve and unresolved dependencies.
    # Here we report things we just never resolved. Next we report in more
    # detail the things that failed to resolve.
    if unresolved_dependencies:
        logger.warning(
            f"resolution terminated before resolving {sorted(n for n, _ in unresolved_dependencies)}"
        )

    # No single version could be used, so go ahead and print all the
    # versions with a warning message
    for dep_name in failed_to_resolve:  # type: ignore
        # Make sure we return a failure.
        ret = False

        nodes = graph.get_nodes_by_name(dep_name)
        msg = f"# ERROR: no single version of {dep_name} met all requirements\n"
        output.write(msg)
        for node in sorted(nodes, key=lambda n: n.version):
            output.write(f"{dep_name}=={node.version}\n")

        print(f"finding why {dep_name} was being used")
        for node in graph.get_nodes_by_name(dep_name):
            find_why(graph, node, 0, 0, [])

    return ret


bootstrap._fromager_show_build_settings = True  # type: ignore
