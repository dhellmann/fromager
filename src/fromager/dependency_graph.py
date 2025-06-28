import json
import logging
import pathlib
import typing

from packaging.requirements import Requirement
from packaging.utils import NormalizedName, canonicalize_name
from packaging.version import Version

from .read import open_file_or_url
from .requirements_file import RequirementType

logger = logging.getLogger(__name__)

ROOT = ""


class DependencyEdgeDict(typing.TypedDict):
    req_type: str
    req: str
    key: str


class DependencyNodeDict(typing.TypedDict):
    download_url: str
    canonicalized_name: str
    version: str
    pre_built: bool
    edges: list[DependencyEdgeDict]


class DependencyNode:
    def __init__(
        self,
        req_name: NormalizedName,
        version: Version,
        download_url: str = "",
        pre_built: bool = False,
    ) -> None:
        self.canonicalized_name = req_name
        self.version = version
        self.parents: list[DependencyEdge] = []
        self.children: list[DependencyEdge] = []
        self.key = f"{self.canonicalized_name}=={version}"
        self.download_url = download_url
        self.pre_built = pre_built

    def __repr__(self) -> str:
        return f"{self.canonicalized_name}=={self.version}"

    def add_child(
        self,
        child: "DependencyNode",
        req: Requirement,
        req_type: RequirementType,
    ):
        current_to_child_edge = DependencyEdge(
            req=req, req_type=req_type, destination_node=child
        )
        self.children.append(current_to_child_edge)
        child_to_current_edge = DependencyEdge(
            req=req, req_type=req_type, destination_node=self
        )
        # will create a cyclic dependency in memory, which will make it harder to garbage collect
        # not an issue for fromager since it is used as a short-lived process
        child.parents.append(child_to_current_edge)

    def get_incoming_install_edges(self) -> list["DependencyEdge"]:
        return [
            edge for edge in self.parents if edge.req_type == RequirementType.INSTALL
        ]

    def to_dict(self) -> DependencyNodeDict:
        return {
            "download_url": self.download_url,
            "pre_built": self.pre_built,
            "version": str(self.version),
            "canonicalized_name": str(self.canonicalized_name),
            "edges": [edge.to_dict() for edge in self.children],
        }

    def get_outgoing_edges(
        self, req_name: str, req_type: RequirementType
    ) -> list["DependencyEdge"]:
        return [
            edge
            for edge in self.children
            if canonicalize_name(edge.req.name) == canonicalize_name(req_name)
            and edge.req_type == req_type
        ]

    @classmethod
    def construct_root_node(cls) -> "DependencyNode":
        root = cls(
            canonicalize_name(ROOT), Version("0")
        )  # version doesn't really matter for root
        root.key = ROOT  # root has a special key type
        return root


class DependencyEdge:
    def __init__(
        self,
        destination_node: DependencyNode,
        req_type: RequirementType,
        req: Requirement,
    ) -> None:
        self.req_type = req_type
        self.req = req
        self.destination_node = destination_node

    def to_dict(self) -> DependencyEdgeDict:
        return {
            "key": self.destination_node.key,
            "req_type": str(self.req_type),
            "req": str(self.req),
        }

    def __repr__(self) -> str:
        return f"{self.destination_node} ({self.req})"


class DependencyGraph:
    def __init__(self) -> None:
        self.nodes: dict[str, DependencyNode] = {}
        root = DependencyNode.construct_root_node()
        self.nodes[ROOT] = root

    @classmethod
    def from_file(
        cls,
        graph_file: pathlib.Path | str,
    ) -> "DependencyGraph":
        with open_file_or_url(graph_file) as f:
            # TODO: add JSON validation to ensure it is a parsable graph json
            raw_graph = typing.cast(dict[str, dict], json.load(f))
            return cls.from_dict(raw_graph)

    @classmethod
    def from_dict(
        cls,
        graph_dict: dict[str, dict[str, typing.Any]],
    ) -> "DependencyGraph":
        graph = cls()
        stack = [ROOT]
        visited = set()
        while stack:
            curr_key = stack.pop()
            if curr_key in visited:
                continue
            node_dict = typing.cast(DependencyNodeDict, graph_dict[curr_key])
            parent_name = parent_version = None
            if curr_key != ROOT:
                parent_name = canonicalize_name(node_dict["canonicalized_name"])
                parent_version = Version(node_dict["version"])
            for raw_edge in node_dict["edges"]:
                edge_dict = typing.cast(DependencyEdgeDict, raw_edge)
                destination_node_dict = typing.cast(
                    DependencyNodeDict, graph_dict[edge_dict["key"]]
                )
                graph.add_dependency(
                    parent_name=parent_name,
                    parent_version=parent_version,
                    req_type=RequirementType(edge_dict["req_type"]),
                    req=Requirement(edge_dict["req"]),
                    req_version=Version(destination_node_dict["version"]),
                    download_url=destination_node_dict["download_url"],
                    pre_built=destination_node_dict["pre_built"],
                )
                stack.append(edge_dict["key"])
            visited.add(curr_key)
        return graph

    def _to_dict(self):
        raw_graph = {}
        stack = [self.nodes[ROOT]]
        visited = set()
        while stack:
            node = stack.pop()
            raw_graph[node.key] = node.to_dict()
            for child in node.children:
                if child.destination_node.key not in visited:
                    stack.append(child.destination_node)
            visited.add(node.key)
        return raw_graph

    def serialize(self, file_handle: typing.TextIO):
        raw_graph = self._to_dict()
        json.dump(raw_graph, file_handle, indent=2, default=str)

    def _add_node(
        self,
        req_name: NormalizedName,
        version: Version,
        download_url: str,
        pre_built: bool,
    ):
        new_node = DependencyNode(
            req_name=req_name,
            version=version,
            download_url=download_url,
            pre_built=pre_built,
        )
        # check if a node with that key already exists. if it does then use that
        node = self.nodes.get(new_node.key, new_node)
        # store the node in case we are using new_node
        self.nodes[node.key] = node
        return node

    def add_dependency(
        self,
        parent_name: NormalizedName | None,
        parent_version: Version | None,
        req_type: RequirementType,
        req: Requirement,
        req_version: Version,
        download_url: str = "",
        pre_built: bool = False,
    ) -> None:
        logger.debug(
            "recording %s %s dependency %s -> %s %s",
            req_type,
            parent_name if parent_name else f"({RequirementType.TOP_LEVEL})",
            f"=={parent_version}" if parent_version else "",
            req.name,
            req_version,
        )

        node = self._add_node(
            req_name=canonicalize_name(req.name),
            version=req_version,
            download_url=download_url,
            pre_built=pre_built,
        )

        parent_key = ROOT if parent_name is None else f"{parent_name}=={parent_version}"
        if parent_key not in self.nodes:
            raise ValueError(
                f"Trying to add {node.key} to parent {parent_key} but {parent_key} does not exist"
            )

        self.nodes[parent_key].add_child(node, req=req, req_type=req_type)

    def get_dependency_edges(
        self, match_dep_types: list[RequirementType] | None = None
    ) -> typing.Iterable[DependencyEdge]:
        visited = set()
        for edge in self._depth_first_traversal(
            self.nodes[ROOT].children,
            set(),
            match_dep_types=match_dep_types,
        ):
            if edge.destination_node.key not in visited:
                yield edge
                visited.add(edge.destination_node.key)

    def get_install_dependencies(self) -> typing.Iterable[DependencyNode]:
        for edge in self.get_dependency_edges(
            match_dep_types=[RequirementType.INSTALL, RequirementType.TOP_LEVEL]
        ):
            yield edge.destination_node

    def get_nodes_by_name(self, req_name: str | None) -> list[DependencyNode]:
        if not req_name:
            return [self.nodes[ROOT]]
        return [
            node
            for node in self.get_all_nodes()
            if node.canonicalized_name == canonicalize_name(req_name)
        ]

    def get_root_node(self) -> DependencyNode:
        return self.nodes[ROOT]

    def get_all_nodes(self) -> typing.Iterable[DependencyNode]:
        return self.nodes.values()

    def get_install_dependency_versions(
        self,
    ) -> dict[NormalizedName, list[DependencyNode]]:
        all_versions: dict[NormalizedName, list[DependencyNode]] = {}
        for node in self.get_install_dependencies():
            all_versions.setdefault(node.canonicalized_name, []).append(node)
        return all_versions

    def _depth_first_traversal(
        self,
        start_edges: list[DependencyEdge],
        visited: set[str],
        match_dep_types: list[RequirementType] | None = None,
    ) -> typing.Iterable[DependencyEdge]:
        for edge in start_edges:
            if edge.destination_node.key in visited:
                continue
            if match_dep_types and edge.req_type not in match_dep_types:
                continue
            visited.add(edge.destination_node.key)
            yield edge
            yield from self._depth_first_traversal(
                edge.destination_node.children, visited, match_dep_types
            )
