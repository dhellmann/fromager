# Based on https://github.com/sarugaku/resolvelib/blob/main/examples/pypi_wheel_provider.py
#
# Modified to look at sdists instead of wheels and to avoid trying to
# resolve any dependencies.
#
from __future__ import annotations

import functools
import logging
import os
import re
import typing
from collections import defaultdict
from collections.abc import Iterable
from operator import attrgetter
from platform import python_version
from urllib.parse import quote, unquote, urljoin, urlparse

import html5lib
import resolvelib
from packaging.requirements import Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.tags import Tag, sys_tags
from packaging.utils import (
    BuildTag,
    canonicalize_name,
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import Version
from requests.models import Response
from resolvelib.resolvers import RequirementInformation

from . import overrides
from .candidate import Candidate
from .constraints import Constraints
from .extras_provider import ExtrasProvider
from .request_session import session
from .requirements_file import RequirementType

if typing.TYPE_CHECKING:
    from . import context

logger = logging.getLogger(__name__)

PYTHON_VERSION = Version(python_version())
DEBUG_RESOLVER = os.environ.get("DEBUG_RESOLVER", "")
PYPI_SERVER_URL = "https://pypi.org/simple"
GITHUB_URL = "https://github.com"

# all supported tags
SUPPORTED_TAGS: frozenset[Tag] = frozenset(sys_tags())
# same, but ignore the platform for 'ignore_platform' flag
IGNORE_PLATFORM: str = "ignore"
SUPPORTED_TAGS_IGNORE_PLATFORM: frozenset[Tag] = frozenset(
    Tag(t.interpreter, t.abi, IGNORE_PLATFORM) for t in SUPPORTED_TAGS
)


@functools.lru_cache(maxsize=200)
def match_py_req(py_req: str, *, python_version: Version = PYTHON_VERSION) -> bool:
    """Python version requirement lookup with LRU cache

    Raises InvalidSpecifier on error

    SpecifierSet parsing and matching takes a non-trivial amount of time. A
    bootstrap run can spend over 10% of its time in parsing and matching
    Python version requirements.

    This function caches the result of SpecifierSet parsing and contains
    operation for Python version requirement. Most packages share similar
    constraints, e.g. ``>= 3.10``.
    """
    return python_version in SpecifierSet(py_req)


def resolve(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    sdist_server_url: str,
    include_sdists: bool = True,
    include_wheels: bool = True,
    req_type: RequirementType | None = None,
    ignore_platform: bool = False,
) -> tuple[str, Version]:
    # Create the (reusable) resolver.
    provider = overrides.find_and_invoke(
        req.name,
        "get_resolver_provider",
        default_resolver_provider,
        ctx=ctx,
        req=req,
        include_sdists=include_sdists,
        include_wheels=include_wheels,
        sdist_server_url=sdist_server_url,
        req_type=req_type,
        ignore_platform=ignore_platform,
    )
    return resolve_from_provider(provider, req)


def default_resolver_provider(
    ctx: context.WorkContext,
    req: Requirement,
    sdist_server_url: str,
    include_sdists: bool,
    include_wheels: bool,
    req_type: RequirementType | None = None,
    ignore_platform: bool = False,
) -> PyPIProvider | GenericProvider | GitHubTagProvider:
    """Lookup resolver provider to resolve package versions"""
    return PyPIProvider(
        include_sdists=include_sdists,
        include_wheels=include_wheels,
        sdist_server_url=sdist_server_url,
        constraints=ctx.constraints,
        req_type=req_type,
        ignore_platform=ignore_platform,
    )


def extract_filename_from_url(url: str) -> str:
    """Extract filename from URL and decode it."""
    path = urlparse(url).path
    filename = os.path.basename(path)
    return unquote(filename)


class LogReporter(resolvelib.BaseReporter):
    """Report resolution events

    Implements part of the BaseReporter API to log a few events related to
    resolving requirements.

    """

    def __init__(self, req: Requirement):
        self.req = req
        super().__init__()

    def _report(self, msg: str, *args: typing.Any) -> None:
        logger.info(msg, *args)

    def starting(self) -> None:
        self._report("looking for candidates for %r", self.req)

    def rejecting_candidate(self, criterion, candidate):
        self._report("resolver rejecting candidate %s: %s", candidate, criterion)

    def pinning(self, candidate):
        self._report("selecting %s", candidate)

    def ending(self, state):
        self._report("successfully resolved %r", self.req)


def resolve_from_provider(
    provider: BaseProvider, req: Requirement
) -> tuple[str, Version]:
    reporter = LogReporter(req)
    rslvr: resolvelib.Resolver = resolvelib.Resolver(provider, reporter)
    try:
        result = rslvr.resolve([req])
    except resolvelib.resolvers.ResolverException as err:
        constraint = provider.constraints.get_constraint(req.name)
        raise resolvelib.resolvers.ResolverException(
            f"Unable to resolve requirement specifier {req} with constraint {constraint}"
        ) from err
    # resolvelib actually just returns one candidate per requirement.
    # result.mapping is map from an identifier to its resolved candidate
    candidate: Candidate
    for candidate in result.mapping.values():
        return candidate.url, candidate.version
    raise ValueError(f"Unable to resolve {req}")


def get_project_from_pypi(
    project: str,
    extras: typing.Iterable[str],
    sdist_server_url: str,
    ignore_platform: bool = False,
) -> typing.Iterable[Candidate]:
    """Return candidates created from the project name and extras."""
    found_candidates: set[str] = set()
    ignored_candidates: set[str] = set()
    simple_index_url = sdist_server_url.rstrip("/") + "/" + project + "/"
    logger.debug("%s: getting available versions from %s", project, simple_index_url)

    try:
        response = session.get(simple_index_url)
        response.raise_for_status()
        data = response.content
    except Exception as e:
        logger.debug(
            "%s: failed to fetch package index from %s: %s",
            project,
            simple_index_url,
            e,
        )
        raise

    doc = html5lib.parse(data, namespaceHTMLElements=False)
    for i in doc.findall(".//a"):
        candidate_url = urljoin(simple_index_url, i.attrib["href"])
        py_req = i.attrib.get("data-requires-python")
        # PEP 658: Check for metadata availability (PEP 714 data-core-metadata first)
        dist_info_metadata = i.attrib.get("data-core-metadata") or i.attrib.get(
            "data-dist-info-metadata"
        )
        # PEP 592: Check if package was yanked
        reason_data_yanked = i.attrib.get("data-yanked")
        # file names are URL quoted, "1.0%2Blocal" -> "1.0+local"
        filename = extract_filename_from_url(candidate_url)
        found_candidates.add(filename)
        if DEBUG_RESOLVER:
            logger.debug("%s: candidate %r -> %r", project, candidate_url, filename)

        # PEP 592: Skip items that were yanked
        if reason_data_yanked is not None:
            if DEBUG_RESOLVER:
                logger.debug(
                    "%s: skipping %s because it was yanked (%s)",
                    project,
                    filename,
                    reason_data_yanked if reason_data_yanked else "no reason found",
                )
            ignored_candidates.add(filename)
            continue

        # Construct metadata URL if PEP 658 metadata is available
        metadata_url = None
        if dist_info_metadata:
            # PEP 658: metadata is available at {file_url}.metadata
            metadata_url = candidate_url + ".metadata"
            if DEBUG_RESOLVER:
                logger.debug(
                    "%s: PEP 658 metadata available at %s", project, metadata_url
                )
        # Skip items that need a different Python version
        if py_req:
            try:
                matched_py: bool = match_py_req(py_req)
            except InvalidSpecifier as err:
                # Ignore files with invalid python specifiers
                # e.g. shellingham has files with ">= '2.7'"
                if DEBUG_RESOLVER:
                    logger.debug(
                        f"{project}: skipping {filename} because of an invalid python version specifier {py_req}: {err}"
                    )
                ignored_candidates.add(filename)
                continue
            if not matched_py:
                if DEBUG_RESOLVER:
                    logger.debug(
                        f"{project}: skipping {filename} because of python version {py_req}"
                    )
                ignored_candidates.add(filename)
                continue

        # TODO: Handle compatibility tags?

        try:
            if filename.endswith(".tar.gz") or filename.endswith(".zip"):
                is_sdist = True
                name, version = parse_sdist_filename(filename)
                tags: frozenset[Tag] = frozenset()
                build_tag: BuildTag = ()
            else:
                is_sdist = False
                name, version, build_tag, tags = parse_wheel_filename(filename)
            if tags:
                # FIXME: This doesn't take into account precedence of
                # the supported tags for best fit.
                matching_tags = SUPPORTED_TAGS.intersection(tags)
                if not matching_tags and ignore_platform:
                    if DEBUG_RESOLVER:
                        logger.debug(f"{project}: ignoring platform for {filename}")
                    ignore_platform_tags: frozenset[Tag] = frozenset(
                        Tag(t.interpreter, t.abi, IGNORE_PLATFORM) for t in tags
                    )
                    matching_tags = SUPPORTED_TAGS_IGNORE_PLATFORM.intersection(
                        ignore_platform_tags
                    )
                if not matching_tags:
                    if DEBUG_RESOLVER:
                        logger.debug(f"{project}: ignoring {filename} with tags {tags}")
                    ignored_candidates.add(filename)
                    continue
        except Exception as err:
            # Ignore files with invalid versions
            if DEBUG_RESOLVER:
                logger.debug(
                    f'{project}: could not determine version for "{filename}": {err}'
                )
            ignored_candidates.add(filename)
            continue
        # Look for and ignore cases like `cffi-1.0.2-2.tar.gz` which
        # produces the name `cffi-1-0-2`. We can't just compare the
        # names directly because of case and punctuation changes in
        # making names canonical and the way requirements are
        # expressed and there seems to be *no* way of producing sdist
        # filenames consistently, so we compare the length for this
        # case.
        if len(name) != len(project):
            if DEBUG_RESOLVER:
                logger.debug(f'{project}: skipping invalid filename "{filename}"')
            ignored_candidates.add(filename)
            continue

        c = Candidate(
            name,
            version,
            url=candidate_url,
            extras=extras,
            is_sdist=is_sdist,
            build_tag=build_tag,
            metadata_url=metadata_url,
        )
        if DEBUG_RESOLVER:
            logger.debug(
                "%s: candidate %s (%s) %s", project, filename, c, candidate_url
            )
        yield c

    if not found_candidates:
        logger.info(f"{project}: found no candidate files at {simple_index_url}")
    elif ignored_candidates == found_candidates:
        logger.info(f"{project}: ignored all candidate files at {simple_index_url}")


RequirementsMap: typing.TypeAlias = typing.Mapping[str, typing.Iterable[Requirement]]
CandidatesMap: typing.TypeAlias = typing.Mapping[str, typing.Iterable[Candidate]]
VersionSource: typing.TypeAlias = typing.Callable[
    [str, RequirementsMap, CandidatesMap],
    typing.Iterable[tuple[str, str | Version]],
]


class BaseProvider(ExtrasProvider):
    def __init__(
        self,
        include_sdists: bool = True,
        include_wheels: bool = True,
        sdist_server_url: str = "https://pypi.org/simple/",
        constraints: Constraints | None = None,
        req_type: RequirementType | None = None,
        ignore_platform: bool = False,
    ):
        super().__init__()
        self.include_sdists = include_sdists
        self.include_wheels = include_wheels
        self.sdist_server_url = sdist_server_url
        self.constraints = constraints or Constraints()
        self.req_type = req_type
        self.ignore_platform = ignore_platform

    def identify(self, requirement_or_candidate: Requirement | Candidate) -> str:
        return canonicalize_name(requirement_or_candidate.name)

    def get_extras_for(
        self,
        requirement_or_candidate: Requirement | Candidate,
    ) -> typing.Iterable[str]:
        # Extras is a set, which is not hashable
        if requirement_or_candidate.extras:
            return tuple(sorted(requirement_or_candidate.extras))
        return tuple()

    def get_base_requirement(self, candidate: Candidate) -> Requirement:
        return Requirement(f"{candidate.name}=={candidate.version}")

    def validate_candidate(
        self,
        identifier: str,
        requirements: RequirementsMap,
        incompatibilities: CandidatesMap,
        candidate: Candidate,
    ) -> bool:
        identifier_reqs = list(requirements[identifier])
        bad_versions = {c.version for c in incompatibilities[identifier]}
        # Skip versions that are known bad
        if candidate.version in bad_versions:
            if DEBUG_RESOLVER:
                logger.debug(
                    f"{identifier}: skipping bad version {candidate.version} from {bad_versions}"
                )
            return False
        for r in identifier_reqs:
            if self.is_satisfied_by(requirement=r, candidate=candidate):
                return True
        return False

    def get_cache(self) -> dict[str, list[Candidate]]:
        raise NotImplementedError()

    def get_from_cache(
        self,
        identifier: str,
        requirements: RequirementsMap,
        incompatibilities: CandidatesMap,
    ) -> list[Candidate]:
        cache = self.get_cache()
        # we only want caching for build reqs because for install time reqs we always want to get the latest version
        # we can't guarantee that the latest version is available in the cache so install time reqs cannot use the cache
        if self.req_type is None or not self.req_type.is_build_requirement:
            return []
        if identifier in cache:
            logger.debug("using previously discovered versions in the cache")
        return [
            c
            for c in cache[identifier]
            if self.validate_candidate(identifier, requirements, incompatibilities, c)
        ]

    def add_to_cache(self, identifier: str, candidates: list[Candidate]) -> None:
        # we can add candidates to cache even for install type reqs because build time reqs are
        # allowed to use candidates seen when we were resolving the same req as an install type
        logging.debug("updating cache for %s with %s", identifier, candidates)
        self.get_cache()[identifier].extend(candidates)

    def get_preference(
        self,
        identifier: str,
        resolutions: typing.Mapping[str, Candidate],
        candidates: CandidatesMap,
        information: typing.Mapping[
            str, typing.Iterable[RequirementInformation[Requirement, Candidate]]
        ],
        backtrack_causes: typing.Sequence[
            RequirementInformation[Requirement, Candidate]
        ],
    ) -> int:
        return sum(1 for _ in candidates[identifier])

    def is_satisfied_by(self, requirement: Requirement, candidate: Candidate) -> bool:
        if canonicalize_name(requirement.name) != candidate.name:
            return False
        allow_prerelease = self.constraints.allow_prerelease(requirement.name) or bool(
            requirement.specifier.prereleases
        )
        if not requirement.specifier.contains(
            candidate.version, prereleases=allow_prerelease
        ):
            if DEBUG_RESOLVER:
                logger.debug(
                    f"{requirement.name}: skipping candidate version {candidate.version} because it does not match {requirement.specifier}"
                )
            return False

        if not self.constraints.is_satisfied_by(requirement.name, candidate.version):
            if DEBUG_RESOLVER:
                c = self.constraints.get_constraint(requirement.name)
                logger.debug(
                    f"{requirement.name}: skipping {candidate.version} due to constraint {c}"
                )
            return False

        return True

    def get_dependencies(self, candidate: Candidate) -> list[Requirement]:
        # return candidate.dependencies
        return []

    def find_matches(
        self,
        identifier: str,
        requirements: RequirementsMap,
        incompatibilities: CandidatesMap,
    ) -> typing.Iterable[Candidate]:
        raise NotImplementedError()


class PyPIProvider(BaseProvider):
    """Lookup package and versions from a simple Python index (PyPI)"""

    pypi_resolver_cache: typing.ClassVar[dict[str, list[Candidate]]] = defaultdict(list)

    def __init__(
        self,
        include_sdists: bool = True,
        include_wheels: bool = True,
        sdist_server_url: str = "https://pypi.org/simple/",
        constraints: Constraints | None = None,
        req_type: RequirementType | None = None,
        ignore_platform: bool = False,
    ):
        super().__init__(
            include_sdists=include_sdists,
            include_wheels=include_wheels,
            sdist_server_url=sdist_server_url,
            constraints=constraints,
            req_type=req_type,
            ignore_platform=ignore_platform,
        )

    def get_cache(self) -> dict[str, list[Candidate]]:
        return PyPIProvider.pypi_resolver_cache

    def validate_candidate(
        self,
        identifier: str,
        requirements: RequirementsMap,
        incompatibilities: CandidatesMap,
        candidate: Candidate,
    ) -> bool:
        if not super().validate_candidate(
            identifier, requirements, incompatibilities, candidate
        ):
            return False
        # Only include sdists if we're asked to
        if candidate.is_sdist and not self.include_sdists:
            if DEBUG_RESOLVER:
                logger.debug(
                    f"{identifier}: skipping {candidate} because it is an sdist"
                )
            return False
        # Only include wheels if we're asked to
        if not candidate.is_sdist and not self.include_wheels:
            if DEBUG_RESOLVER:
                logger.debug(
                    f"{identifier}: skipping {candidate} because it is a wheel"
                )
            return False
        return True

    def find_matches(
        self,
        identifier: str,
        requirements: RequirementsMap,
        incompatibilities: CandidatesMap,
    ) -> typing.Iterable[Candidate]:
        candidates = self.get_from_cache(identifier, requirements, incompatibilities)
        if not candidates:
            # Need to pass the extras to the search, so they
            # are added to the candidate at creation - we
            # treat candidates as immutable once created.
            for candidate in get_project_from_pypi(
                identifier,
                set(),
                self.sdist_server_url,
                self.ignore_platform,
            ):
                if self.validate_candidate(
                    identifier, requirements, incompatibilities, candidate
                ):
                    candidates.append(candidate)
            self.add_to_cache(identifier, candidates)
        if not candidates:
            # Try to construct a meaningful error message that points out the
            # type(s) of files the resolver has been told it can choose as a
            # hint in case that should be adjusted for the package that does not
            # resolve.
            r = next(iter(requirements[identifier]))
            if self.include_sdists and self.include_wheels:
                raise resolvelib.resolvers.ResolverException(
                    f"found no match for {r}, any file type, in cache or at {self.sdist_server_url}"
                )
            elif self.include_sdists:
                raise resolvelib.resolvers.ResolverException(
                    f"found no match for {r}, limiting search to sdists, in cache or at {self.sdist_server_url}"
                )
            raise resolvelib.resolvers.ResolverException(
                f"found no match for {r}, limiting search to wheels, in cache or at {self.sdist_server_url}"
            )
        return sorted(candidates, key=attrgetter("version", "build_tag"), reverse=True)


class MatchFunction(typing.Protocol):
    def __call__(self, identifier: str, item: str) -> Version | None:
        pass


class GenericProvider(BaseProvider):
    """Lookup package and version by using a callback"""

    generic_resolver_cache: typing.ClassVar[dict[str, list[Candidate]]] = defaultdict(
        list
    )

    def __init__(
        self,
        version_source: VersionSource,
        constraints: Constraints | None = None,
        req_type: RequirementType | None = None,
        matcher: MatchFunction | re.Pattern | None = None,
    ):
        super().__init__(constraints=constraints, req_type=req_type)
        self._version_source = version_source
        if matcher is None:
            self._match_function = self._default_match_function
        elif isinstance(matcher, re.Pattern):
            self._match_function = functools.partial(
                self._re_match_function, regex=matcher
            )
        else:
            self._match_function = matcher

    def _default_match_function(self, identifier: str, item: str) -> Version | None:
        try:
            return Version(item)
        except Exception as err:
            logger.debug(f"{identifier}: could not parse version from {item}: {err}")
            return None

    def _re_match_function(
        self, identifier: str, item: str, *, regex: re.Pattern
    ) -> Version | None:
        mo = regex.match(item)
        if mo is None:
            logger.debug(
                f"{identifier}: tag {item} does not match pattern {regex.pattern}"
            )
            return None
        value = mo.group(1)
        try:
            return Version(value)
        except Exception as err:
            logger.debug(f"{identifier}: could not parse version from {value}: {err}")
            return None

    def get_cache(self) -> dict[str, list[Candidate]]:
        return GenericProvider.generic_resolver_cache

    def find_matches(
        self,
        identifier: str,
        requirements: RequirementsMap,
        incompatibilities: CandidatesMap,
    ) -> typing.Iterable[Candidate]:
        candidates = self.get_from_cache(identifier, requirements, incompatibilities)
        version: Version | None

        if not candidates:
            # Need to pass the extras to the search, so they
            # are added to the candidate at creation - we
            # treat candidates as immutable once created.
            for url, item in self._version_source(
                identifier, requirements, incompatibilities
            ):
                if isinstance(item, Version):
                    version = item
                else:
                    version = self._match_function(identifier, item)
                    if version is None:
                        logger.debug(f"{identifier}: match function ignores {item}")
                        continue
                    assert isinstance(version, Version)
                    version = version
                candidate = Candidate(identifier, version, url=url)
                if self.validate_candidate(
                    identifier, requirements, incompatibilities, candidate
                ):
                    candidates.append(candidate)
                self.add_to_cache(identifier, candidates)

        return sorted(candidates, key=attrgetter("version"), reverse=True)


class GitHubTagProvider(GenericProvider):
    """Lookup tarball and version from GitHub git tags

    Assumes that upstream uses version tags `1.2.3` or `v1.2.3`.
    """

    host = "github.com:443"
    api_url = "https://api.{self.host}/repos/{self.organization}/{self.repo}/tags"
    github_resolver_cache: typing.ClassVar[dict[str, list[Candidate]]] = defaultdict(
        list
    )

    def __init__(
        self,
        organization: str,
        repo: str,
        constraints: Constraints | None = None,
        matcher: MatchFunction | re.Pattern | None = None,
    ):
        super().__init__(
            version_source=self._find_tags,
            constraints=constraints,
            matcher=matcher,
        )
        self.organization = organization
        self.repo = repo

    def get_cache(self) -> dict[str, list[Candidate]]:
        return GitHubTagProvider.github_resolver_cache

    def _find_tags(
        self,
        identifier: str,
        requirements: RequirementsMap,
        incompatibilities: CandidatesMap,
    ) -> Iterable[tuple[str, Version]]:
        headers = {"accept": "application/vnd.github+json"}

        # Add GitHub authentication if available
        github_token = os.environ.get("GITHUB_TOKEN")
        if github_token:
            headers["Authorization"] = f"token {github_token}"

        nexturl = self.api_url.format(self=self)
        while nexturl:
            try:
                resp = session.get(nexturl, headers=headers)
                resp.raise_for_status()
            except Exception as e:
                logger.error(
                    "%s: Failed to fetch GitHub tags from %s: %s",
                    identifier,
                    nexturl,
                    e,
                )
                raise

            for entry in resp.json():
                name = entry["name"]
                result = self._match_function(identifier, name)
                if result is None:
                    logger.debug(f"{identifier}: match function ignores {name}")
                    continue
                assert isinstance(result, Version)
                yield entry["tarball_url"], result

            # pagination links
            nexturl = resp.links.get("next", {}).get("url")


class GitLabTagProvider(GenericProvider):
    """Lookup tarball and version from GitLab git tags"""

    gitlab_resolver_cache: typing.ClassVar[dict[str, list[Candidate]]] = defaultdict(
        list
    )

    def __init__(
        self,
        project_path: str,
        server_url: str = "https://gitlab.com",
        constraints: Constraints | None = None,
        matcher: MatchFunction | re.Pattern | None = None,
    ) -> None:
        super().__init__(
            version_source=self._find_tags,
            constraints=constraints,
            matcher=matcher,
        )
        self.server_url = server_url.rstrip("/")
        self.project_path = project_path.lstrip("/")
        # URL-encode the project path as required by GitLab API.
        # The safe="" parameter tells quote() to encode ALL characters,
        # including forward slashes (/), which would otherwise be left
        # unencoded. This ensures paths like "group/subgroup/project"
        # become "group%2Fsubgroup%2Fproject" as required by the API.
        encoded_path: str = quote(self.project_path, safe="")
        self.api_url = (
            f"{self.server_url}/api/v4/projects/{encoded_path}/repository/tags"
        )

    def get_cache(self) -> dict[str, list[Candidate]]:
        return GitLabTagProvider.gitlab_resolver_cache

    def _find_tags(
        self,
        identifier: str,
        requirements: RequirementsMap,
        incompatibilities: CandidatesMap,
    ) -> Iterable[tuple[str, Version]]:
        nexturl: str = self.api_url
        while nexturl:
            try:
                resp: Response = session.get(nexturl)
                resp.raise_for_status()
            except Exception as e:
                logger.error(
                    "%s: Failed to fetch GitLab tags from %s: %s",
                    identifier,
                    nexturl,
                    e,
                )
                raise
            for entry in resp.json():
                name = entry["name"]
                version = self._match_function(identifier, name)
                if version is None:
                    logger.debug(f"{identifier}: match function ignores {name}")
                    continue
                assert isinstance(version, Version)

                # GitLab provides a download URL for the archive, so return it
                # in case prepare_source wants to download it instead of cloning
                # the repository.
                archive_path: str = f"{self.project_path}/-/archive/{name}/{self.project_path.split('/')[-1]}-{name}.tar.gz"
                archive_url: str = urljoin(self.server_url, archive_path)
                yield archive_url, version

            # GitLab API uses Link headers for pagination
            nexturl = resp.links.get("next", {}).get("url")
