import collections
import re

import pytest
import requests_mock
import resolvelib
from packaging.requirements import Requirement
from packaging.version import Version

from fromager import constraints, resolver
from fromager.requirements_file import RequirementType

_hydra_core_simple_response = """
<!DOCTYPE html>
<html>
<head>
<meta name="pypi:repository-version" content="1.1">
<title>Links for hydra-core</title>
</head>
<body>
<h1>Links for hydra-core</h1>
<a href="https://files.pythonhosted.org/packages/6d/8e/07e42bc434a847154083b315779b0a81d567154504624e181caf2c71cd98/hydra-core-1.2.2.tar.gz#sha256=8a878ed67216997c3e9d88a8e72e7b4767e81af37afb4ea3334b269a4390a824">hydra-core-1.2.2.tar.gz</a>
<br/>
<a href="https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-1.2.2-py3-none-any.whl#sha256=fa0238a9e31df3373b35b0bfb672c34cc92718d21f81311d8996a16de1141d8b" data-dist-info-metadata="sha256=399046cbf9ae7ebab8dfd009e2b4f748212c710a0e75ca501a72bbb2d456e2e7" data-core-metadata="sha256=399046cbf9ae7ebab8dfd009e2b4f748212c710a0e75ca501a72bbb2d456e2e7">hydra_core-1.2.2-py3-none-any.whl</a>
<br/>
<a href="https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-1.3.1%2Blocal-py3-none-any.whl">hydra_core-1.3.1+local-py3-none-any.whl</a>
<br/>
<a href="https://files.pythonhosted.org/packages/6d/8e/07e42bc434a847154083b315779b0a81d567154504624e181caf2c71cd98/hydra-core-1.3.2.tar.gz#sha256=8a878ed67216997c3e9d88a8e72e7b4767e81af37afb4ea3334b269a4390a824">hydra-core-1.3.2.tar.gz</a>
<br/>
<a href="https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-1.3.2-1-py3-none-any.whl#sha256=fa0238a9e31df3373b35b0bfb672c34cc92718d21f81311d8996a16de1141d8b" data-dist-info-metadata="sha256=399046cbf9ae7ebab8dfd009e2b4f748212c710a0e75ca501a72bbb2d456e2e7" data-core-metadata="sha256=399046cbf9ae7ebab8dfd009e2b4f748212c710a0e75ca501a72bbb2d456e2e7">hydra_core-1.3.2-1-py3-none-any.whl</a>
<br/>
<a href="https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-1.3.2-2-py3-none-any.whl#sha256=fa0238a9e31df3373b35b0bfb672c34cc92718d21f81311d8996a16de1141d8b" data-dist-info-metadata="sha256=399046cbf9ae7ebab8dfd009e2b4f748212c710a0e75ca501a72bbb2d456e2e7" data-core-metadata="sha256=399046cbf9ae7ebab8dfd009e2b4f748212c710a0e75ca501a72bbb2d456e2e7">hydra_core-1.3.2-2-py3-none-any.whl</a>
<br />
<a href="https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-2.0.0a1-py3-none-any.whl" >hydra_core-2.0.0a1-py3-none-any.whl</a>
</body>
</html>
<!--SERIAL 22812307-->
"""


@pytest.fixture(autouse=True)
def reset_cache():
    resolver.PyPIProvider.pypi_resolver_cache = collections.defaultdict(list)
    resolver.GenericProvider.generic_resolver_cache = collections.defaultdict(list)
    resolver.GitHubTagProvider.github_resolver_cache = collections.defaultdict(list)


def test_provider_choose_wheel():
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        provider = resolver.PyPIProvider(include_sdists=False)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("hydra-core")])
        assert "hydra-core" in result.mapping

        candidate = result.mapping["hydra-core"]
        assert (
            candidate.url
            == "https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-1.3.2-2-py3-none-any.whl#sha256=fa0238a9e31df3373b35b0bfb672c34cc92718d21f81311d8996a16de1141d8b"
        )
        assert str(candidate.version) == "1.3.2"


def test_provider_cache():
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        # fill the cache
        provider = resolver.PyPIProvider(include_sdists=False)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core<1.3")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.2.2"
        assert "hydra-core" in resolver.PyPIProvider.pypi_resolver_cache
        assert len(resolver.PyPIProvider.pypi_resolver_cache["hydra-core"]) == 1

        # store a copy of the cache
        cache_copy = {
            "hydra-core": resolver.PyPIProvider.pypi_resolver_cache["hydra-core"][:]
        }

        # resolve for build requirement should end up with the already seen older version
        provider = resolver.PyPIProvider(
            include_sdists=False, req_type=RequirementType.BUILD_SDIST
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core>=1.2")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.2.2"
        assert "hydra-core" in resolver.PyPIProvider.pypi_resolver_cache
        assert len(resolver.PyPIProvider.pypi_resolver_cache["hydra-core"]) == 1

        # resolve for install requirement should ignore the already seen older version
        provider = resolver.PyPIProvider(
            include_sdists=False, req_type=RequirementType.INSTALL
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core>=1.2")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.3.2"

        # have to restore the cache so that 1.3.2 doesn't get picked up from there
        resolver.PyPIProvider.pypi_resolver_cache = cache_copy

        # double check that the restoration worked
        provider = resolver.PyPIProvider(
            include_sdists=False, req_type=RequirementType.BUILD_SDIST
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core>=1.2")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.2.2"

        # if resolving for build but with different conditions, don't use cache
        provider = resolver.PyPIProvider(
            include_wheels=False, req_type=RequirementType.BUILD_SDIST
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core>=1.2")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.3.2"


def test_provider_cache_capped_requirement():
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        # fill the cache
        provider = resolver.PyPIProvider(include_sdists=False)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core<1.2.4")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.2.2"
        assert "hydra-core" in resolver.PyPIProvider.pypi_resolver_cache
        assert len(resolver.PyPIProvider.pypi_resolver_cache["hydra-core"]) == 1

        # resolve for build requirement should end up with the already seen older version
        provider = resolver.PyPIProvider(
            include_sdists=False, req_type=RequirementType.BUILD_SDIST
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core>=1.2")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.2.2"
        assert "hydra-core" in resolver.PyPIProvider.pypi_resolver_cache
        assert len(resolver.PyPIProvider.pypi_resolver_cache["hydra-core"]) == 1

        # resolve for build requirement should end up with the already seen older version
        provider = resolver.PyPIProvider(
            include_sdists=False, req_type=RequirementType.BUILD_SDIST
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.2.2"
        assert "hydra-core" in resolver.PyPIProvider.pypi_resolver_cache
        assert len(resolver.PyPIProvider.pypi_resolver_cache["hydra-core"]) == 1

        # resolve for install requirement should ignore the already seen older version
        provider = resolver.PyPIProvider(
            include_sdists=False, req_type=RequirementType.INSTALL
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core>=1.2")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.3.2"
        assert len(resolver.PyPIProvider.pypi_resolver_cache["hydra-core"]) == 5

        # double check previous rule resolves the same way
        provider = resolver.PyPIProvider(
            include_sdists=False, req_type=RequirementType.BUILD_SDIST
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core>=1.2")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.2.2"
        assert len(resolver.PyPIProvider.pypi_resolver_cache["hydra-core"]) == 1

        # if resolving for build but with different conditions, don't use cache
        provider = resolver.PyPIProvider(
            include_wheels=False, req_type=RequirementType.BUILD_SDIST
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)
        result = rslvr.resolve([Requirement("hydra-core>=1.2")])
        candidate = result.mapping["hydra-core"]
        assert str(candidate.version) == "1.3.2"
        assert len(resolver.PyPIProvider.pypi_resolver_cache["hydra-core"]) == 1


def test_provider_choose_wheel_prereleases():
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        provider = resolver.PyPIProvider(include_sdists=False)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("hydra-core==2.0.0a1")])
        assert "hydra-core" in result.mapping

        candidate = result.mapping["hydra-core"]
        assert (
            candidate.url
            == "https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-2.0.0a1-py3-none-any.whl"
        )
        assert str(candidate.version) == "2.0.0a1"


def test_provider_choose_wheel_local():
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        provider = resolver.PyPIProvider(include_sdists=False)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("hydra-core==1.3.1+local")])
        assert "hydra-core" in result.mapping

        candidate = result.mapping["hydra-core"]
        assert (
            candidate.url
            == "https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-1.3.1%2Blocal-py3-none-any.whl"
        )
        assert str(candidate.version) == "1.3.1+local"


def test_provider_choose_sdist():
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        provider = resolver.PyPIProvider(include_wheels=False)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("hydra-core")])
        assert "hydra-core" in result.mapping

        candidate = result.mapping["hydra-core"]
        assert (
            candidate.url
            == "https://files.pythonhosted.org/packages/6d/8e/07e42bc434a847154083b315779b0a81d567154504624e181caf2c71cd98/hydra-core-1.3.2.tar.gz#sha256=8a878ed67216997c3e9d88a8e72e7b4767e81af37afb4ea3334b269a4390a824"
        )
        assert str(candidate.version) == "1.3.2"


def test_provider_choose_either_with_constraint():
    constraint = constraints.Constraints()
    constraint.add_constraint("hydra-core==1.3.2")
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        provider = resolver.PyPIProvider(
            include_wheels=True, include_sdists=True, constraints=constraint
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("hydra-core")])
        assert "hydra-core" in result.mapping

        candidate = result.mapping["hydra-core"]
        assert (
            candidate.url
            == "https://files.pythonhosted.org/packages/6d/8e/07e42bc434a847154083b315779b0a81d567154504624e181caf2c71cd98/hydra-core-1.3.2.tar.gz#sha256=8a878ed67216997c3e9d88a8e72e7b4767e81af37afb4ea3334b269a4390a824"
            or candidate.url
            == "https://files.pythonhosted.org/packages/c6/50/e0edd38dcd63fb26a8547f13d28f7a008bc4a3fd4eb4ff030673f22ad41a/hydra_core-1.3.2-2-py3-none-any.whl#sha256=fa0238a9e31df3373b35b0bfb672c34cc92718d21f81311d8996a16de1141d8b"
        )


def test_provider_constraint_mismatch():
    constraint = constraints.Constraints()
    constraint.add_constraint("hydra-core<=1.1")
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        provider = resolver.PyPIProvider(include_wheels=False, constraints=constraint)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        with pytest.raises(resolvelib.resolvers.ResolverException):
            rslvr.resolve([Requirement("hydra-core")])


def test_provider_constraint_match():
    constraint = constraints.Constraints()
    constraint.add_constraint("hydra-core<=1.3")
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/hydra-core/",
            text=_hydra_core_simple_response,
        )

        provider = resolver.PyPIProvider(include_wheels=False, constraints=constraint)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("hydra-core")])
        assert "hydra-core" in result.mapping

        candidate = result.mapping["hydra-core"]
        assert (
            candidate.url
            == "https://files.pythonhosted.org/packages/6d/8e/07e42bc434a847154083b315779b0a81d567154504624e181caf2c71cd98/hydra-core-1.2.2.tar.gz#sha256=8a878ed67216997c3e9d88a8e72e7b4767e81af37afb4ea3334b269a4390a824"
        )
        assert str(candidate.version) == "1.2.2"


_ignore_platform_simple_response = """
<!DOCTYPE html>
<html>
<head>
<meta name="pypi:repository-version" content="1.1">
<title>Links for fromager</title>
</head>
<body>
<h1>Links for fromager</h1>
<a href="https://files.pythonhosted.org/packages/7c/06/620610984c2794ef55c4257c77211b7a625431b380880c524c2f6bc264b1/fromager-0.51.0-cp311-abi3-manylinux_2_28_plan9.whl" >fromager-0.51.0-cp311-abi3-manylinux_2_28_plan9.whl</a>
<br />
<a href="https://files.pythonhosted.org/packages/7c/06/620610984c2794ef55c4257c77211b7a625431b380880c524c2f6bc264b1/fromager-0.51.0-cp3000-abi3-manylinux_2_28_plan9.whl" >fromager-0.51.0-cp3000-abi3-manylinux_2_28_plan9.whl</a>
</body>
</html>
"""


def test_provider_platform_mismatch():
    constraint = constraints.Constraints()
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/fromager/",
            text=_ignore_platform_simple_response,
        )

        provider = resolver.PyPIProvider(include_wheels=True, constraints=constraint)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        with pytest.raises(resolvelib.resolvers.ResolverException):
            rslvr.resolve([Requirement("fromager")])


def test_provider_ignore_platform():
    constraint = constraints.Constraints()
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/fromager/",
            text=_ignore_platform_simple_response,
        )

        provider = resolver.PyPIProvider(
            include_wheels=True, constraints=constraint, ignore_platform=True
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("fromager")])
        assert "fromager" in result.mapping

        candidate = result.mapping["fromager"]
        assert (
            candidate.url
            == "https://files.pythonhosted.org/packages/7c/06/620610984c2794ef55c4257c77211b7a625431b380880c524c2f6bc264b1/fromager-0.51.0-cp311-abi3-manylinux_2_28_plan9.whl"
        )
        assert str(candidate.version) == "0.51.0"


_github_fromager_repo_response = """
{
  "id": 808690091,
  "node_id": "R_kgDOMDOhqw",
  "name": "fromager",
  "full_name": "python-wheel-build/fromager",
  "private": false,
  "owner": {
    "login": "python-wheel-build",
    "id": 171364409,
    "node_id": "O_kgDOCjbQOQ",
    "avatar_url": "https://avatars.githubusercontent.com/u/171364409?v=4",
    "gravatar_id": "",
    "url": "https://api.github.com/users/python-wheel-build",
    "html_url": "https://github.com/python-wheel-build",
    "followers_url": "https://api.github.com/users/python-wheel-build/followers",
    "following_url": "https://api.github.com/users/python-wheel-build/following{/other_user}",
    "gists_url": "https://api.github.com/users/python-wheel-build/gists{/gist_id}",
    "starred_url": "https://api.github.com/users/python-wheel-build/starred{/owner}{/repo}",
    "subscriptions_url": "https://api.github.com/users/python-wheel-build/subscriptions",
    "organizations_url": "https://api.github.com/users/python-wheel-build/orgs",
    "repos_url": "https://api.github.com/users/python-wheel-build/repos",
    "events_url": "https://api.github.com/users/python-wheel-build/events{/privacy}",
    "received_events_url": "https://api.github.com/users/python-wheel-build/received_events",
    "type": "Organization",
    "site_admin": false
  },
  "html_url": "https://github.com/python-wheel-build/fromager",
  "description": "Build your own wheels",
  "fork": false,
  "url": "https://api.github.com/repos/python-wheel-build/fromager",
  "forks_url": "https://api.github.com/repos/python-wheel-build/fromager/forks",
  "keys_url": "https://api.github.com/repos/python-wheel-build/fromager/keys{/key_id}",
  "collaborators_url": "https://api.github.com/repos/python-wheel-build/fromager/collaborators{/collaborator}",
  "teams_url": "https://api.github.com/repos/python-wheel-build/fromager/teams",
  "hooks_url": "https://api.github.com/repos/python-wheel-build/fromager/hooks",
  "issue_events_url": "https://api.github.com/repos/python-wheel-build/fromager/issues/events{/number}",
  "events_url": "https://api.github.com/repos/python-wheel-build/fromager/events",
  "assignees_url": "https://api.github.com/repos/python-wheel-build/fromager/assignees{/user}",
  "branches_url": "https://api.github.com/repos/python-wheel-build/fromager/branches{/branch}",
  "tags_url": "https://api.github.com/repos/python-wheel-build/fromager/tags",
  "blobs_url": "https://api.github.com/repos/python-wheel-build/fromager/git/blobs{/sha}",
  "git_tags_url": "https://api.github.com/repos/python-wheel-build/fromager/git/tags{/sha}",
  "git_refs_url": "https://api.github.com/repos/python-wheel-build/fromager/git/refs{/sha}",
  "trees_url": "https://api.github.com/repos/python-wheel-build/fromager/git/trees{/sha}",
  "statuses_url": "https://api.github.com/repos/python-wheel-build/fromager/statuses/{sha}",
  "languages_url": "https://api.github.com/repos/python-wheel-build/fromager/languages",
  "stargazers_url": "https://api.github.com/repos/python-wheel-build/fromager/stargazers",
  "contributors_url": "https://api.github.com/repos/python-wheel-build/fromager/contributors",
  "subscribers_url": "https://api.github.com/repos/python-wheel-build/fromager/subscribers",
  "subscription_url": "https://api.github.com/repos/python-wheel-build/fromager/subscription",
  "commits_url": "https://api.github.com/repos/python-wheel-build/fromager/commits{/sha}",
  "git_commits_url": "https://api.github.com/repos/python-wheel-build/fromager/git/commits{/sha}",
  "comments_url": "https://api.github.com/repos/python-wheel-build/fromager/comments{/number}",
  "issue_comment_url": "https://api.github.com/repos/python-wheel-build/fromager/issues/comments{/number}",
  "contents_url": "https://api.github.com/repos/python-wheel-build/fromager/contents/{+path}",
  "compare_url": "https://api.github.com/repos/python-wheel-build/fromager/compare/{base}...{head}",
  "merges_url": "https://api.github.com/repos/python-wheel-build/fromager/merges",
  "archive_url": "https://api.github.com/repos/python-wheel-build/fromager/{archive_format}{/ref}",
  "downloads_url": "https://api.github.com/repos/python-wheel-build/fromager/downloads",
  "issues_url": "https://api.github.com/repos/python-wheel-build/fromager/issues{/number}",
  "pulls_url": "https://api.github.com/repos/python-wheel-build/fromager/pulls{/number}",
  "milestones_url": "https://api.github.com/repos/python-wheel-build/fromager/milestones{/number}",
  "notifications_url": "https://api.github.com/repos/python-wheel-build/fromager/notifications{?since,all,participating}",
  "labels_url": "https://api.github.com/repos/python-wheel-build/fromager/labels{/name}",
  "releases_url": "https://api.github.com/repos/python-wheel-build/fromager/releases{/id}",
  "deployments_url": "https://api.github.com/repos/python-wheel-build/fromager/deployments",
  "created_at": "2024-05-31T15:49:02Z",
  "updated_at": "2024-06-26T14:52:13Z",
  "pushed_at": "2024-06-26T14:52:09Z",
  "git_url": "git://github.com/python-wheel-build/fromager.git",
  "ssh_url": "git@github.com:python-wheel-build/fromager.git",
  "clone_url": "https://github.com/python-wheel-build/fromager.git",
  "svn_url": "https://github.com/python-wheel-build/fromager",
  "homepage": "https://pypi.org/project/fromager/",
  "size": 907,
  "stargazers_count": 1,
  "watchers_count": 1,
  "language": "Python",
  "has_issues": true,
  "has_projects": true,
  "has_downloads": true,
  "has_wiki": false,
  "has_pages": false,
  "has_discussions": false,
  "forks_count": 4,
  "mirror_url": null,
  "archived": false,
  "disabled": false,
  "open_issues_count": 14,
  "license": {
    "key": "apache-2.0",
    "name": "Apache License 2.0",
    "spdx_id": "Apache-2.0",
    "url": "https://api.github.com/licenses/apache-2.0",
    "node_id": "MDc6TGljZW5zZTI="
  },
  "allow_forking": true,
  "is_template": false,
  "web_commit_signoff_required": false,
  "topics": [

  ],
  "visibility": "public",
  "forks": 4,
  "open_issues": 14,
  "watchers": 1,
  "default_branch": "main",
  "temp_clone_token": null,
  "custom_properties": {

  },
  "organization": {
    "login": "python-wheel-build",
    "id": 171364409,
    "node_id": "O_kgDOCjbQOQ",
    "avatar_url": "https://avatars.githubusercontent.com/u/171364409?v=4",
    "gravatar_id": "",
    "url": "https://api.github.com/users/python-wheel-build",
    "html_url": "https://github.com/python-wheel-build",
    "followers_url": "https://api.github.com/users/python-wheel-build/followers",
    "following_url": "https://api.github.com/users/python-wheel-build/following{/other_user}",
    "gists_url": "https://api.github.com/users/python-wheel-build/gists{/gist_id}",
    "starred_url": "https://api.github.com/users/python-wheel-build/starred{/owner}{/repo}",
    "subscriptions_url": "https://api.github.com/users/python-wheel-build/subscriptions",
    "organizations_url": "https://api.github.com/users/python-wheel-build/orgs",
    "repos_url": "https://api.github.com/users/python-wheel-build/repos",
    "events_url": "https://api.github.com/users/python-wheel-build/events{/privacy}",
    "received_events_url": "https://api.github.com/users/python-wheel-build/received_events",
    "type": "Organization",
    "site_admin": false
  },
  "network_count": 4,
  "subscribers_count": 1
}
"""

# This mock response text does not include the most recent release to
# ensure we're actually using the mock data.
_github_fromager_tag_response = """
[
  {
    "name": "0.9.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.9.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.9.0",
    "commit": {
      "sha": "5fbdab491e983152f7e5c8200b4f7f62f714aedf",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/5fbdab491e983152f7e5c8200b4f7f62f714aedf"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC45LjA"
  },
  {
    "name": "0.8.1",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.8.1",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.8.1",
    "commit": {
      "sha": "a790c71adeb21a02e09173407339bc25085bdf4d",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/a790c71adeb21a02e09173407339bc25085bdf4d"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC44LjE"
  },
  {
    "name": "0.8.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.8.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.8.0",
    "commit": {
      "sha": "5e3b5595d2f8751eb3ba81c287045eac85441fc3",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/5e3b5595d2f8751eb3ba81c287045eac85441fc3"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC44LjA"
  },
  {
    "name": "0.7.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.7.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.7.0",
    "commit": {
      "sha": "d97a0520a7b21bc3d0617c77da0227a4e7656be0",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/d97a0520a7b21bc3d0617c77da0227a4e7656be0"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC43LjA"
  },
  {
    "name": "0.6.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.6.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.6.0",
    "commit": {
      "sha": "7935210bb23a9fd662273dc38e98dc13f8fa3243",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/7935210bb23a9fd662273dc38e98dc13f8fa3243"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC42LjA"
  },
  {
    "name": "0.5.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.5.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.5.0",
    "commit": {
      "sha": "de945241b85ae386203383e250d3b33032f9ff4b",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/de945241b85ae386203383e250d3b33032f9ff4b"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC41LjA"
  },
  {
    "name": "0.4.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.4.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.4.0",
    "commit": {
      "sha": "b1f79701cf95c1dfb098d83438afdd66661118e3",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/b1f79701cf95c1dfb098d83438afdd66661118e3"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC40LjA"
  },
  {
    "name": "0.3.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.3.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.3.0",
    "commit": {
      "sha": "f677a155e804a5b1a4bfbf6c28773a88a6d934c6",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/f677a155e804a5b1a4bfbf6c28773a88a6d934c6"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC4zLjA"
  },
  {
    "name": "0.2.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.2.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.2.0",
    "commit": {
      "sha": "7b80c36074af597f78850eb9247d7cd1d9b27fce",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/7b80c36074af597f78850eb9247d7cd1d9b27fce"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC4yLjA"
  },
  {
    "name": "0.1.0",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.1.0",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.1.0",
    "commit": {
      "sha": "e75facfd9dd9437fa5c19badeddd6e40e9197659",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/e75facfd9dd9437fa5c19badeddd6e40e9197659"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC4xLjA"
  },
  {
    "name": "0.0.1",
    "zipball_url": "https://api.github.com/repos/python-wheel-build/fromager/zipball/refs/tags/0.0.1",
    "tarball_url": "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.0.1",
    "commit": {
      "sha": "39b0422377d0b405a8fe2ba32f384d677f1c06af",
      "url": "https://api.github.com/repos/python-wheel-build/fromager/commits/39b0422377d0b405a8fe2ba32f384d677f1c06af"
    },
    "node_id": "REF_kwDOMDOhq69yZWZzL3RhZ3MvMC4wLjE"
  }
]
"""


def test_resolve_github():
    with requests_mock.Mocker() as r:
        r.get(
            "https://api.github.com:443/repos/python-wheel-build/fromager",
            text=_github_fromager_repo_response,
        )
        r.get(
            "https://api.github.com:443/repos/python-wheel-build/fromager/tags",
            text=_github_fromager_tag_response,
        )

        provider = resolver.GitHubTagProvider("python-wheel-build", "fromager")
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("fromager")])
        assert "fromager" in result.mapping

        candidate = result.mapping["fromager"]
        assert str(candidate.version) == "0.9.0"
        # check the "URL" in case tag syntax does not match version syntax
        assert (
            str(candidate.url)
            == "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.9.0"
        )


def test_github_constraint_mismatch():
    constraint = constraints.Constraints()
    constraint.add_constraint("fromager>=1.0")
    with requests_mock.Mocker() as r:
        r.get(
            "https://api.github.com:443/repos/python-wheel-build/fromager",
            text=_github_fromager_repo_response,
        )
        r.get(
            "https://api.github.com:443/repos/python-wheel-build/fromager/tags",
            text=_github_fromager_tag_response,
        )

        provider = resolver.GitHubTagProvider(
            "python-wheel-build", "fromager", constraints=constraint
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        with pytest.raises(resolvelib.resolvers.ResolutionImpossible):
            rslvr.resolve([Requirement("fromager")])


def test_github_constraint_match():
    constraint = constraints.Constraints()
    constraint.add_constraint("fromager<0.9")
    with requests_mock.Mocker() as r:
        r.get(
            "https://api.github.com:443/repos/python-wheel-build/fromager",
            text=_github_fromager_repo_response,
        )
        r.get(
            "https://api.github.com:443/repos/python-wheel-build/fromager/tags",
            text=_github_fromager_tag_response,
        )

        provider = resolver.GitHubTagProvider(
            "python-wheel-build", "fromager", constraints=constraint
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("fromager")])
        assert "fromager" in result.mapping

        candidate = result.mapping["fromager"]
        assert str(candidate.version) == "0.8.1"
        # check the "URL" in case tag syntax does not match version syntax
        assert (
            str(candidate.url)
            == "https://api.github.com/repos/python-wheel-build/fromager/tarball/refs/tags/0.8.1"
        )


def test_resolve_generic():
    def _versions(*args, **kwds):
        return [("url", "1.2"), ("url", "1.3"), ("url", "1.4.1")]

    provider = resolver.GenericProvider(_versions, None)
    reporter = resolvelib.BaseReporter()
    rslvr = resolvelib.Resolver(provider, reporter)

    result = rslvr.resolve([Requirement("fromager")])
    assert "fromager" in result.mapping

    candidate = result.mapping["fromager"]
    assert str(candidate.version) == "1.4.1"


_gitlab_submodlib_repo_response = """
[
  {
    "name": "v0.0.3",
    "message": "Version 0.0.3 release targeting specific commit",
    "target": "5d7737e306d8a266f2da162fb535f52f1570cea4",
    "commit": {
      "id": "72ae33a1ead9761e7240c2e095873047339ada7c",
      "short_id": "72ae33a1",
      "created_at": "2025-04-24T07:38:34.000-05:00",
      "parent_ids": [
        "ae066d21ec3bb92b7994e4aaede01d6e3decd177",
        "c77e898cfa5e34b8af8b545205315e221951b23a"
      ],
      "title": "Merge pull request #54 from anastasds/sampling-termination",
      "message": "Merge pull request #54 from anastasds/sampling-termination Ensure that random sampling always terminates in randomness-based optimizers",
      "author_name": "Krishnateja Killamsetty",
      "author_email": "61333497+krishnatejakk@users.noreply.github.com",
      "authored_date": "2025-04-24T07:38:34.000-05:00",
      "committer_name": "GitHub",
      "committer_email": "noreply@github.com",
      "committed_date": "2025-04-24T07:38:34.000-05:00",
      "trailers": {},
      "extended_trailers": {},
      "web_url": "https://gitlab.com/mirrors/github/decile-team/submodlib/-/commit/72ae33a1ead9761e7240c2e095873047339ada7c"
    },
    "release": null,
    "protected": false,
    "created_at": "2025-05-14T15:43:00.000Z"
  },
  {
    "name": "v0.0.2",
    "message": "",
    "target": "ae066d21ec3bb92b7994e4aaede01d6e3decd177",
    "commit": {
      "id": "ae066d21ec3bb92b7994e4aaede01d6e3decd177",
      "short_id": "ae066d21",
      "created_at": "2025-04-14T14:41:32.000-05:00",
      "parent_ids": [
        "b414069392bca65b1829caeeaea3138cfd69aa53"
      ],
      "title": "Update python-publish.yml",
      "message": "Update python-publish.yml",
      "author_name": "Krishnateja Killamsetty",
      "author_email": "61333497+krishnatejakk@users.noreply.github.com",
      "authored_date": "2025-04-14T14:41:32.000-05:00",
      "committer_name": "GitHub",
      "committer_email": "noreply@github.com",
      "committed_date": "2025-04-14T14:41:32.000-05:00",
      "trailers": {},
      "extended_trailers": {},
      "web_url": "https://gitlab.com/mirrors/github/decile-team/submodlib/-/commit/ae066d21ec3bb92b7994e4aaede01d6e3decd177"
    },
    "release": null,
    "protected": false,
    "created_at": null
  },
  {
    "name": "v0.0.1",
    "message": "Version 0.0.1 release targeting specific commit",
    "target": "e60a53f357d8851c6c61de7b0952ac04bd6415b3",
    "commit": {
      "id": "7fdc3f67c81af78554aee173ad90ee7dc9948902",
      "short_id": "7fdc3f67",
      "created_at": "2025-04-07T18:58:04.000+00:00",
      "parent_ids": [
        "d93fcdc6bb4467015a7c9375716f89ded9e466e7",
        "0db18f7f86dcf21d141da89a6e4adffc7a85d5d1"
      ],
      "title": "Merge branch 'master' of https://github.com/decile-team/submodlib",
      "message": "Merge branch 'master' of https://github.com/decile-team/submodlib",
      "author_name": "Krishnateja",
      "author_email": "krishnateja.k@ibm.com",
      "authored_date": "2025-04-07T18:58:04.000+00:00",
      "committer_name": "Krishnateja",
      "committer_email": "krishnateja.k@ibm.com",
      "committed_date": "2025-04-07T18:58:04.000+00:00",
      "trailers": {},
      "extended_trailers": {},
      "web_url": "https://gitlab.com/mirrors/github/decile-team/submodlib/-/commit/7fdc3f67c81af78554aee173ad90ee7dc9948902"
    },
    "release": null,
    "protected": false,
    "created_at": "2025-04-14T19:04:20.000Z"
  }
]
"""


def tag_match(identifier: str, item: str) -> Version | None:
    """Extract version from v-prefixed tags"""
    mo = re.match("^v(.*)$", item)
    if mo:
        try:
            return Version(mo.group(1))
        except Exception:
            pass
    return None


def test_resolve_gitlab():
    with requests_mock.Mocker() as r:
        r.get(
            "https://gitlab.com/api/v4/projects/mirrors%2Fgithub%2Fdecile-team%2Fsubmodlib/repository/tags",
            text=_gitlab_submodlib_repo_response,
        )

        provider = resolver.GitLabTagProvider(
            project_path="mirrors/github/decile-team/submodlib",
            server_url="https://gitlab.com",
            matcher=re.compile("v(.*)"),  # with match object
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("submodlib")])
        assert "submodlib" in result.mapping

        candidate = result.mapping["submodlib"]
        assert str(candidate.version) == "0.0.3"
        # check the "URL" in case tag syntax does not match version syntax
        assert (
            str(candidate.url)
            == "https://gitlab.com/mirrors/github/decile-team/submodlib/-/archive/v0.0.3/submodlib-v0.0.3.tar.gz"
        )


def test_gitlab_constraint_mismatch():
    constraint = constraints.Constraints()
    constraint.add_constraint("submodlib>=1.0")
    with requests_mock.Mocker() as r:
        r.get(
            "https://gitlab.com/api/v4/projects/mirrors%2Fgithub%2Fdecile-team%2Fsubmodlib/repository/tags",
            text=_gitlab_submodlib_repo_response,
        )

        provider = resolver.GitLabTagProvider(
            project_path="mirrors/github/decile-team/submodlib",
            server_url="https://gitlab.com",
            matcher=tag_match,  # match function
            constraints=constraint,
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        with pytest.raises(resolvelib.resolvers.ResolutionImpossible):
            rslvr.resolve([Requirement("submodlib")])


def test_gitlab_constraint_match():
    constraint = constraints.Constraints()
    constraint.add_constraint("submodlib<0.0.3")
    with requests_mock.Mocker() as r:
        r.get(
            "https://gitlab.com/api/v4/projects/mirrors%2Fgithub%2Fdecile-team%2Fsubmodlib/repository/tags",
            text=_gitlab_submodlib_repo_response,
        )

        provider = resolver.GitLabTagProvider(
            project_path="mirrors/github/decile-team/submodlib",
            server_url="https://gitlab.com",
            matcher=None,  # default, Version() also ignores leading 'v'
            constraints=constraint,
        )
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("submodlib")])
        assert "submodlib" in result.mapping

        candidate = result.mapping["submodlib"]
        assert str(candidate.version) == "0.0.2"
        # check the "URL" in case tag syntax does not match version syntax
        assert (
            str(candidate.url)
            == "https://gitlab.com/mirrors/github/decile-team/submodlib/-/archive/v0.0.2/submodlib-v0.0.2.tar.gz"
        )


_response_with_data_yanked = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta name="pypi:repository-version" content="1.1">
    <title>Links for setuptools-scm</title>
</head>
<body>
    <h1>Links for setuptools-scm</h1>
    <a href="https://files.pythonhosted.org/packages/ab/ac/8f96ba9b4cfe3e4ea201f23f4f97165862395e9331a424ed325ae37024a8/setuptools_scm-8.3.1-py3-none-any.whl#sha256=332ca0d43791b818b841213e76b1971b7711a960761c5bea5fc5cdb5196fbce3">setuptools_scm-8.3.1-py3-none-any.whl</a>
    <br/>
    <a href="https://files.pythonhosted.org/packages/b9/19/7ae64b70b2429c48c3a7a4ed36f50f94687d3bfcd0ae2f152367b6410dff/setuptools_scm-8.3.1.tar.gz#sha256=3d555e92b75dacd037d32bafdf94f97af51ea29ae8c7b234cf94b7a5bd242a63">setuptools_scm-8.3.1.tar.gz</a>
    <br/>
    <a href="https://files.pythonhosted.org/packages/0c/c2/d5c5722178eb4f8d449d96dd7d6ea894fd0cb3313a24efb8bfef65fcc411/setuptools_scm-9.0.0-py3-none-any.whl#sha256=6768003ef91d3343b3f7194f911c22bcbd82ee7844e8907c5e4b35158f128b2e" data-yanked="2 unplanned regressions in config parsing and downstream api usage">setuptools_scm-9.0.0-py3-none-any.whl</a>
    <br/>
    <a href="https://files.pythonhosted.org/packages/c8/b7/7d94697d2192be602497ac82c9465496d215de7228ab4fe5c3bf641e54fb/setuptools_scm-9.0.0.tar.gz#sha256=a237477abd152e707b739fc082c9b45e1cebf3a14249de8ed4d15ee01e72c5e6" data-yanked="2 unplanned regressions in config parsing and downstream api usage">setuptools_scm-9.0.0.tar.gz</a>
</body>
</html>
"""


def test_pep592_support_latest_version_yanked():
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/setuptools-scm/", text=_response_with_data_yanked
        )

        provider = resolver.PyPIProvider()
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        result = rslvr.resolve([Requirement("setuptools-scm")])

        assert "setuptools-scm" in result.mapping

        candidate = result.mapping["setuptools-scm"]
        assert str(candidate.version) == "8.3.1"


def test_pep592_support_constraint_mismatch():
    constraint = constraints.Constraints()
    constraint.add_constraint("setuptools-scm>=9.0.0")
    with requests_mock.Mocker() as r:
        r.get(
            "https://pypi.org/simple/setuptools-scm/", text=_response_with_data_yanked
        )

        provider = resolver.PyPIProvider(constraints=constraint)
        reporter = resolvelib.BaseReporter()
        rslvr = resolvelib.Resolver(provider, reporter)

        with pytest.raises(resolvelib.resolvers.ResolverException):
            rslvr.resolve([Requirement("setuptools-scm")])


@pytest.mark.parametrize(
    "url,filename",
    [
        ("http://example.com/path/to/file.txt", "file.txt"),
        (
            "http://localhost:8080/simple/vllm/vllm-0.10.0%2Brhai1-15-cp312-cp312-linux_x86_64.whl",
            "vllm-0.10.0+rhai1-15-cp312-cp312-linux_x86_64.whl",
        ),
    ],
)
def test_extract_filename_from_url(url, filename):
    result = resolver.extract_filename_from_url(url)
    assert result == filename
