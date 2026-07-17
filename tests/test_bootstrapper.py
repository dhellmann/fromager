import json
import logging
import pathlib
import typing
from unittest.mock import Mock, patch

import pytest
import requests.exceptions
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version
from resolvelib.resolvers import ResolverException

from fromager import bootstrapper, log
from fromager.bootstrapper._build import Build
from fromager.bootstrapper._cache import (
    _download_wheel_from_cache,
    bg_prepare_prebuilt,
    find_cached_wheel,
)
from fromager.bootstrapper._phase import Phase
from fromager.bootstrapper._prepare_source import (
    PrepareSource,
    _bg_prepare_source,
)
from fromager.bootstrapper._process_install_deps import (
    ProcessInstallDeps,
    _get_install_dependencies,
)
from fromager.bootstrapper._resolve import Resolve
from fromager.bootstrapper._start import Start
from fromager.bootstrapper._types import (
    BootstrapPhase,
    SourceBuildResult,
)
from fromager.bootstrapper._work_item import WorkItem
from fromager.context import WorkContext
from fromager.requirements_file import RequirementType, SourceType


def test_phase_subclass_without_phase_attribute_raises() -> None:
    """Concrete Phase subclass missing 'phase' raises TypeError at class definition."""
    with pytest.raises(TypeError, match="must define the 'phase' class attribute"):

        class _BadPhase(Phase):
            def run(self, bt: typing.Any) -> list[Phase]:  # type: ignore[override]
                return []


def test_phase_abstract_subclass_without_phase_attribute_is_allowed() -> None:
    """Abstract Phase subclass may omit 'phase' without error."""
    import abc

    class _AbstractMiddle(Phase):
        @abc.abstractmethod
        def helper(self) -> None: ...

    # Defining _AbstractMiddle itself must not raise.
    assert _AbstractMiddle.__abstractmethods__


def test_seen(tmp_context: WorkContext) -> None:
    bt = bootstrapper.Bootstrapper(tmp_context)
    req = Requirement("testdist")
    version = Version("1.2")
    assert not bt.has_been_seen(req, version)
    bt.mark_as_seen(req, version)
    assert bt.has_been_seen(req, version)


def test_seen_extras(tmp_context: WorkContext) -> None:
    req1 = Requirement("testdist")
    req2 = Requirement("testdist[extra]")
    version = Version("1.2")
    bt = bootstrapper.Bootstrapper(tmp_context)
    assert not bt.has_been_seen(req1, version)
    bt.mark_as_seen(req1, version)
    assert bt.has_been_seen(req1, version)
    assert not bt.has_been_seen(req2, version)
    bt.mark_as_seen(req2, version)
    assert bt.has_been_seen(req1, version)
    assert bt.has_been_seen(req2, version)


def test_seen_name_canonicalization(tmp_context: WorkContext) -> None:
    req = Requirement("flit_core")
    version = Version("1.2")
    bt = bootstrapper.Bootstrapper(tmp_context)
    assert not bt.has_been_seen(req, version)
    bt.mark_as_seen(req, version)
    assert bt.has_been_seen(req, version)


def test_seen_requirements_sdist(tmp_context: WorkContext) -> None:
    bt = bootstrapper.Bootstrapper(tmp_context)
    req = Requirement("testdist")
    version = Version("1.2")
    assert not bt.has_been_seen(req, version, sdist_only=False)
    assert not bt.has_been_seen(req, version, sdist_only=True)
    # sdist only does not affect wheel status
    bt.mark_as_seen(req, version, sdist_only=True)
    assert bt.has_been_seen(req, version, sdist_only=True)
    assert not bt.has_been_seen(req, version, sdist_only=False)

    bt.mark_as_seen(req, version, sdist_only=False)
    assert bt.has_been_seen(req, version, sdist_only=True)
    assert bt.has_been_seen(req, version, sdist_only=False)

    req2 = Requirement("testwheel")
    assert not bt.has_been_seen(req2, version, sdist_only=False)
    assert not bt.has_been_seen(req2, version, sdist_only=True)
    # full seen affects both sdist and wheel status
    bt.mark_as_seen(req2, version, sdist_only=False)
    assert bt.has_been_seen(req2, version, sdist_only=True)
    assert bt.has_been_seen(req2, version, sdist_only=False)


def test_build_order(tmp_context: WorkContext) -> None:
    bt = bootstrapper.Bootstrapper(tmp_context)
    bt.add_to_build_order(
        req=Requirement("buildme>1.0"),
        version=Version("6.0"),
        source_url="url",
        source_type=SourceType.SDIST,
    )
    bt.add_to_build_order(
        req=Requirement("testdist>1.0"),
        version=Version("1.2"),
        source_url="url",
        source_type=SourceType.SDIST,
    )
    bt.finalize()
    contents_str = bt._build_order_filename.read_text()
    contents = json.loads(contents_str)
    expected = [
        {
            "req": "buildme>1.0",
            "dist": "buildme",
            "version": "6.0",
            "prebuilt": False,
            "source_url": "url",
            "source_url_type": "sdist",
            "constraint": "",
        },
        {
            "req": "testdist>1.0",
            "dist": "testdist",
            "version": "1.2",
            "prebuilt": False,
            "source_url": "url",
            "source_url_type": "sdist",
            "constraint": "",
        },
    ]
    assert expected == contents


def test_build_order_repeats(tmp_context: WorkContext) -> None:
    bt = bootstrapper.Bootstrapper(tmp_context)
    bt.add_to_build_order(
        Requirement("buildme>1.0"),
        Version("6.0"),
        "url",
        SourceType.SDIST,
    )
    bt.add_to_build_order(
        Requirement("buildme>1.0"),
        Version("6.0"),
        "url",
        SourceType.SDIST,
    )
    bt.add_to_build_order(
        Requirement("buildme[extra]>1.0"),
        Version("6.0"),
        "url",
        SourceType.SDIST,
    )
    bt.finalize()
    contents_str = bt._build_order_filename.read_text()
    contents = json.loads(contents_str)
    expected = [
        {
            "req": "buildme>1.0",
            "dist": "buildme",
            "version": "6.0",
            "prebuilt": False,
            "source_url": "url",
            "source_url_type": "sdist",
            "constraint": "",
        },
    ]
    assert expected == contents


def test_build_order_name_canonicalization(tmp_context: WorkContext) -> None:
    bt = bootstrapper.Bootstrapper(tmp_context)
    bt.add_to_build_order(
        Requirement("flit-core>1.0"),
        Version("3.9.0"),
        "url",
        SourceType.SDIST,
    )
    bt.add_to_build_order(
        Requirement("flit_core>1.0"),
        Version("3.9.0"),
        "url",
        SourceType.SDIST,
    )
    bt.finalize()
    contents_str = bt._build_order_filename.read_text()
    contents = json.loads(contents_str)
    expected = [
        {
            "req": "flit-core>1.0",
            "dist": "flit-core",
            "version": "3.9.0",
            "prebuilt": False,
            "source_url": "url",
            "source_url_type": "sdist",
            "constraint": "",
        },
    ]
    assert expected == contents


def test_explain(tmp_context: WorkContext) -> None:
    bt = bootstrapper.Bootstrapper(tmp_context)
    bt.why = [(RequirementType.TOP_LEVEL, Requirement("foo"), Version("1.0.0"))]
    assert bt.explain == f"{RequirementType.TOP_LEVEL} dependency foo (1.0.0)"

    bt.why = []
    assert bt.explain == ""

    bt.why = [
        (RequirementType.TOP_LEVEL, Requirement("foo"), Version("1.0.0")),
        (RequirementType.BUILD_SYSTEM, Requirement("bar==4.0.0"), Version("4.0.0")),
    ]
    assert (
        bt.explain
        == f"{RequirementType.BUILD_SYSTEM} dependency bar==4.0.0 (4.0.0) for {RequirementType.TOP_LEVEL} dependency foo (1.0.0)"
    )


def _make_work_item(
    req_type: RequirementType,
    why_snapshot: list[tuple[RequirementType, Requirement, Version]] | None = None,
) -> WorkItem:
    return WorkItem(
        req=Requirement("testpkg"),
        req_type=req_type,
        why_snapshot=why_snapshot or [],
    )


def test_is_build_requirement(tmp_context: WorkContext) -> None:
    # No ancestry: req_type alone determines result
    assert not _make_work_item(RequirementType.TOP_LEVEL).is_build_requirement_context()
    assert _make_work_item(RequirementType.BUILD_SYSTEM).is_build_requirement_context()
    assert _make_work_item(RequirementType.BUILD_BACKEND).is_build_requirement_context()
    assert _make_work_item(RequirementType.BUILD_SDIST).is_build_requirement_context()
    assert not _make_work_item(RequirementType.INSTALL).is_build_requirement_context()

    # TOP_LEVEL ancestry: install is still not a build requirement
    top_level_why = [(RequirementType.TOP_LEVEL, Requirement("foo"), Version("1.0.0"))]
    assert not _make_work_item(
        RequirementType.INSTALL, top_level_why
    ).is_build_requirement_context()
    assert _make_work_item(
        RequirementType.BUILD_SYSTEM, top_level_why
    ).is_build_requirement_context()
    assert _make_work_item(
        RequirementType.BUILD_BACKEND, top_level_why
    ).is_build_requirement_context()
    assert _make_work_item(
        RequirementType.BUILD_SDIST, top_level_why
    ).is_build_requirement_context()

    # BUILD_SYSTEM in ancestry: install becomes a build requirement
    build_why = [
        (RequirementType.TOP_LEVEL, Requirement("foo"), Version("1.0.0")),
        (RequirementType.BUILD_SYSTEM, Requirement("bar==4.0.0"), Version("4.0.0")),
    ]
    assert _make_work_item(
        RequirementType.INSTALL, build_why
    ).is_build_requirement_context()
    assert _make_work_item(
        RequirementType.BUILD_SYSTEM, build_why
    ).is_build_requirement_context()
    assert _make_work_item(
        RequirementType.BUILD_BACKEND, build_why
    ).is_build_requirement_context()
    assert _make_work_item(
        RequirementType.BUILD_SDIST, build_why
    ).is_build_requirement_context()


def test_find_cached_wheel_returns_tuple(tmp_context: WorkContext) -> None:
    """Verify `find_cached_wheel` returns tuple of (Path|None, Path|None)."""
    bt = bootstrapper.Bootstrapper(tmp_context)

    # Call method (will return None, None since no wheels exist)
    result = find_cached_wheel(
        bt.ctx,
        bt.cache_wheel_server_url,
        req=Requirement("test-package"),
        resolved_version=Version("1.0.0"),
    )

    # Verify return type is tuple with 2 elements
    assert isinstance(result, tuple)
    assert len(result) == 2


@patch("fromager.dependencies.get_install_dependencies_of_wheel", return_value=set())
def test_get_install_dependencies_returns_list(
    mock_get_deps: Mock, tmp_context: WorkContext
) -> None:
    """Verify _get_install_dependencies returns list."""
    # Create fake wheel file and mock dependencies
    wheel_file = pathlib.Path("/fake/package-1.0.0-py3-none-any.whl")
    unpack_dir = tmp_context.work_dir

    result = _get_install_dependencies(
        ctx=tmp_context,
        req=Requirement("test-package"),
        resolved_version=Version("1.0.0"),
        wheel_filename=wheel_file,
        sdist_filename=None,
        sdist_root_dir=None,
        build_env=None,
        unpack_dir=unpack_dir,
    )

    # Verify return type is list
    assert isinstance(result, list)
    # Verify the mocked function was called
    mock_get_deps.assert_called_once()


def test_phase_build_produces_source_build_result(tmp_context: WorkContext) -> None:
    """Verify Build.run() produces a SourceBuildResult with correct values."""
    bt = bootstrapper.Bootstrapper(tmp_context)

    mock_sdist_root = tmp_context.work_dir / "package-1.0.0" / "package-1.0.0"
    mock_sdist_root.parent.mkdir(parents=True, exist_ok=True)
    mock_wheel = tmp_context.work_dir / "package-1.0.0-py3-none-any.whl"

    wi = WorkItem(
        req=Requirement("test-package"),
        req_type=RequirementType.TOP_LEVEL,
        source_url="https://pypi.org/simple/test-package",
        resolved_version=Version("1.0.0"),
        why_snapshot=[],
        sdist_root_dir=mock_sdist_root,
        unpack_dir=mock_sdist_root.parent,
        build_env=Mock(),
        build_system_deps=set(),
        build_backend_deps=set(),
        build_sdist_deps=set(),
    )
    item = Build(wi)

    # Set up why stack so _track_why works
    bt.why = []

    with (
        patch("fromager.sources.get_source_type", return_value=SourceType.SDIST),
        patch.object(item, "_build_wheel", return_value=(mock_wheel, None)),
    ):
        with bt._track_why(item):
            result_items = item.run(bt)

    assert len(result_items) == 1
    assert isinstance(result_items[0], ProcessInstallDeps)

    result = result_items[0].work_item.build_result
    assert isinstance(result, SourceBuildResult)
    assert result.wheel_filename == mock_wheel
    assert result.sdist_filename is None
    assert result.unpack_dir == mock_sdist_root.parent
    assert result.sdist_root_dir == mock_sdist_root
    assert result.source_type == SourceType.SDIST


def test_multiple_versions_continues_on_error(tmp_context: WorkContext) -> None:
    """Test that multiple versions mode continues when one version fails."""
    bt = bootstrapper.Bootstrapper(tmp_context, multiple_versions=True)

    # Mock the resolver to return 3 versions
    with patch.object(
        bt._resolver,
        "resolve",
        return_value=[
            ("https://pypi.org/testpkg-2.0.tar.gz", Version("2.0")),
            ("https://pypi.org/testpkg-1.5.tar.gz", Version("1.5")),
            ("https://pypi.org/testpkg-1.0.tar.gz", Version("1.0")),
        ],
    ):
        # Let RESOLVE and START run normally, but intercept PrepareSource onwards.
        # Fail for version 1.5; count all interceptions.
        build_phase_count = {"count": 0}

        def prepare_source_run(
            self: PrepareSource,
            bt_arg: bootstrapper.Bootstrapper,
        ) -> list[Phase]:
            build_phase_count["count"] += 1
            if str(self.work_item.resolved_version) == "1.5":
                raise ValueError("Simulated failure for version 1.5")
            return []

        req = Requirement("testpkg>=1.0")

        with patch.object(PrepareSource, "run", prepare_source_run):
            with patch.object(bt, "has_been_seen", return_value=False):
                bt._bootstrap_one(
                    req=req,
                    req_type=RequirementType.INSTALL,
                )

                # All 3 versions should reach build phases
                assert build_phase_count["count"] == 3

                # Verify that version 1.5 is in failed_versions
                assert len(bt._failed_versions) == 1
                pkg_name = canonicalize_name("testpkg")
                version_str = "1.5"
                assert (pkg_name, version_str) in bt._failed_versions
                exc = bt._failed_versions[(pkg_name, version_str)]
                assert isinstance(exc, ValueError)
                assert str(exc) == "Simulated failure for version 1.5"

        # Verify that failed version 1.5 is NOT in the dependency graph
        failed_key = f"{canonicalize_name('testpkg')}==1.5"
        assert failed_key not in tmp_context.dependency_graph.nodes

        # Verify that successful versions ARE in the dependency graph
        success_key_20 = f"{canonicalize_name('testpkg')}==2.0"
        success_key_10 = f"{canonicalize_name('testpkg')}==1.0"
        assert success_key_20 in tmp_context.dependency_graph.nodes
        assert success_key_10 in tmp_context.dependency_graph.nodes


@patch("fromager.resolver.find_all_matching_from_provider")
@patch("fromager.finders.PyPICacheProvider")
def test_download_wheel_from_cache_bypasses_hooks(
    mock_cache_provider: Mock,
    mock_find_all: Mock,
    tmp_context: WorkContext,
) -> None:
    """Verify _download_wheel_from_cache uses PyPICacheProvider, not hooks."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    bt.cache_wheel_server_url = "https://cache.test/simple/"

    mock_provider = Mock()
    mock_cache_provider.return_value = mock_provider
    # Raise so the except clause returns (None, None) before hitting
    # network calls later in the function.
    mock_find_all.side_effect = RuntimeError("no match")

    with patch("fromager.overrides.find_and_invoke") as mock_override:
        result = _download_wheel_from_cache(
            bt.ctx,
            bt.cache_wheel_server_url,
            req=Requirement("test-pkg"),
            resolved_version=Version("1.0.0"),
        )

    assert result == (None, None)

    # Hook system must NOT be called for cache lookups
    mock_override.assert_not_called()

    # PyPICacheProvider must be instantiated directly
    mock_cache_provider.assert_called_once_with(
        cache_server_url="https://cache.test/simple/",
        constraints=tmp_context.constraints,
    )
    mock_find_all.assert_called_once_with(mock_provider, Requirement("test-pkg==1.0.0"))


def _make_cache_bootstrapper(
    tmp_context: WorkContext,
) -> bootstrapper.Bootstrapper:
    bt = bootstrapper.Bootstrapper(tmp_context)
    bt.cache_wheel_server_url = "https://cache.test/simple"
    return bt


def test_cache_lookup_resolver_exception_logs_info(
    tmp_context: WorkContext,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """ResolverException (wheel not found) returns (None, None) and logs info."""
    bt = _make_cache_bootstrapper(tmp_context)

    with (
        caplog.at_level(logging.INFO, logger="fromager.bootstrapper"),
        patch(
            "fromager.resolver.find_all_matching_from_provider",
            side_effect=ResolverException("no matching version"),
        ),
    ):
        result = _download_wheel_from_cache(
            bt.ctx,
            bt.cache_wheel_server_url,
            req=Requirement("test-package"),
            resolved_version=Version("1.0.0"),
        )

    assert result == (None, None)
    assert "did not find wheel for" in caplog.text


@pytest.mark.parametrize(
    "exc_class,exc_msg",
    [
        (requests.exceptions.ConnectionError, "DNS failure"),
        (requests.exceptions.Timeout, "timed out"),
        (requests.exceptions.HTTPError, "401 Unauthorized"),
    ],
)
def test_cache_lookup_request_exception_logs_warning(
    tmp_context: WorkContext,
    caplog: pytest.LogCaptureFixture,
    exc_class: type[Exception],
    exc_msg: str,
) -> None:
    """RequestException subtypes return (None, None) and log warning."""
    bt = _make_cache_bootstrapper(tmp_context)

    with patch(
        "fromager.resolver.find_all_matching_from_provider",
        side_effect=exc_class(exc_msg),
    ):
        result = _download_wheel_from_cache(
            bt.ctx,
            bt.cache_wheel_server_url,
            req=Requirement("test-package"),
            resolved_version=Version("1.0.0"),
        )

    assert result == (None, None)
    assert "network error checking wheel cache" in caplog.text


def test_cache_lookup_unexpected_exception_logs_warning(
    tmp_context: WorkContext,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unexpected exceptions return (None, None) and log warning."""
    bt = _make_cache_bootstrapper(tmp_context)

    with patch(
        "fromager.resolver.find_all_matching_from_provider",
        side_effect=ValueError("unexpected parsing error"),
    ):
        result = _download_wheel_from_cache(
            bt.ctx,
            bt.cache_wheel_server_url,
            req=Requirement("test-package"),
            resolved_version=Version("1.0.0"),
        )

    assert result == (None, None)
    assert "unexpected error checking wheel cache" in caplog.text


@pytest.mark.parametrize(
    "exc_class,exc_msg,expected_log",
    [
        (
            requests.exceptions.ConnectionError,
            "connection reset",
            "network error checking wheel cache",
        ),
        (OSError, "disk full", "unexpected error checking wheel cache"),
    ],
)
def test_cache_lookup_download_wheel_error_logs_warning(
    tmp_context: WorkContext,
    caplog: pytest.LogCaptureFixture,
    exc_class: type[Exception],
    exc_msg: str,
    expected_log: str,
) -> None:
    """Errors from download_wheel (after resolve succeeds) are caught."""
    bt = _make_cache_bootstrapper(tmp_context)

    with (
        patch(
            "fromager.resolver.find_all_matching_from_provider",
            return_value=[
                (
                    "https://cache.test/simple/test-package/test_package-1.0.0-py3-none-any.whl",
                    Version("1.0.0"),
                ),
            ],
        ),
        patch(
            "fromager.wheels.extract_info_from_wheel_file",
            return_value=("test_package", "1.0.0", None, None),
        ),
        patch(
            "fromager.wheels.download_wheel",
            side_effect=exc_class(exc_msg),
        ),
    ):
        result = _download_wheel_from_cache(
            bt.ctx,
            bt.cache_wheel_server_url,
            req=Requirement("test-package"),
            resolved_version=Version("1.0.0"),
        )

    assert result == (None, None)
    assert expected_log in caplog.text


def test_cache_lookup_no_cache_url_returns_none(tmp_context: WorkContext) -> None:
    """When no cache URL is configured, returns (None, None) immediately."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    bt.cache_wheel_server_url = ""

    result = _download_wheel_from_cache(
        bt.ctx,
        bt.cache_wheel_server_url,
        req=Requirement("test-package"),
        resolved_version=Version("1.0.0"),
    )

    assert result == (None, None)


def _make_resolve_item(
    req: str = "testpkg",
    req_type: RequirementType = RequirementType.TOP_LEVEL,
    why_snapshot: list[tuple[RequirementType, Requirement, Version]] | None = None,
    parent: tuple[Requirement, Version] | None = None,
) -> Resolve:
    return Resolve(
        WorkItem(
            req=Requirement(req),
            req_type=req_type,
            why_snapshot=why_snapshot or [],
            parent=parent,
        )
    )


def _record_and_load(
    bt: bootstrapper.Bootstrapper, stack: list[Phase]
) -> list[typing.Any]:
    bt._record_stack_state(stack)
    return typing.cast(list[typing.Any], json.loads(bt._stack_filename.read_text()))


def test_record_stack_state_minimal_item(tmp_context: WorkContext) -> None:
    """Minimal RESOLVE-phase item serializes with all optional fields None/empty."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    contents = _record_and_load(bt, [_make_resolve_item()])

    result = contents[0]
    assert result["req"] == "testpkg"
    assert result["req_type"] == str(RequirementType.TOP_LEVEL)
    assert result["phase"] == str(BootstrapPhase.RESOLVE)
    assert result["resolved_version"] is None
    assert result["source_url"] is None
    assert result["build_sdist_only"] is False
    assert result["why"] == []
    assert result["parent"] is None
    assert result["build_system_deps"] == []
    assert result["build_backend_deps"] == []
    assert result["build_sdist_deps"] == []


def test_record_stack_state_full_item(tmp_context: WorkContext) -> None:
    """Fully-populated item serializes resolved_version, parent, why, and dep sets."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    parent_req = Requirement("parent-pkg")
    parent_version = Version("2.0")
    why_snapshot = [(RequirementType.INSTALL, parent_req, parent_version)]

    item = Build(
        WorkItem(
            req=Requirement("child-pkg>=1.0"),
            req_type=RequirementType.INSTALL,
            why_snapshot=why_snapshot,
            parent=(parent_req, parent_version),
            resolved_version=Version("1.5"),
            source_url="https://pypi.test/child-pkg-1.5.tar.gz",
            build_sdist_only=True,
            build_system_deps={Requirement("setuptools")},
            build_backend_deps={Requirement("wheel")},
            build_sdist_deps={Requirement("flit-core")},
        )
    )

    contents = _record_and_load(bt, [item])
    result = contents[0]

    assert result["resolved_version"] == "1.5"
    assert result["source_url"] == "https://pypi.test/child-pkg-1.5.tar.gz"
    assert result["build_sdist_only"] is True
    assert result["why"] == [
        {
            "req_type": str(RequirementType.INSTALL),
            "req": "parent-pkg",
            "version": "2.0",
        }
    ]
    assert result["parent"] == {"req": "parent-pkg", "version": "2.0"}
    assert result["build_system_deps"] == ["setuptools"]
    assert result["build_backend_deps"] == ["wheel"]
    assert result["build_sdist_deps"] == ["flit-core"]


def test_record_stack_state_dep_sets_are_sorted(tmp_context: WorkContext) -> None:
    """Mixed-order dep sets come out alphabetically sorted."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    item = Build(
        WorkItem(
            req=Requirement("mypkg"),
            req_type=RequirementType.TOP_LEVEL,
            why_snapshot=[],
            build_system_deps={
                Requirement("zzz"),
                Requirement("aaa"),
                Requirement("mmm"),
            },
        )
    )

    contents = _record_and_load(bt, [item])
    assert contents[0]["build_system_deps"] == ["aaa", "mmm", "zzz"]


def test_record_stack_state_writes_file(tmp_context: WorkContext) -> None:
    """File is created; list length matches stack size."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    stack: list[Phase] = [
        _make_resolve_item("pkga"),
        _make_resolve_item("pkgb"),
    ]

    bt._record_stack_state(stack)

    assert bt._stack_filename.exists()
    contents = json.loads(bt._stack_filename.read_text())
    assert isinstance(contents, list)
    assert len(contents) == 2


def test_record_stack_state_ordering(tmp_context: WorkContext) -> None:
    """Index 0 = stack[-1] (next to pop); last index = stack[0]."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    stack: list[Phase] = [
        _make_resolve_item("pkga"),
        _make_resolve_item("pkgb"),
        _make_resolve_item("pkgc"),
    ]

    contents = _record_and_load(bt, stack)

    assert contents[0]["req"] == "pkgc"
    assert contents[-1]["req"] == "pkga"


def test_record_stack_state_overwrites_each_call(tmp_context: WorkContext) -> None:
    """Second call replaces first call's content when throttle interval has elapsed."""
    bt = bootstrapper.Bootstrapper(tmp_context)

    bt._record_stack_state([_make_resolve_item("pkga"), _make_resolve_item("pkgb")])
    first_content = bt._stack_filename.read_text()

    # Reset throttle so the second call is not suppressed
    bt._last_stack_write = 0.0
    bt._record_stack_state([_make_resolve_item("pkgc")])
    second_content = bt._stack_filename.read_text()

    assert first_content != second_content
    contents = json.loads(second_content)
    assert len(contents) == 1
    assert contents[0]["req"] == "pkgc"


def test_record_stack_state_throttled_when_called_rapidly(
    tmp_context: WorkContext,
) -> None:
    """Rapid successive calls do not overwrite the file (throttle active)."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    stack: list[Phase] = [_make_resolve_item("pkga"), _make_resolve_item("pkgb")]

    # First call writes (interval has elapsed from epoch)
    bt._record_stack_state(stack)
    first_mtime = bt._stack_filename.stat().st_mtime

    # Second call is throttled — file should not change
    bt._record_stack_state([_make_resolve_item("pkgc")])
    second_mtime = bt._stack_filename.stat().st_mtime

    # NOTE: This assertion compares st_mtime values for equality. On filesystems
    # with coarse mtime resolution (e.g. 1-second), two calls that span a
    # resolution boundary could produce equal mtimes even when the file was
    # rewritten, or unequal mtimes when it was not. APFS (nanosecond resolution)
    # makes this reliable in practice, but the test is inherently filesystem-
    # dependent.
    assert first_mtime == second_mtime


def test_finalize_writes_build_order_and_graph(tmp_context: WorkContext) -> None:
    """finalize() writes build-order.json and graph.json, and drains the write pool."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    bt.add_to_build_order(
        req=Requirement("mypkg==1.0"),
        version=Version("1.0"),
        source_url="https://pypi.test/mypkg-1.0.tar.gz",
        source_type=SourceType.SDIST,
    )

    assert not bt._build_order_filename.exists()

    bt.finalize()

    assert bt._build_order_filename.exists()
    assert bt.ctx.graph_file.exists()
    assert bt._write_pool is None  # pool was drained and closed

    contents = json.loads(bt._build_order_filename.read_text())
    assert len(contents) == 1
    assert contents[0]["dist"] == "mypkg"


def test_check_write_error_logs_on_failure(
    tmp_context: WorkContext, caplog: pytest.LogCaptureFixture
) -> None:
    """_check_write_error() logs an error when the background write future raises."""
    bt = bootstrapper.Bootstrapper(tmp_context)

    # Arrange: create a future that has already failed with an OSError
    import concurrent.futures

    fut: concurrent.futures.Future[int] = concurrent.futures.Future()
    fut.set_exception(OSError("disk full"))

    # Act
    with caplog.at_level(logging.ERROR):
        bt._check_write_error(fut)

    # Assert: error is logged, not raised
    assert any("disk full" in r.message for r in caplog.records)


def test_bootstrap_calls_record_stack_state(tmp_context: WorkContext) -> None:
    """`_record_stack_state` is called at least once during `bootstrap()`."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    call_count = {"n": 0}

    original = bt._record_stack_state

    def counting_record(stack: list[Phase]) -> None:
        call_count["n"] += 1
        original(stack)

    req = Requirement("testpkg")

    with (
        patch.object(bt, "_record_stack_state", side_effect=counting_record),
        patch.object(
            bt._resolver,
            "resolve",
            return_value=[("https://pypi.test/testpkg-1.0.tar.gz", Version("1.0"))],
        ),
        patch.object(Start, "run", return_value=[]),
    ):
        bt._bootstrap_one(req=req, req_type=RequirementType.TOP_LEVEL)

    assert call_count["n"] >= 1


def test_bootstrap_with_empty_list(tmp_context: WorkContext) -> None:
    """bootstrap([]) completes without error and runs no phases."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    with patch.object(Resolve, "run") as mock_run:
        bt.bootstrap([])
    mock_run.assert_not_called()


def test_bootstrap_with_single_requirement(tmp_context: WorkContext) -> None:
    """bootstrap([req]) resolves and processes the requirement."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    req = Requirement("testpkg==1.0")
    captured: list[Resolve] = []

    def capture_run(self: Resolve, bt_arg: bootstrapper.Bootstrapper) -> list[Phase]:
        captured.append(self)
        return []

    with (
        patch.object(
            bt,
            "_resolve_and_add_top_level",
            return_value=("http://example.test/testpkg-1.0.tar.gz", Version("1.0")),
        ),
        patch.object(Resolve, "run", capture_run),
        patch.object(bt, "_record_stack_state"),
    ):
        bt.bootstrap([req])

    assert len(captured) == 1
    assert isinstance(captured[0], Resolve)
    assert captured[0].work_item.req == req
    assert captured[0].work_item.req_type == RequirementType.TOP_LEVEL


def test_bootstrap_skips_failed_resolution(tmp_context: WorkContext) -> None:
    """bootstrap() skips requirements whose resolution returns None."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    req = Requirement("badpkg")

    with (
        patch.object(bt, "_resolve_and_add_top_level", return_value=None),
        patch.object(Resolve, "run") as mock_run,
        patch.object(bt, "_record_stack_state"),
    ):
        bt.bootstrap([req])

    mock_run.assert_not_called()


def test_bootstrap_two_requirements_both_processed(tmp_context: WorkContext) -> None:
    """bootstrap() processes all requirements in the list."""
    bt = bootstrapper.Bootstrapper(tmp_context)
    req1 = Requirement("pkg1==1.0")
    req2 = Requirement("pkg2==2.0")

    dispatch_calls: list = []

    def capture_run(self: Resolve, bt_arg: bootstrapper.Bootstrapper) -> list[Phase]:
        dispatch_calls.append(self.work_item.req.name)
        return []

    with (
        patch.object(
            bt,
            "_resolve_and_add_top_level",
            return_value=("http://example.test/pkg-1.0.tar.gz", Version("1.0")),
        ),
        patch.object(Resolve, "run", capture_run),
        patch.object(bt, "_record_stack_state"),
    ):
        bt.bootstrap([req1, req2])

    assert sorted(dispatch_calls) == ["pkg1", "pkg2"]


def test_bg_prepare_source_log_prefix_includes_version(
    tmp_context: WorkContext,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """_bg_prepare_source log messages include name-version prefix when called with context."""
    old_factory = logging.getLogRecordFactory()
    logging.setLogRecordFactory(log.FromagerLogRecord)
    req = Requirement("mypkg==1.2.3")
    version = Version("1.2.3")

    messages: list[str] = []
    try:
        with (
            caplog.at_level(logging.INFO, logger="fromager.bootstrapper"),
            patch(
                "fromager.bootstrapper._cache.find_cached_wheel",
                return_value=(None, None),
            ),
            patch(
                "fromager.sources.download_source",
                return_value=pathlib.Path("mypkg-1.2.3.tar.gz"),
            ),
            patch(
                "fromager.sources.prepare_source",
                return_value=pathlib.Path(tmp_context.work_dir / "mypkg-1.2.3"),
            ),
            log.req_ctxvar_context(req, version),
        ):
            _bg_prepare_source(
                ctx=tmp_context,
                cache_wheel_server_url=None,
                req=req,
                resolved_version=version,
                source_url="https://pkg.test/simple/mypkg/mypkg-1.2.3.tar.gz",
            )
            # Collect messages while context vars are still set so getMessage()
            # returns the prefixed form.
            messages = [
                r.getMessage()
                for r in caplog.records
                if r.name.startswith("fromager.bootstrapper")
            ]
    finally:
        logging.setLogRecordFactory(old_factory)

    for msg in messages:
        assert msg.startswith("mypkg-1.2.3: "), (
            f"Expected 'mypkg-1.2.3: ' prefix, got: {msg!r}"
        )


def test_bg_prepare_prebuilt_log_prefix_includes_version(
    tmp_context: WorkContext,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`bg_prepare_prebuilt` log message includes name-version prefix when called with context."""
    old_factory = logging.getLogRecordFactory()
    logging.setLogRecordFactory(log.FromagerLogRecord)
    req = Requirement("mypkg==1.2.3")
    version = Version("1.2.3")

    messages: list[str] = []
    try:
        with (
            caplog.at_level(logging.INFO, logger="fromager.bootstrapper"),
            patch(
                "fromager.wheels.download_wheel",
                return_value=pathlib.Path("mypkg-1.2.3-py3-none-any.whl"),
            ),
            patch("fromager.server.update_wheel_mirror"),
            log.req_ctxvar_context(req, version),
        ):
            bg_prepare_prebuilt(
                ctx=tmp_context,
                req=req,
                req_type=RequirementType.INSTALL,
                resolved_version=version,
                wheel_url="https://pkg.test/simple/mypkg/mypkg-1.2.3-py3-none-any.whl",
            )
            # Collect messages while context vars are still set so getMessage()
            # returns the prefixed form.
            messages = [
                r.getMessage()
                for r in caplog.records
                if r.name.startswith("fromager.bootstrapper")
            ]
    finally:
        logging.setLogRecordFactory(old_factory)

    assert len(messages) >= 1
    for msg in messages:
        assert msg.startswith("mypkg-1.2.3: "), (
            f"Expected 'mypkg-1.2.3: ' prefix, got: {msg!r}"
        )


def test_build_item_build_sdist_finds_existing(tmp_context: WorkContext) -> None:
    """Build._build_sdist returns cached sdist when finders.find_sdist hits."""
    sdist_root = tmp_context.work_dir / "testpkg-1.0" / "testpkg-1.0"
    sdist_root.mkdir(parents=True, exist_ok=True)
    wi = WorkItem(
        req=Requirement("testpkg"),
        req_type=RequirementType.TOP_LEVEL,
        why_snapshot=[],
        resolved_version=Version("1.0"),
        sdist_root_dir=sdist_root,
        build_env=Mock(),
    )
    item = Build(wi)
    cached = tmp_context.sdists_builds / "testpkg-1.0.tar.gz"
    cached.touch()

    with patch("fromager.finders.find_sdist", return_value=cached):
        result = item._build_sdist(tmp_context)

    assert result == cached


def test_build_item_build_sdist_calls_build_when_not_cached(
    tmp_context: WorkContext,
) -> None:
    """Build._build_sdist calls sources.build_sdist when no cached sdist found."""
    sdist_root = tmp_context.work_dir / "testpkg-1.0" / "testpkg-1.0"
    sdist_root.mkdir(parents=True, exist_ok=True)
    wi = WorkItem(
        req=Requirement("testpkg"),
        req_type=RequirementType.TOP_LEVEL,
        why_snapshot=[],
        resolved_version=Version("1.0"),
        sdist_root_dir=sdist_root,
        build_env=Mock(),
    )
    item = Build(wi)
    built = tmp_context.sdists_builds / "testpkg-1.0.tar.gz"

    with (
        patch("fromager.finders.find_sdist", return_value=None),
        patch("fromager.sources.build_sdist", return_value=built) as mock_build,
    ):
        result = item._build_sdist(tmp_context)

    mock_build.assert_called_once_with(
        ctx=tmp_context,
        req=wi.req,
        version=wi.resolved_version,
        sdist_root_dir=sdist_root,
        build_env=wi.build_env,
    )
    assert result == built


def test_build_item_build_wheel(tmp_context: WorkContext) -> None:
    """Build._build_wheel builds sdist then wheel and updates mirror."""
    sdist_root = tmp_context.work_dir / "testpkg-1.0" / "testpkg-1.0"
    sdist_root.mkdir(parents=True, exist_ok=True)
    wi = WorkItem(
        req=Requirement("testpkg"),
        req_type=RequirementType.TOP_LEVEL,
        why_snapshot=[],
        resolved_version=Version("1.0"),
        sdist_root_dir=sdist_root,
        build_env=Mock(),
    )
    item = Build(wi)
    built_wheel = tmp_context.wheels_build / "testpkg-1.0-py3-none-any.whl"
    built_sdist = tmp_context.sdists_builds / "testpkg-1.0.tar.gz"

    with (
        patch.object(
            item, "_build_sdist", return_value=built_sdist
        ) as mock_build_sdist,
        patch("fromager.wheels.build_wheel", return_value=built_wheel),
        patch("fromager.server.update_wheel_mirror") as mock_update_mirror,
    ):
        wheel_filename, sdist_filename = item._build_wheel(tmp_context)

    mock_build_sdist.assert_called_once_with(tmp_context)
    mock_update_mirror.assert_called_once_with(tmp_context)
    assert wheel_filename == tmp_context.wheels_downloads / built_wheel.name
    assert sdist_filename == built_sdist


def test_build_item_do_build_returns_cached_wheel(
    tmp_context: WorkContext,
) -> None:
    """Build.do_build returns cached wheel immediately without building."""
    cached = tmp_context.wheels_downloads / "testpkg-1.0-py3-none-any.whl"
    cached.touch()
    wi = WorkItem(
        req=Requirement("testpkg"),
        req_type=RequirementType.TOP_LEVEL,
        why_snapshot=[],
        resolved_version=Version("1.0"),
        build_sdist_only=False,
        cached_wheel_filename=cached,
        sdist_root_dir=tmp_context.work_dir,
        build_env=Mock(),
    )
    item = Build(wi)

    with patch.object(item, "_build_wheel") as mock_wheel:
        wheel, sdist = item.do_build(tmp_context)

    mock_wheel.assert_not_called()
    assert wheel == cached
    assert sdist is None


def test_build_item_do_build_sdist_only(tmp_context: WorkContext) -> None:
    """Build.do_build calls _build_sdist and returns (None, sdist) when build_sdist_only=True."""
    built_sdist = tmp_context.sdists_builds / "testpkg-1.0.tar.gz"
    sdist_root = tmp_context.work_dir / "testpkg-1.0" / "testpkg-1.0"
    sdist_root.mkdir(parents=True, exist_ok=True)
    wi = WorkItem(
        req=Requirement("testpkg"),
        req_type=RequirementType.TOP_LEVEL,
        why_snapshot=[],
        resolved_version=Version("1.0"),
        build_sdist_only=True,
        cached_wheel_filename=None,
        sdist_root_dir=sdist_root,
        build_env=Mock(),
    )
    item = Build(wi)

    with (
        patch.object(item, "_build_sdist", return_value=built_sdist) as mock_sdist,
        patch.object(item, "_build_wheel") as mock_wheel,
    ):
        wheel, sdist = item.do_build(tmp_context)

    mock_wheel.assert_not_called()
    mock_sdist.assert_called_once_with(tmp_context)
    assert wheel is None
    assert sdist == built_sdist


def test_build_item_do_build_builds_wheel(tmp_context: WorkContext) -> None:
    """Build.do_build calls _build_wheel when no cache and not sdist_only."""
    built_wheel = tmp_context.wheels_downloads / "testpkg-1.0-py3-none-any.whl"
    built_sdist = tmp_context.sdists_builds / "testpkg-1.0.tar.gz"
    sdist_root = tmp_context.work_dir / "testpkg-1.0" / "testpkg-1.0"
    sdist_root.mkdir(parents=True, exist_ok=True)
    wi = WorkItem(
        req=Requirement("testpkg"),
        req_type=RequirementType.TOP_LEVEL,
        why_snapshot=[],
        resolved_version=Version("1.0"),
        build_sdist_only=False,
        cached_wheel_filename=None,
        sdist_root_dir=sdist_root,
        build_env=Mock(),
    )
    item = Build(wi)

    with patch.object(
        item, "_build_wheel", return_value=(built_wheel, built_sdist)
    ) as mock_wheel:
        wheel, sdist = item.do_build(
            tmp_context, explain="top_level dependency testpkg (1.0)"
        )

    mock_wheel.assert_called_once_with(
        tmp_context, "top_level dependency testpkg (1.0)"
    )
    assert wheel == built_wheel
    assert sdist == built_sdist
