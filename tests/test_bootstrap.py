import io
import pathlib
import textwrap
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner
from packaging.requirements import Requirement
from packaging.version import Version

from fromager import bootstrapper, context, dependency_graph, packagesettings
from fromager.commands import bootstrap


def test_get_requirements_single_arg():
    requirements = bootstrap._get_requirements_from_args(["a"], [])
    assert [Requirement("a")] == requirements


def test_get_requirements_multiple_args():
    requirements = bootstrap._get_requirements_from_args(["a", "b"], [])
    assert [Requirement("a"), Requirement("b")] == requirements


def test_get_requirements_args_and_file(tmp_path: pathlib.Path):
    requirements_file = tmp_path / "requirements.txt"
    requirements_file.write_text("c\n")
    requirements = bootstrap._get_requirements_from_args(
        ["a", "b"], [str(requirements_file)]
    )
    assert [
        Requirement("a"),
        Requirement("b"),
        Requirement("c"),
    ] == requirements


def test_ignore_based_on_marker():
    requirements = bootstrap._get_requirements_from_args(
        ['foo; python_version<"3.9"'], []
    )
    assert [] == requirements


def test_write_constraints_file_simple():
    buffer = io.StringIO()
    raw_graph = {
        "": {
            "download_url": "",
            "pre_built": False,
            "version": "0",
            "canonicalized_name": "",
            "edges": [{"key": "a==1.0", "req_type": "install", "req": "a"}],
        },
        "a==1.0": {
            "download_url": "url for a",
            "pre_built": False,
            "version": "1.0",
            "canonicalized_name": "a",
            "edges": [
                {"key": "b==2.0", "req_type": "install", "req": "b>=2.0"},
                {"key": "c==3.0", "req_type": "install", "req": "c<4.0"},
            ],
        },
        "b==2.0": {
            "download_url": "url for b",
            "pre_built": False,
            "version": "2.0",
            "canonicalized_name": "b",
            "edges": [],
        },
        "c==3.0": {
            "download_url": "url for c",
            "pre_built": False,
            "version": "3.0",
            "canonicalized_name": "c",
            "edges": [],
        },
    }
    graph = dependency_graph.DependencyGraph.from_dict(raw_graph)
    bootstrap.write_constraints_file(graph, buffer)
    expected = textwrap.dedent("""
        a==1.0
        b==2.0
        c==3.0
        """).lstrip()
    assert expected == buffer.getvalue()


def test_write_constraints_file_resolvable_duplicate():
    buffer = io.StringIO()
    raw_graph = {
        "": {
            "download_url": "",
            "pre_built": False,
            "version": "0",
            "canonicalized_name": "",
            "edges": [{"key": "a==1.0", "req_type": "install", "req": "a"}],
        },
        "a==1.0": {
            "download_url": "url for a",
            "pre_built": False,
            "version": "1.0",
            "canonicalized_name": "a",
            "edges": [
                {"key": "b==2.0", "req_type": "install", "req": "b>=2.0"},
                {"key": "c==3.0", "req_type": "install", "req": "c<4.0"},
            ],
        },
        "b==2.0": {
            "download_url": "url for b",
            "pre_built": False,
            "version": "2.0",
            "canonicalized_name": "b",
            "edges": [{"key": "c==3.1", "req_type": "install", "req": "c>3.0"}],
        },
        "c==3.0": {
            "download_url": "url for c",
            "pre_built": False,
            "version": "3.0",
            "canonicalized_name": "c",
            "edges": [],
        },
        "c==3.1": {
            "download_url": "url for c",
            "pre_built": False,
            "version": "3.1",
            "canonicalized_name": "c",
            "edges": [],
        },
    }
    graph = dependency_graph.DependencyGraph.from_dict(raw_graph)
    bootstrap.write_constraints_file(graph, buffer)
    expected = textwrap.dedent("""
        a==1.0
        b==2.0
        # NOTE: fromager selected c==3.1 from: ['3.0', '3.1']
        c==3.1
        """).lstrip()
    assert expected == buffer.getvalue()


def test_write_constraints_file_unresolvable_duplicate():
    buffer = io.StringIO()
    raw_graph = {
        "": {
            "download_url": "",
            "pre_built": False,
            "version": "0",
            "canonicalized_name": "",
            "edges": [{"key": "a==1.0", "req_type": "install", "req": "a"}],
        },
        "a==1.0": {
            "download_url": "url for a",
            "pre_built": False,
            "version": "1.0",
            "canonicalized_name": "a",
            "edges": [
                {"key": "b==2.0", "req_type": "install", "req": "b>=2.0"},
                {"key": "c==3.0", "req_type": "install", "req": "c==3.0"},
            ],
        },
        "b==2.0": {
            "download_url": "url for b",
            "pre_built": False,
            "version": "2.0",
            "canonicalized_name": "b",
            "edges": [{"key": "c==3.1", "req_type": "install", "req": "c>3.0"}],
        },
        "c==3.0": {
            "download_url": "url for c",
            "pre_built": False,
            "version": "3.0",
            "canonicalized_name": "c",
            "edges": [],
        },
        "c==3.1": {
            "download_url": "url for c",
            "pre_built": False,
            "version": "3.1",
            "canonicalized_name": "c",
            "edges": [],
        },
    }
    graph = dependency_graph.DependencyGraph.from_dict(raw_graph)
    bootstrap.write_constraints_file(graph, buffer)
    expected = textwrap.dedent("""
        a==1.0
        b==2.0
        # ERROR: no single version of c met all requirements
        c==3.0
        c==3.1
        """).lstrip()
    assert expected == buffer.getvalue()


def test_write_constraints_file_duplicates():
    buffer = io.StringIO()
    raw_graph = {
        "": {
            "download_url": "",
            "pre_built": False,
            "version": "0",
            "canonicalized_name": "",
            "edges": [
                {"key": "a==1.0", "req_type": "install", "req": "a"},
                {"key": "d==1.0", "req_type": "install", "req": "d"},
            ],
        },
        "a==1.0": {
            "download_url": "url for a",
            "pre_built": False,
            "version": "1.0",
            "canonicalized_name": "a",
            "edges": [
                {"key": "c==3.0", "req_type": "install", "req": "c<=3.0"},
            ],
        },
        "d==1.0": {
            "download_url": "url for a",
            "pre_built": False,
            "version": "1.0",
            "canonicalized_name": "a",
            "edges": [
                {"key": "c==3.1", "req_type": "install", "req": "c>=3.0"},
            ],
        },
        "c==3.0": {  # transformers 4.46
            "download_url": "url for c",
            "pre_built": False,
            "version": "3.0",
            "canonicalized_name": "c",
            "edges": [{"key": "b==2.0", "req_type": "install", "req": "b<2.1,>=2.0"}],
        },
        "c==3.1": {  # transformers 4.47
            "download_url": "url for c",
            "pre_built": False,
            "version": "3.1",
            "canonicalized_name": "c",
            "edges": [{"key": "b==2.1", "req_type": "install", "req": "b<2.2,>=2.1"}],
        },
        "b==2.0": {  # tokenizer
            "download_url": "url for b",
            "pre_built": False,
            "version": "2.0",
            "canonicalized_name": "b",
            "edges": [],
        },
        "b==2.1": {  # tokenizer
            "download_url": "url for b",
            "pre_built": False,
            "version": "2.1",
            "canonicalized_name": "b",
            "edges": [],
        },
    }
    graph = dependency_graph.DependencyGraph.from_dict(raw_graph)
    assert bootstrap.write_constraints_file(graph, buffer)
    expected = textwrap.dedent("""
        a==1.0
        # NOTE: fromager selected b==2.0 from: ['2.0', '2.1']
        b==2.0
        # NOTE: fromager selected c==3.0 from: ['3.0', '3.1']
        c==3.0
        d==1.0
        """).lstrip()
    assert expected == buffer.getvalue()


def test_write_constraints_file_multiples():
    buffer = io.StringIO()
    raw_graph = {
        "": {
            "download_url": "",
            "pre_built": False,
            "version": "0",
            "canonicalized_name": "",
            "edges": [
                {
                    "key": "a==2.7.0",
                    "req_type": "toplevel",
                    "req": "a==2.7.0",
                },
                {
                    "key": "b==0.26.1",
                    "req_type": "toplevel",
                    "req": "b==0.26.1",
                },
            ],
        },
        "b==0.26.1": {
            "download_url": "",
            "pre_built": False,
            "version": "0.26.1",
            "canonicalized_name": "b",
            "edges": [],
        },
        "b==0.26.2": {
            "download_url": "",
            "pre_built": False,
            "version": "0.26.2",
            "canonicalized_name": "b",
            "edges": [],
        },
        "a==2.7.0": {
            "download_url": "",
            "pre_built": False,
            "version": "2.7.0",
            "canonicalized_name": "a",
            "edges": [
                {
                    "key": "b==0.26.2",
                    "req_type": "install",
                    "req": "b<0.27.0,>=0.26.1",
                },
            ],
        },
    }
    graph = dependency_graph.DependencyGraph.from_dict(raw_graph)
    assert bootstrap.write_constraints_file(graph, buffer)
    expected = textwrap.dedent("""
        a==2.7.0
        # NOTE: fromager selected b==0.26.2 from: ['0.26.1', '0.26.2']
        b==0.26.2
        """).lstrip()
    assert expected == buffer.getvalue()


def test_detect_incompatible_dependencies(testdata_path: pathlib.Path):
    """Test that incompatible dependencies are detected.

    $ fromager graph why ~/Downloads/release/outputs/graph.json accelerate

    accelerate==1.8.1
    * is an install dependency of trl==0.14.0 with req accelerate>=0.34.0
    * is an install dependency of peft==0.15.2 with req accelerate>=0.21.0
    * is an install dependency of trl==0.19.0 with req accelerate>=1.4.0
    * is an install dependency of instructlab-eval==0.5.1 with req accelerate
    * is an install dependency of lm-eval==0.4.9 with req accelerate>=0.26.0

    accelerate==1.0.1
    * is an install dependency of instructlab-training==0.11 with req accelerate<1.1.0,>=0.34.2; extra == "cuda"
    """
    graph_path = testdata_path / "incompatible-dependencies-graph.json"
    graph = dependency_graph.DependencyGraph.from_file(graph_path)
    buffer = io.StringIO()
    assert not bootstrap.write_constraints_file(graph, buffer)
    assert (
        "ERROR: no single version of accelerate met all requirements"
        in buffer.getvalue()
    )


def test_skip_constraints_cli_option():
    """Test that the --skip-constraints option is available in the CLI"""
    runner = CliRunner()
    result = runner.invoke(bootstrap.bootstrap, ["--help"])

    # Check that the help text includes our new option
    assert "--skip-constraints" in result.output
    assert "Skip generating constraints.txt file" in result.output


@patch("fromager.gitutils.git_clone")
def test_resolve_version_from_git_url_with_submodules_enabled(
    mock_git_clone: Mock,
    tmp_context: context.WorkContext,
):
    """Test that git_clone is called with submodules=True when configured."""
    req = Requirement("test-pkg @ git+https://github.com/example/repo.git")

    mock_git_options = packagesettings.GitOptions(submodules=True)

    with patch.object(tmp_context, "package_build_info") as mock_pbi:
        mock_pbi_instance = Mock()
        mock_pbi_instance.git_options = mock_git_options
        mock_pbi.return_value = mock_pbi_instance

        with patch(
            "fromager.bootstrapper.Bootstrapper._get_version_from_package_metadata"
        ) as mock_get_version:
            with patch("shutil.move"):
                with patch("pathlib.Path.mkdir"):
                    mock_get_version.return_value = Version("1.0.0")

                    # Execute
                    bs = bootstrapper.Bootstrapper(tmp_context)
                    try:
                        bs._resolve_version_from_git_url(req)
                    except AssertionError:
                        # Expected since we're mocking everything
                        pass

    mock_git_clone.assert_called_once()
    call_args = mock_git_clone.call_args
    assert call_args.kwargs["submodules"] is True
    assert call_args.kwargs["repo_url"] == "https://github.com/example/repo.git"
    assert call_args.kwargs["ref"] is None


@patch("fromager.gitutils.git_clone")
def test_resolve_version_from_git_url_with_specific_submodule_paths(
    mock_git_clone: Mock,
    tmp_context: context.WorkContext,
):
    """Test that git_clone is called with specific submodule paths when configured."""
    req = Requirement("test-pkg @ git+https://github.com/example/repo.git")

    mock_git_options = packagesettings.GitOptions(
        submodule_paths=["vendor/lib1", "vendor/lib2"]
    )

    with patch.object(tmp_context, "package_build_info") as mock_pbi:
        mock_pbi_instance = Mock()
        mock_pbi_instance.git_options = mock_git_options
        mock_pbi.return_value = mock_pbi_instance

        with patch(
            "fromager.bootstrapper.Bootstrapper._get_version_from_package_metadata"
        ) as mock_get_version:
            with patch("shutil.move"):
                with patch("pathlib.Path.mkdir"):
                    mock_get_version.return_value = Version("1.0.0")

                    bs = bootstrapper.Bootstrapper(tmp_context)
                    try:
                        bs._resolve_version_from_git_url(req)
                    except AssertionError:
                        # Expected since we're mocking everything
                        pass

    mock_git_clone.assert_called_once()
    call_args = mock_git_clone.call_args
    assert call_args.kwargs["submodules"] == ["vendor/lib1", "vendor/lib2"]


@patch("fromager.gitutils.git_clone")
def test_resolve_version_from_git_url_with_submodules_disabled(
    mock_git_clone: Mock,
    tmp_context: context.WorkContext,
):
    """Test that git_clone is called with submodules=False by default."""
    req = Requirement("test-pkg @ git+https://github.com/example/repo.git")

    with patch(
        "fromager.bootstrapper.Bootstrapper._get_version_from_package_metadata"
    ) as mock_get_version:
        with patch("shutil.move"):
            with patch("pathlib.Path.mkdir"):
                mock_get_version.return_value = Version("1.0.0")

                bs = bootstrapper.Bootstrapper(tmp_context)
                try:
                    bs._resolve_version_from_git_url(req)
                except AssertionError:
                    # Expected since we're mocking everything
                    pass

    mock_git_clone.assert_called_once()
    call_args = mock_git_clone.call_args
    assert call_args.kwargs["submodules"] is False


@patch("fromager.gitutils.git_clone")
def test_resolve_version_from_git_url_with_git_ref(
    mock_git_clone: Mock,
    tmp_context: context.WorkContext,
):
    """Test that git_clone is called with the correct ref when URL includes @ref."""
    req = Requirement("test-pkg @ git+https://github.com/example/repo.git@v1.2.3")

    mock_git_options = packagesettings.GitOptions(submodules=True)

    with patch.object(tmp_context, "package_build_info") as mock_pbi:
        mock_pbi_instance = Mock()
        mock_pbi_instance.git_options = mock_git_options
        mock_pbi.return_value = mock_pbi_instance

        with patch(
            "fromager.bootstrapper.Bootstrapper._get_version_from_package_metadata"
        ) as mock_get_version:
            with patch("shutil.move"):
                with patch("pathlib.Path.mkdir"):
                    mock_get_version.return_value = Version("1.2.3")

                    bs = bootstrapper.Bootstrapper(tmp_context)
                    try:
                        bs._resolve_version_from_git_url(req)
                    except AssertionError:
                        # Expected since we're mocking everything
                        pass

    mock_git_clone.assert_called_once()
    call_args = mock_git_clone.call_args
    assert call_args.kwargs["submodules"] is True
    assert call_args.kwargs["repo_url"] == "https://github.com/example/repo.git"
    assert call_args.kwargs["ref"] == "v1.2.3"


def test_resolve_version_from_git_url_invalid_scheme(tmp_context: context.WorkContext):
    """Test that non-git URLs raise ValueError."""
    req = Requirement("test-pkg @ https://github.com/example/repo.git")

    bs = bootstrapper.Bootstrapper(tmp_context)
    with pytest.raises(ValueError, match="unable to handle URL scheme"):
        bs._resolve_version_from_git_url(req)
