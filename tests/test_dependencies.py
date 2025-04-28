import functools
import itertools
import pathlib
import shutil
import typing
from unittest.mock import Mock, patch

import pytest
from packaging.requirements import Requirement

from fromager import build_environment, context, dependencies

_fromager_root = pathlib.Path(__file__).parent.parent


@pytest.mark.parametrize(
    "build_system,expected_results",
    [
        # Empty
        ({}, dependencies._DEFAULT_BACKEND),
        # Only specify requirements (pyarrow)
        (
            {"requires": ["a-dep"]},
            {
                "build-backend": "setuptools.build_meta:__legacy__",
                "backend-path": None,
                "requires": ["a-dep"],
            },
        ),
        # Specify everything
        (
            {
                "build-backend": "setuptools.build_meta:__legacy__",
                "backend-path": None,
                "requires": ["a-dep"],
            },
            {
                "build-backend": "setuptools.build_meta:__legacy__",
                "backend-path": None,
                "requires": ["a-dep"],
            },
        ),
    ],
)
def test_get_build_backend(
    build_system: dict[str, list[str]] | dict[str, str | list[str] | None],
    expected_results: dict[str, typing.Any] | dict[str, str | list[str] | None],
):
    pyproject_toml = {"build-system": build_system}
    actual = dependencies.get_build_backend(pyproject_toml)
    assert expected_results == actual


def _clean_build_artifacts(f):
    @functools.wraps(f)
    def _with_cleanup(*args, **kwds):
        try:
            f(*args, **kwds)
        finally:
            for d in itertools.chain(
                _fromager_root.glob("fromager-*.dist-info"),
                _fromager_root.glob("fromager.egg-info"),
            ):
                shutil.rmtree(d)

    return _with_cleanup


@patch("fromager.dependencies._write_requirements_file")
@_clean_build_artifacts
def test_get_build_system_dependencies(
    _: Mock, tmp_context: context.WorkContext, tmp_path: pathlib.Path
):
    pyproject_file = _fromager_root / "pyproject.toml"
    shutil.copyfile(pyproject_file, tmp_path / "pyproject.toml")

    results = dependencies.get_build_system_dependencies(
        ctx=tmp_context,
        req=Requirement("fromager"),
        sdist_root_dir=tmp_path,
    )
    names = set(r.name for r in results)
    assert names == set(["hatchling", "hatch-vcs"])


def test_get_build_system_dependencies_cached(
    tmp_context: context.WorkContext, tmp_path: pathlib.Path
):
    sdist_root_dir = tmp_path / "sdist"
    sdist_root_dir.mkdir()

    req_file = tmp_path / "build-system-requirements.txt"
    req_file.write_text("foo==1.0")
    results = dependencies.get_build_system_dependencies(
        ctx=tmp_context,
        req=Requirement("fromager"),
        sdist_root_dir=sdist_root_dir,
    )
    assert results == set([Requirement("foo==1.0")])


@patch("fromager.dependencies._write_requirements_file")
@_clean_build_artifacts
def test_get_build_backend_dependencies(
    _: Mock, tmp_context: context.WorkContext, tmp_path: pathlib.Path
):
    req = Requirement("fromager")
    build_env = build_environment.BuildEnvironment(
        ctx=tmp_context, parent_dir=tmp_path, build_requirements=None, req=req
    )
    results = dependencies.get_build_backend_dependencies(
        ctx=tmp_context,
        req=req,
        sdist_root_dir=_fromager_root,
        build_env=build_env,
    )
    names = set(r.name for r in results)
    assert names == set()


def test_get_build_backend_dependencies_cached(
    tmp_context: context.WorkContext, tmp_path: pathlib.Path
):
    sdist_root_dir = tmp_path / "sdist"
    sdist_root_dir.mkdir()

    req_file = tmp_path / "build-backend-requirements.txt"
    req_file.write_text("foo==1.0")

    req = Requirement("fromager")
    build_env = build_environment.BuildEnvironment(
        ctx=tmp_context, parent_dir=tmp_path, build_requirements=None, req=req
    )
    results = dependencies.get_build_backend_dependencies(
        ctx=tmp_context,
        req=Requirement("fromager"),
        sdist_root_dir=sdist_root_dir,
        build_env=build_env,
    )
    assert results == set([Requirement("foo==1.0")])


@patch("fromager.dependencies._write_requirements_file")
@_clean_build_artifacts
def test_get_build_sdist_dependencies(
    _: Mock, tmp_context: context.WorkContext, tmp_path: pathlib.Path
):
    req = Requirement("fromager")
    build_env = build_environment.BuildEnvironment(
        ctx=tmp_context, parent_dir=tmp_path, build_requirements=None, req=req
    )
    results = dependencies.get_build_sdist_dependencies(
        ctx=tmp_context,
        req=req,
        sdist_root_dir=_fromager_root,
        build_env=build_env,
    )
    names = set(r.name for r in results)
    assert names == set()


def test_get_build_sdist_dependencies_cached(
    tmp_context: context.WorkContext, tmp_path: pathlib.Path
):
    sdist_root_dir = tmp_path / "sdist"
    sdist_root_dir.mkdir()

    req_file = tmp_path / "build-sdist-requirements.txt"
    req_file.write_text("foo==1.0")

    req = Requirement("fromager")
    build_env = build_environment.BuildEnvironment(
        ctx=tmp_context, parent_dir=tmp_path, build_requirements=None, req=req
    )
    results = dependencies.get_build_sdist_dependencies(
        ctx=tmp_context,
        req=req,
        sdist_root_dir=sdist_root_dir,
        build_env=build_env,
    )
    assert results == set([Requirement("foo==1.0")])
