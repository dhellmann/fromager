import pathlib
import typing
from unittest.mock import Mock, patch

import pydantic
import pytest
from packaging.requirements import Requirement
from packaging.utils import NormalizedName
from packaging.version import Version

from fromager import build_environment, context
from fromager.packagesettings import (
    BuildDirectory,
    EnvVars,
    Package,
    PackageSettings,
    SettingsFile,
    Variant,
    substitute_template,
)

TEST_PKG = "test-pkg"
TEST_EMPTY_PKG = "test-empty-pkg"
TEST_OTHER_PKG = "test-other-pkg"
TEST_RELATED_PKG = "test-pkg-library"

FULL_EXPECTED: dict[str, typing.Any] = {
    "build_dir": pathlib.Path("python"),
    "build_options": {
        "build_ext_parallel": True,
        "cpu_cores_per_job": 4,
        "memory_per_job_gb": 4.0,
    },
    "changelog": {
        Version("1.0.1"): ["fixed bug"],
        Version("1.0.2"): ["more bugs", "rebuild"],
    },
    "config_settings": {
        "setup-args": [
            "-Dsystem-freetype=true",
            "-Dsystem-qhull=true",
        ],
    },
    "download_source": {
        "destination_filename": "${canonicalized_name}-${version}.tar.gz",
        "url": "https://egg.test/${canonicalized_name}/v${version}.tar.gz",
    },
    "env": {
        "EGG": "spam",
        "EGG_AGAIN": "$EGG",
        "SPAM": "alot $EXTRA",
        "QUOTES": "A\"BC'$$EGG",
        "DEF": "${DEF:-default}",
    },
    "name": "test-pkg",
    "has_config": True,
    "project_override": {
        "remove_build_requires": ["cmake"],
        "update_build_requires": ["setuptools>=68.0.0", "torch"],
        "requires_external": ["openssl-libs"],
    },
    "resolver_dist": {
        "include_sdists": True,
        "include_wheels": False,
        "sdist_server_url": "https://sdist.test/egg",
    },
    "variants": {
        "cpu": {
            "env": {"EGG": "spam ${EGG}", "EGG_AGAIN": "$EGG"},
            "wheel_server_url": "https://wheel.test/simple",
            "pre_built": False,
        },
        "rocm": {
            "env": {"SPAM": ""},
            "wheel_server_url": None,
            "pre_built": True,
        },
        "cuda": {
            "env": {},
            "wheel_server_url": None,
            "pre_built": False,
        },
    },
    "exclusive_build": False,
}

EMPTY_EXPECTED: dict[str, typing.Any] = {
    "name": "test-empty-pkg",
    "build_dir": None,
    "build_options": {
        "build_ext_parallel": False,
        "cpu_cores_per_job": 1,
        "memory_per_job_gb": 1.0,
    },
    "changelog": {},
    "config_settings": {},
    "env": {},
    "download_source": {
        "url": None,
        "destination_filename": None,
    },
    "has_config": True,
    "project_override": {
        "remove_build_requires": [],
        "update_build_requires": [],
        "requires_external": [],
    },
    "resolver_dist": {
        "sdist_server_url": None,
        "include_sdists": True,
        "include_wheels": False,
    },
    "variants": {},
    "exclusive_build": False,
}


def test_parse_full(testdata_path: pathlib.Path) -> None:
    filename = testdata_path / "context/overrides/settings/test_pkg.yaml"
    p = PackageSettings.from_string(TEST_PKG, filename.read_text())
    assert p.model_dump() == FULL_EXPECTED


def test_parse_full_file(testdata_path: pathlib.Path) -> None:
    filename = testdata_path / "context/overrides/settings/test_pkg.yaml"
    p = PackageSettings.from_file(filename)
    assert p.model_dump() == FULL_EXPECTED


def test_parse_minimal(testdata_path) -> None:
    filename = testdata_path / "context/overrides/settings/test_empty_pkg.yaml"
    p = PackageSettings.from_string(TEST_EMPTY_PKG, filename.read_text())
    assert p.model_dump() == EMPTY_EXPECTED


def test_parse_minimal_file(testdata_path: pathlib.Path) -> None:
    filename = testdata_path / "context/overrides/settings/test_empty_pkg.yaml"
    p = PackageSettings.from_file(filename)
    assert p.model_dump() == EMPTY_EXPECTED


def test_default_settings() -> None:
    p = PackageSettings.from_default(TEST_EMPTY_PKG)
    expected = EMPTY_EXPECTED.copy()
    expected["has_config"] = False
    assert p.model_dump() == expected


def test_pbi_test_pkg_extra_environ(
    tmp_path: pathlib.Path, testdata_context: context.WorkContext
) -> None:
    testdata_context.settings.max_jobs = 1
    parallel = {
        "CMAKE_BUILD_PARALLEL_LEVEL": "1",
        "MAKEFLAGS": "-j1",
        "MAX_JOBS": "1",
    }

    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    assert (
        pbi.get_extra_environ(template_env={"EXTRA": "extra"})
        == {
            "EGG": "spam spam",
            "EGG_AGAIN": "spam spam",
            "QUOTES": "A\"BC'$EGG",  # $$EGG is transformed into $EGG
            "SPAM": "alot extra",
            "DEF": "default",
        }
        | parallel
    )
    assert (
        pbi.get_extra_environ(template_env={"EXTRA": "extra", "DEF": "nondefault"})
        == {
            "EGG": "spam spam",
            "EGG_AGAIN": "spam spam",
            "QUOTES": "A\"BC'$EGG",  # $$EGG is transformed into $EGG
            "SPAM": "alot extra",
            "DEF": "nondefault",
        }
        | parallel
    )

    testdata_context.settings.variant = Variant("rocm")
    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    assert (
        pbi.get_extra_environ(template_env={"EXTRA": "extra"})
        == {
            "EGG": "spam",
            "EGG_AGAIN": "spam",
            "QUOTES": "A\"BC'$EGG",
            "SPAM": "",
            "DEF": "default",
        }
        | parallel
    )

    testdata_context.settings.variant = Variant("cuda")
    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    assert (
        pbi.get_extra_environ(template_env={"EXTRA": "spam"})
        == {
            "EGG": "spam",
            "EGG_AGAIN": "spam",
            "QUOTES": "A\"BC'$EGG",
            "SPAM": "alot spam",
            "DEF": "default",
        }
        | parallel
    )

    build_env = build_environment.BuildEnvironment(
        testdata_context,
        parent_dir=tmp_path,
        build_requirements=None,
        req=Requirement("test_pkg"),
    )
    result = pbi.get_extra_environ(
        template_env={"EXTRA": "spam", "PATH": "/sbin:/bin"}, build_env=build_env
    )
    assert (
        result
        == {
            "EGG": "spam",
            "EGG_AGAIN": "spam",
            "QUOTES": "A\"BC'$EGG",
            "SPAM": "alot spam",
            "DEF": "default",
            "PATH": f"{build_env.path / 'bin'}:/sbin:/bin",
            "VIRTUAL_ENV": str(build_env.path),
        }
        | parallel
    )


def test_pbi_test_pkg(testdata_context: context.WorkContext) -> None:
    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    assert pbi.package == NormalizedName(TEST_PKG)
    assert pbi.variant == Variant(testdata_context.settings.variant)
    assert pbi.pre_built is False
    assert pbi.has_config is True
    assert pbi.wheel_server_url == "https://wheel.test/simple"
    assert pbi.override_module_name == "test_pkg"
    assert (
        pbi.download_source_url(Version("1.0.2"), resolve_template=False)
        == "https://egg.test/${canonicalized_name}/v${version}.tar.gz"
    )
    assert (
        pbi.download_source_url(Version("1.0.2"))
        == "https://egg.test/test-pkg/v1.0.2.tar.gz"
    )
    assert (
        pbi.download_source_destination_filename(
            Version("1.0.2"), resolve_template=False
        )
        == "${canonicalized_name}-${version}.tar.gz"
    )
    assert (
        pbi.download_source_destination_filename(Version("1.0.2"))
        == "test-pkg-1.0.2.tar.gz"
    )
    assert pbi.resolver_include_sdists is True
    assert pbi.resolver_include_wheels is False
    assert (
        pbi.resolver_sdist_server_url("https://pypi.org/simple")
        == "https://sdist.test/egg"
    )
    assert pbi.build_tag(Version("1.0.2")) == (2, "")
    sdist_root_dir = pathlib.Path("/sdist-root")
    assert pbi.build_dir(sdist_root_dir) == sdist_root_dir / "python"


def test_pbi_test_pkg_patches(testdata_context: context.WorkContext) -> None:
    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    norm_test_pkg = TEST_PKG.replace("-", "_")
    unversioned_patchdir = testdata_context.settings.patches_dir / norm_test_pkg
    versioned_patchdir = (
        testdata_context.settings.patches_dir / f"{norm_test_pkg}-1.0.2"
    )

    patch001 = versioned_patchdir / "001-somepatch.patch"
    patch002 = versioned_patchdir / "002-otherpatch.patch"
    patch004 = unversioned_patchdir / "cpu" / "004-cpu.patch"
    patch005 = versioned_patchdir / "cpu" / "005-cpuver.patch"
    patch010 = unversioned_patchdir / "010-unversioned.patch"

    assert pbi.get_all_patches() == {
        None: [patch004, patch010],
        Version("1.0.2"): [patch001, patch002, patch005],
    }
    assert pbi.get_all_patches() is pbi.get_all_patches()

    assert pbi.get_patches(Version("1.0.2")) == [
        patch001,
        patch002,
        patch004,
        patch005,
        patch010,
    ]
    assert pbi.get_patches(Version("1.0.1")) == [
        patch004,
        patch010,
    ]


def test_pbi_other(testdata_context: context.WorkContext) -> None:
    pbi = testdata_context.settings.package_build_info(TEST_OTHER_PKG)
    assert pbi.package == NormalizedName(TEST_OTHER_PKG)
    assert pbi.variant == Variant(testdata_context.settings.variant)
    assert pbi.pre_built is False
    assert pbi.has_config is False
    assert pbi.wheel_server_url is None
    assert pbi.override_module_name == "test_other_pkg"
    assert pbi.download_source_url(Version("1.0.0")) is None
    assert pbi.download_source_destination_filename(Version("1.0.0")) is None
    assert pbi.download_source_destination_filename(Version("1.0.0")) is None
    assert pbi.resolver_include_sdists is True
    assert pbi.resolver_include_wheels is False
    assert (
        pbi.resolver_sdist_server_url("https://pypi.org/simple")
        == "https://pypi.org/simple"
    )
    assert pbi.build_tag(Version("1.0.0")) == ()
    sdist_root_dir = pathlib.Path("/sdist-root")
    assert pbi.build_dir(sdist_root_dir) == sdist_root_dir

    patchdir = (
        testdata_context.settings.patches_dir
        / f"{TEST_OTHER_PKG.replace('-', '_')}-1.0.0"
    )
    assert pbi.get_all_patches() == {
        Version("1.0.0"): [
            patchdir / "001-mypatch.patch",
        ],
    }
    assert pbi.get_all_patches() is pbi.get_all_patches()


def test_type_envvars():
    ta = pydantic.TypeAdapter(EnvVars)
    v = ta.validate_python(
        {"int": 1, "float": 2.0, "true": True, "false": False, "str": "string"}
    )
    assert v == {
        "int": "1",
        "float": "2.0",
        "true": "1",
        "false": "0",
        "str": "string",
    }
    with pytest.raises(ValueError):
        ta.validate_python({"shell": "$(subshell)"})
    with pytest.raises(TypeError):
        ta.validate_python({"none": None})


def test_type_package():
    ta = pydantic.TypeAdapter(Package)
    assert ta.validate_python("Some_Package") == "some-package"
    assert ta.validate_python("some.package") == "some-package"
    with pytest.raises(ValueError):
        ta.validate_python("invalid/package")


def test_type_builddirectory():
    ta = pydantic.TypeAdapter(BuildDirectory)
    assert ta.validate_python("python") == pathlib.Path("python")
    assert ta.validate_python("../tmp/build") == pathlib.Path("../tmp/build")
    with pytest.raises(ValueError):
        ta.validate_python("/absolute/path")


def test_global_settings(testdata_path: pathlib.Path):
    filename = testdata_path / "context/overrides/settings.yaml"
    gs = SettingsFile.from_file(filename)
    assert gs.changelog == {
        "rocm": [
            "setuptools upgraded to 82.0.0",
        ],
    }


def test_settings_overrides(testdata_context: context.WorkContext) -> None:
    assert testdata_context.settings.list_overrides() == {
        TEST_PKG,
        TEST_EMPTY_PKG,
        TEST_OTHER_PKG,
        TEST_RELATED_PKG,
    }


def test_global_changelog(testdata_context: context.WorkContext) -> None:
    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    assert pbi.variant == "cpu"
    assert pbi.build_tag(Version("0.99")) == ()
    assert pbi.build_tag(Version("1.0.1")) == (1, "")
    assert pbi.build_tag(Version("1.0.2")) == (2, "")
    assert pbi.build_tag(Version("2.0.0")) == ()

    testdata_context.settings.variant = Variant("rocm")
    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    assert pbi.variant == "rocm"
    assert pbi.build_tag(Version("0.99")) == (1, "")
    assert pbi.build_tag(Version("1.0.1")) == (2, "")
    assert pbi.build_tag(Version("1.0.2")) == (3, "")
    assert pbi.build_tag(Version("2.0.0")) == (1, "")


def test_settings_list(testdata_context: context.WorkContext) -> None:
    assert testdata_context.settings.list_overrides() == {
        TEST_EMPTY_PKG,
        TEST_OTHER_PKG,
        TEST_PKG,
        TEST_RELATED_PKG,
    }
    assert testdata_context.settings.list_pre_built() == set()
    assert testdata_context.settings.variant_changelog() == []
    testdata_context.settings.variant = Variant("rocm")
    assert testdata_context.settings.list_pre_built() == {TEST_PKG}
    assert testdata_context.settings.variant_changelog() == [
        "setuptools upgraded to 82.0.0"
    ]


@patch("fromager.packagesettings.get_cpu_count", return_value=8)
@patch("fromager.packagesettings.get_available_memory_gib", return_value=7.1)
def test_parallel_jobs(
    get_available_memory_gib: Mock,
    get_cpu_count: Mock,
    testdata_context: context.WorkContext,
) -> None:
    assert testdata_context.settings.max_jobs is None

    pbi = testdata_context.settings.package_build_info(TEST_EMPTY_PKG)
    assert pbi.parallel_jobs() == 7

    get_cpu_count.return_value = 4
    assert pbi.parallel_jobs() == 4

    get_available_memory_gib.return_value = 2.1
    assert pbi.parallel_jobs() == 2

    get_available_memory_gib.return_value = 1.5
    assert pbi.parallel_jobs() == 1

    testdata_context.settings.max_jobs = 2
    pbi = testdata_context.settings.package_build_info(TEST_EMPTY_PKG)
    get_available_memory_gib.return_value = 23
    assert pbi.parallel_jobs() == 2

    # test-pkg needs more memory
    testdata_context.settings.max_jobs = 200
    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    get_cpu_count.return_value = 16
    get_available_memory_gib.return_value = 20
    assert pbi.parallel_jobs() == 4

    get_cpu_count.return_value = 32
    get_available_memory_gib.return_value = 25
    assert pbi.parallel_jobs() == 6

    testdata_context.settings.max_jobs = 4
    pbi = testdata_context.settings.package_build_info(TEST_PKG)
    assert pbi.parallel_jobs() == 4


@pytest.mark.parametrize(
    "value,template_env,expected",
    [
        ("", {}, ""),
        ("${var}", {"var": "value"}, "value"),
        ("$${var}", {"var": "value"}, "${var}"),
        ("${var:-}", {}, ""),
        ("${var:-default}", {}, "default"),
        ("${var:-default}", {"var": "value"}, "value"),
        ("$${var:-default}", {}, "${var:-default}"),
    ],
)
def test_substitute_template(value: str, template_env: dict[str, str], expected: str):
    assert substitute_template(value, template_env) == expected


def test_substitute_template_key_error():
    # This test expects a ValueError to be raised by substitute_template
    with pytest.raises(ValueError) as excinfo:
        substitute_template("${DEFAULT:-default} ${UNKNOWN}", {})
    # Verify that the error message matches the expected message
    assert (
        str(excinfo.value)
        == "Undefined environment variable KeyError('UNKNOWN') referenced in expression '${DEFAULT} ${UNKNOWN}'"
    )
