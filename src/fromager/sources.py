from __future__ import annotations

import inspect
import json
import logging
import os.path
import pathlib
import shutil
import tarfile
import typing
import zipfile
from urllib.parse import urlparse

import resolvelib
from packaging.requirements import Requirement
from packaging.version import Version
from requests.exceptions import ChunkedEncodingError, ConnectionError
from urllib3.exceptions import IncompleteRead, ProtocolError

from . import (
    build_environment,
    dependencies,
    external_commands,
    metrics,
    overrides,
    pyproject,
    requirements_file,
    resolver,
    tarballs,
    vendor_rust,
)
from .http_retry import RETRYABLE_EXCEPTIONS, retry_on_exception
from .request_session import session
from .requirements_file import RequirementType

if typing.TYPE_CHECKING:
    from . import build_environment, context

logger = logging.getLogger(__name__)


def get_source_type(ctx: context.WorkContext, req: Requirement) -> str:
    source_type = requirements_file.SourceType.SDIST
    pbi = ctx.package_build_info(req)
    if (
        overrides.find_override_method(req.name, "download_source")
        or overrides.find_override_method(req.name, "resolve_source")
        or overrides.find_override_method(req.name, "get_resolver_provider")
        or pbi.download_source_url(resolve_template=False)
    ):
        source_type = requirements_file.SourceType.OVERRIDE
    return str(source_type)


@metrics.timeit(description="download source")
def download_source(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    version: Version,
    download_url: str,
) -> pathlib.Path:
    if req.url:
        logger.info(
            "downloaded source to %s by cloning %s, ignoring any plugins",
            download_url,
            req.url,
        )
        return pathlib.Path(download_url)

    source_path = overrides.find_and_invoke(
        req.name,
        "download_source",
        default_download_source,
        ctx=ctx,
        req=req,
        version=version,
        download_url=download_url,
        sdists_downloads_dir=ctx.sdists_downloads,
    )

    if not isinstance(source_path, pathlib.Path):
        raise ValueError(
            f"expected a Path back to downloaded source. got {source_path}"
        )
    return source_path


@metrics.timeit(description="resolve source")
def resolve_source(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    sdist_server_url: str,
    req_type: RequirementType | None = None,
) -> tuple[str, Version]:
    "Return URL to source and its version."

    constraint = ctx.constraints.get_constraint(req.name)
    logger.debug(
        f"resolving requirement {req} using {sdist_server_url} with constraint {constraint}"
    )

    try:
        resolver_results = overrides.find_and_invoke(
            req.name,
            "resolve_source",
            default_resolve_source,
            ctx=ctx,
            req=req,
            sdist_server_url=sdist_server_url,
            req_type=req_type,
        )
    except (
        resolvelib.InconsistentCandidate,
        resolvelib.RequirementsConflicted,
        resolvelib.ResolutionImpossible,
    ) as err:
        logger.debug(f"could not resolve {req} with {constraint}: {err}")
        raise

    if len(resolver_results) == 2:
        url, version = resolver_results
    else:
        raise ValueError(
            f"do not know how to unpack {resolver_results}, expected 2 members"
        )

    if not isinstance(version, Version):
        raise ValueError(f"expected 2nd member to be of type Version, got {version}")

    return str(url), version


def default_resolve_source(
    ctx: context.WorkContext,
    req: Requirement,
    sdist_server_url: str,
    req_type: RequirementType | None = None,
) -> tuple[str, Version]:
    "Return URL to source and its version."

    pbi = ctx.package_build_info(req)
    override_sdist_server_url = pbi.resolver_sdist_server_url(sdist_server_url)

    url, version = resolver.resolve(
        ctx=ctx,
        req=req,
        sdist_server_url=override_sdist_server_url,
        include_sdists=pbi.resolver_include_sdists,
        include_wheels=pbi.resolver_include_wheels,
        req_type=req_type,
    )
    return url, version


def default_download_source(
    ctx: context.WorkContext,
    req: Requirement,
    version: Version,
    download_url: str,
    sdists_downloads_dir: pathlib.Path,
) -> pathlib.Path:
    "Download the requirement and return the name of the output path."
    pbi = ctx.package_build_info(req)
    destination_filename = pbi.download_source_destination_filename(version=version)
    url = pbi.download_source_url(version=version, default=download_url)
    source_filename = _download_source_check(
        req=req,
        destination_dir=sdists_downloads_dir,
        url=url,
        destination_filename=destination_filename,
    )

    logger.debug(f"have source for {req} version {version} in {source_filename}")
    return source_filename


# Helper method to check whether .zip /.tar / .tgz is able to extract and check its content.
# It will throw exception if any other file is encountered. Eg: index.html
def _download_source_check(
    req: Requirement,
    destination_dir: pathlib.Path,
    url: str,
    destination_filename: str | None = None,
) -> str:
    source_filename = download_url(
        req=req,
        destination_dir=destination_dir,
        url=url,
        destination_filename=destination_filename,
    )
    if source_filename.suffix == ".zip":
        with zipfile.ZipFile(source_filename) as zip_file:
            source_file_contents = zip_file.namelist()
            if not source_file_contents:
                raise zipfile.BadZipFile(
                    f"Empty zip file encountered: {source_filename}"
                )
    elif (
        source_filename.suffix == ".tgz"
        or source_filename.suffix == ".gz"
        or str(source_filename).endswith(".tar.gz")
    ):
        with tarfile.open(source_filename) as tar:
            if not tar.next():
                raise tarfile.TarError(f"Empty tar file encountered: {source_filename}")
    else:
        raise ValueError(
            f"The source file encountered is not a zip or tar file: {source_filename}"
        )
    return source_filename


def download_url(
    *,
    req: Requirement,
    destination_dir: pathlib.Path,
    url: str,
    destination_filename: str | None = None,
) -> pathlib.Path:
    basename = (
        destination_filename
        if destination_filename
        else os.path.basename(urlparse(url).path)
    )
    outfile = pathlib.Path(destination_dir) / basename
    logger.debug(
        "looking for %s %s",
        outfile,
        "(exists)" if outfile.exists() else "(not there)",
    )
    if outfile.exists():
        logger.debug("already have %s", outfile)
        return outfile

    # Create a temporary file to avoid partial downloads
    temp_file = outfile.with_suffix(outfile.suffix + ".tmp")

    def _download_with_retry():
        """Internal function that performs the actual download with retry logic."""
        logger.debug(f"reading from {url}")
        try:
            with session.get(url, stream=True) as r:
                r.raise_for_status()
                with open(temp_file, "wb") as f:
                    logger.debug("writing to %s", temp_file)
                    # Use smaller chunk size for better error recovery
                    for chunk in r.iter_content(chunk_size=64 * 1024):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)

            # Only move to final location if download completed successfully
            temp_file.rename(outfile)
            logger.info("saved %s", outfile)
            return outfile

        except (
            ChunkedEncodingError,
            IncompleteRead,
            ProtocolError,
            ConnectionError,
        ) as e:
            # Clean up partial file on failure
            if temp_file.exists():
                temp_file.unlink()
            logger.warning(f"Download failed for {url}: {e}")
            raise
        except Exception:
            # Clean up partial file on any other failure
            if temp_file.exists():
                temp_file.unlink()
            raise

    # Apply retry logic specifically for download operations
    @retry_on_exception(
        exceptions=RETRYABLE_EXCEPTIONS,
        max_attempts=5,
        backoff_factor=1.5,
        max_backoff=120.0,
    )
    def download_with_retry():
        return _download_with_retry()

    try:
        return download_with_retry()
    except Exception:
        # Ensure temp file is cleaned up if it still exists
        if temp_file.exists():
            temp_file.unlink()
        raise


def _takes_arg(f: typing.Callable, arg_name: str) -> bool:
    sig = inspect.signature(f)
    return arg_name in sig.parameters


def unpack_source(
    ctx: context.WorkContext,
    req: Requirement,
    version: Version,
    source_filename: pathlib.Path,
) -> tuple[pathlib.Path, bool]:
    # sdist names are less standardized and the names of the directories they
    # contain are also not very standard. Force the names into a predictable
    # form based on the override module name for the requirement.
    req_name = overrides.pkgname_to_override_module(req.name)
    expected_name = f"{req_name}-{version}"

    # The unpack_dir is a parent dir where we put temporary outputs during the
    # build process, including the unpacked source in a subdirectory.
    unpack_dir = ctx.work_dir / expected_name
    if unpack_dir.exists():
        if ctx.cleanup:
            logger.debug("cleaning up %s", unpack_dir)
            shutil.rmtree(unpack_dir)
        else:
            logger.info("reusing %s", unpack_dir)
            return (unpack_dir / unpack_dir.name, False)

    # sdists might be tarballs or zip files.
    logger.debug("unpacking %s to %s", source_filename, unpack_dir)
    if str(source_filename).endswith(".tar.gz"):
        with tarfile.open(source_filename, "r") as t:
            if _takes_arg(t.extractall, "filter"):
                t.extractall(unpack_dir, filter="data")
            else:
                logger.debug('unpacking without filter="data"')
                t.extractall(unpack_dir)
    elif str(source_filename).endswith(".zip"):
        with zipfile.ZipFile(source_filename) as zf:
            zf.extractall(path=unpack_dir)
    else:
        raise ValueError(f"Do not know how to unpack source archive {source_filename}")

    # We create a unique directory based on the requirement name, but that may
    # not be the same name as the root directory of the content in the sdist
    # (due to case, punctuation, etc.), so after we unpack it look for what was
    # created and ensure the extracted directory matches the override module
    # name and version of the requirement.
    unpacked_root_dir = next(iter(unpack_dir.glob("*")))
    if unpacked_root_dir.name != expected_name:
        desired_name = unpacked_root_dir.parent / expected_name
        try:
            shutil.move(
                str(unpacked_root_dir),
                str(desired_name),
            )
        except Exception as err:
            raise Exception(
                f"Could not rename {unpacked_root_dir.name} to {desired_name}: {err}"
            ) from err
        unpacked_root_dir = desired_name

    return (unpacked_root_dir, True)


def patch_source(
    ctx: context.WorkContext,
    source_root_dir: pathlib.Path,
    req: Requirement,
    version: Version,
) -> None:
    pbi = ctx.package_build_info(req)
    patch_count = 0

    for p in pbi.get_patches(version):
        _apply_patch(req, p, source_root_dir)
        patch_count += 1

    logger.debug("applied %d patches", patch_count)
    # If no patch has been applied, call warn for old patch
    patchmap = pbi.get_all_patches()
    if not patch_count and patchmap:
        for patchversion in sorted(patchmap):
            logger.warning(
                f"patch {patchversion} exists but will not be applied for version {version}"
            )


def _apply_patch(req: Requirement, patch: pathlib.Path, source_root_dir: pathlib.Path):
    logger.info("applying patch file %s to %s", patch, source_root_dir)
    with open(patch, "r") as f:
        external_commands.run(["patch", "-p1"], stdin=f, cwd=source_root_dir)


def write_build_meta(
    unpack_dir: pathlib.Path,
    req: Requirement,
    source_filename: pathlib.Path,
    version: Version,
) -> pathlib.Path:
    meta_file = unpack_dir / "build-meta.json"
    with open(meta_file, "w") as f:
        json.dump(
            {
                "req": str(req),
                "source-filename": str(source_filename),
                "version": str(version),
            },
            f,
        )
    logger.debug("wrote build metadata to %s", meta_file)
    return meta_file


def read_build_meta(unpack_dir: pathlib.Path) -> dict:
    meta_file = unpack_dir / "build-meta.json"
    with open(meta_file, "r") as f:
        return json.load(f)


@metrics.timeit(description="prepare source")
def prepare_source(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    source_filename: pathlib.Path,
    version: Version,
) -> pathlib.Path:
    if req.url:
        logger.info(
            "preparing source cloned from %s into %s, ignoring any plugins",
            req.url,
            source_filename,
        )
        source_root_dir = pathlib.Path(source_filename)
        prepare_new_source(
            ctx=ctx,
            req=req,
            source_root_dir=source_root_dir,
            version=version,
        )
    else:
        logger.info(f"preparing source for {req} from {source_filename}")
        prepare_source_details = overrides.find_and_invoke(
            req.name,
            "prepare_source",
            default_prepare_source,
            ctx=ctx,
            req=req,
            source_filename=source_filename,
            version=version,
        )
        if not isinstance(prepare_source_details, tuple):
            source_root_dir = prepare_source_details
        elif len(prepare_source_details) == 2:
            source_root_dir, _ = prepare_source_details
        else:
            raise ValueError(
                f"do not know how to unpack {prepare_source_details}, expected 1 or 2 members"
            )
    write_build_meta(source_root_dir.parent, req, source_filename, version)
    if source_root_dir is not None:
        logger.info(f"prepared source for {req} at {source_root_dir}")
    return source_root_dir


def default_prepare_source(
    ctx: context.WorkContext,
    req: Requirement,
    source_filename: pathlib.Path,
    version: Version,
) -> tuple[pathlib.Path, bool]:
    """Unpack and modify sdist sources

    Calls :func:`~fromager.sources.prepare_new_source` by default.
    """
    source_root_dir, is_new = unpack_source(
        ctx=ctx,
        req=req,
        version=version,
        source_filename=source_filename,
    )
    if is_new:
        prepare_new_source(
            ctx=ctx,
            req=req,
            source_root_dir=source_root_dir,
            version=version,
        )
    return source_root_dir, is_new


def prepare_new_source(
    ctx: context.WorkContext,
    req: Requirement,
    source_root_dir: pathlib.Path,
    version: Version,
) -> None:
    """Default steps for new sources

    - patch sources
    - apply project overrides from settings
    - vendor Rust dependencies

    :func:`~default_prepare_source` runs this function when the sources are new.
    """
    patch_source(ctx, source_root_dir, req, version)
    pyproject.apply_project_override(
        ctx=ctx,
        req=req,
        sdist_root_dir=source_root_dir,
    )
    vendor_rust.vendor_rust(req, source_root_dir)


@metrics.timeit(description="build sdist")
def build_sdist(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    version: Version,
    sdist_root_dir: pathlib.Path,
    build_env: build_environment.BuildEnvironment,
) -> pathlib.Path:
    """Build source distribution"""
    pbi = ctx.package_build_info(req)
    build_dir = pbi.build_dir(sdist_root_dir)

    logger.info(f"building {ctx.variant} source distribution for {req} in {build_dir}")
    extra_environ = pbi.get_extra_environ(build_env=build_env)
    if req.url:
        # The default approach to making an sdist is to make a tarball from the
        # source directory, since most of the time we got the source directory
        # by unpacking an existing sdist. When we know we cloned a git repo to
        # get the source tree, we can be very sure that creating a tarball will
        # NOT produce a valid sdist, so we can use the PEP-517 approach
        # instead.
        logger.info("using PEP-517 sdist build, ignoring any plugins")
        sdist_filename = pep517_build_sdist(
            ctx=ctx,
            extra_environ=extra_environ,
            req=req,
            sdist_root_dir=sdist_root_dir,
            version=version,
            build_env=build_env,
        )
    else:
        sdist_filename = overrides.find_and_invoke(
            req.name,
            "build_sdist",
            default_build_sdist,
            ctx=ctx,
            extra_environ=extra_environ,
            req=req,
            version=version,
            sdist_root_dir=sdist_root_dir,
            build_dir=build_dir,
            build_env=build_env,
        )
    logger.info(f"built source distribution {sdist_filename}")
    return sdist_filename


def default_build_sdist(
    ctx: context.WorkContext,
    extra_environ: dict,
    req: Requirement,
    version: Version,
    sdist_root_dir: pathlib.Path,
    build_env: build_environment.BuildEnvironment,
    build_dir: pathlib.Path,
) -> pathlib.Path:
    # It seems like the "correct" way to do this would be to run the
    # PEP 517 API in the source tree we have modified. However, quite
    # a few packages assume their source distribution is being built
    # from a source code repository checkout and those throw an error
    # when we use the interface to try to rebuild the sdist. Since we
    # know what we have is an exploded tarball, we just tar it back
    # up.
    #
    # For cases where the PEP 517 approach works, use
    # pep517_build_sdist().
    sdist_filename = ctx.sdists_builds / f"{req.name}-{version}.tar.gz"
    if sdist_filename.exists():
        sdist_filename.unlink()
    ensure_pkg_info(
        ctx=ctx,
        req=req,
        version=version,
        sdist_root_dir=sdist_root_dir,
        build_dir=build_dir,
    )
    # The format argument is specified based on
    # https://peps.python.org/pep-0517/#build-sdist.
    with tarfile.open(sdist_filename, "x:gz", format=tarfile.PAX_FORMAT) as sdist:
        tarballs.tar_reproducible(
            tar=sdist,
            basedir=build_dir,
            prefix=build_dir.parent,
        )
    return sdist_filename


def pep517_build_sdist(
    ctx: context.WorkContext,
    extra_environ: dict,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
    version: Version,
    build_env: build_environment.BuildEnvironment,
) -> pathlib.Path:
    """Use the PEP 517 API to build a source distribution from a modified source tree."""
    pbi = ctx.package_build_info(req)
    build_dir = pbi.build_dir(sdist_root_dir)
    hook_caller = dependencies.get_build_backend_hook_caller(
        ctx=ctx,
        req=req,
        build_dir=build_dir,
        override_environ=extra_environ,
        build_env=build_env,
    )
    sdist_filename = hook_caller.build_sdist(
        ctx.sdists_builds,
        config_settings=pbi.config_settings,
    )
    return ctx.sdists_builds / sdist_filename


PKG_INFO_CONTENT = """\
Metadata-Version: 1.0
Name: {name}
Version: {version}
Summary: Fromage stub PKG-INFO
"""


def ensure_pkg_info(
    *,
    ctx: context.WorkContext,
    req: Requirement,
    version: Version,
    sdist_root_dir: pathlib.Path,
    build_dir: pathlib.Path | None = None,
) -> bool:
    """Ensure that sdist has a PKG-INFO file

    Returns True if PKG-INFO was presence, False if file was missing. The
    function also updates build_dir if package has a non-standard build
    directory. Every sdist must have a PKG-INFO file in the first directory.
    The additional PKG-INFO file in build_dir is required for projects
    with non-standard layout and setuptools-scm.
    """
    had_pkg_info = True
    directories = [sdist_root_dir]
    if build_dir is not None and build_dir != sdist_root_dir:
        directories.append(build_dir)
    for directory in directories:
        pkg_info_file = directory / "PKG-INFO"
        if not pkg_info_file.is_file():
            logger.warning(
                f"PKG-INFO file is missing from {directory}, creating stub file"
            )
            pkg_info_file.write_text(
                PKG_INFO_CONTENT.format(
                    name=req.name,
                    version=str(version),
                )
            )
            had_pkg_info = False
    return had_pkg_info
