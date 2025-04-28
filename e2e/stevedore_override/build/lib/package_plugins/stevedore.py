import logging
import pathlib

from packaging.requirements import Requirement
from packaging.version import Version

from fromager import context, sources, build_environment

logger = logging.getLogger(__name__)


def build_sdist(
    ctx: context.WorkContext,
    extra_environ: dict,
    req: Requirement,
    sdist_root_dir: pathlib.Path,
    version: Version,
    build_env: build_environment.BuildEnvironment,
) -> pathlib.Path:
    return sources.pep517_build_sdist(
        ctx=ctx,
        extra_environ=extra_environ,
        req=req,
        sdist_root_dir=sdist_root_dir,
        version=version,
        build_env=build_env,
    )
