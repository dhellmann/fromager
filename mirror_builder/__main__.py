#!/usr/bin/env python3

import argparse
import functools
import logging
import os
import pathlib
import re
import sys

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from . import context, jobs, pkgs, sdist, server, sources, wheels

logger = logging.getLogger(__name__)

TERSE_LOG_FMT = '%(message)s'
VERBOSE_LOG_FMT = '%(levelname)s:%(name)s:%(lineno)d: %(message)s'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', default=False)
    parser.add_argument('--log-file', default='')
    parser.add_argument('-o', '--sdists-repo', default='sdists-repo')
    parser.add_argument('-w', '--wheels-repo', default='wheels-repo')
    parser.add_argument('-t', '--work-dir', default=os.environ.get('WORKDIR', 'work-dir'))
    parser.add_argument('--wheel-server-url')
    parser.add_argument('--no-cleanup', dest='cleanup', default=True, action='store_false')

    subparsers = parser.add_subparsers(title='commands', dest='command')

    parser_bootstrap = subparsers.add_parser('bootstrap')
    parser_bootstrap.set_defaults(func=do_bootstrap)
    parser_bootstrap.add_argument('toplevel', nargs='+')

    parser_download = subparsers.add_parser('download-source-archive')
    parser_download.set_defaults(func=do_download_source_archive)
    parser_download.add_argument('dist_name')
    parser_download.add_argument('dist_version')
    parser_download.add_argument('sdist_server_url')

    parser_prepare_source = subparsers.add_parser('prepare-source')
    parser_prepare_source.set_defaults(func=do_prepare_source)
    parser_prepare_source.add_argument('dist_name')
    parser_prepare_source.add_argument('dist_version')

    parser_prepare_build = subparsers.add_parser('prepare-build')
    parser_prepare_build.set_defaults(func=do_prepare_build)
    parser_prepare_build.add_argument('dist_name')
    parser_prepare_build.add_argument('dist_version')

    parser_build = subparsers.add_parser('build')
    parser_build.set_defaults(func=do_build)
    parser_build.add_argument('dist_name')
    parser_build.add_argument('dist_version')

    # The jobs CLI is complex enough that it's in its own module
    jobs.build_cli(parser, subparsers)

    args = parser.parse_args(sys.argv[1:])

    # Configure console and log output.
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    stream_formatter = logging.Formatter(VERBOSE_LOG_FMT if args.verbose else TERSE_LOG_FMT)
    stream_handler.setFormatter(stream_formatter)
    logging.getLogger().addHandler(stream_handler)
    if args.log_file:
        # Always log to the file at debug level
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(VERBOSE_LOG_FMT)
        file_handler.setFormatter(file_formatter)
        logging.getLogger().addHandler(file_handler)
    # We need to set the overall logger level to debug and allow the
    # handlers to filter messages at their own level.
    logging.getLogger().setLevel(logging.DEBUG)

    try:
        args.func(args)
    except Exception as err:
        logger.exception(err)
        raise


def requires_context(f):
    "Decorate f() to add WorkContext argument before calling it."
    @functools.wraps(f)
    def provides_context(args):
        ctx = context.WorkContext(
            sdists_repo=args.sdists_repo,
            wheels_repo=args.wheels_repo,
            work_dir=args.work_dir,
            wheel_server_url=args.wheel_server_url,
            cleanup=args.cleanup,
        )
        ctx.setup()
        return f(args, ctx)
    return provides_context


@requires_context
def do_bootstrap(args, ctx):
    server.start_wheel_server(ctx)
    for toplevel in args.toplevel:
        sdist.handle_requirement(ctx, Requirement(toplevel))


@requires_context
def do_download_source_archive(args, ctx):
    req = Requirement(f'{args.dist_name}=={args.dist_version}')
    logger.info('downloading source archive for %s from %s', req, args.sdist_server_url)
    filename, _ = sources.download_source(ctx, req, args.sdist_server_url)
    print(filename)


@requires_context
def do_prepare_source(args, ctx):
    req = Requirement(f'{args.dist_name}=={args.dist_version}')
    logger.info('preparing source directory for %s', req)
    source_filename = _find_sdist(pathlib.Path(args.sdists_repo), req, args.dist_version)
    # FIXME: Does the version need to be a Version instead of str?
    source_root_dir = sources.prepare_source(ctx, req, source_filename, args.dist_version)
    print(source_root_dir)


def _dist_name_to_filename(dist_name):
    """Transform the dist name into a prefix for a filename.

    Following https://peps.python.org/pep-0427/
    """
    canonical_name = canonicalize_name(dist_name)
    return re.sub(r"[^\w\d.]+", "_", canonical_name, re.UNICODE)


def _find_sdist(sdists_repo, req, dist_version):
    downloads_dir = sdists_repo / 'downloads'
    sdist_name_func = pkgs.find_override_method(req.name, 'expected_source_archive_name')

    if sdist_name_func:
        # The file must exist exactly as given.
        sdist_file = downloads_dir / sdist_name_func(req, dist_version)
        if sdist_file.exists():
            return sdist_file
        candidates = [sdist_file]

    else:
        filename_prefix = _dist_name_to_filename(req.name)
        canonical_name = canonicalize_name(req.name)

        candidate_bases = [
            # First check if the file is there using the canonically
            # transformed name.
            f'{filename_prefix}-{dist_version}.tar.gz',
            # If that didn't work, try the canonical dist name. That's not
            # "correct" but we do see it. (charset-normalizer-3.3.2.tar.gz
            # and setuptools-scm-8.0.4.tar.gz) for example
            f'{canonical_name}-{dist_version}.tar.gz',
            # If *that* didn't work, try the dist name we've been
            # given as a dependency. That's not "correct", either but we do
            # see it. (oslo.messaging-14.7.0.tar.gz) for example
            f'{req.name}-{dist_version}.tar.gz',
        ]
        # Case-insensitive globbing was added to Python 3.12, but we
        # have to run with older versions, too, so do our own name
        # comparison.
        for filename in downloads_dir.glob('*'):
            for base in candidate_bases:
                if str(filename.name).lower() == base.lower():
                    return filename
        candidates = [downloads_dir / c for c in candidate_bases]

    dir_contents = [str(e) for e in downloads_dir.glob('*.tar.gz')]
    raise RuntimeError(
        f'Cannot find sdist for {req.name} version {dist_version} as any of {candidates} in {dir_contents}'
    )


def _find_source_dir(work_dir, req, dist_version):
    sdist_name_func = pkgs.find_override_method(req.name, 'expected_source_archive_name')

    if sdist_name_func:
        # The directory must exist exactly as given.
        sdist_base_name = sdist_name_func(req, dist_version)[:-len('.tar.gz')]
        source_dir = work_dir / sdist_base_name / sdist_base_name
        if source_dir.exists():
            return source_dir
        candidates = [source_dir]

    else:
        filename_prefix = _dist_name_to_filename(req.name)
        filename_based = f'{filename_prefix}-{dist_version}'
        canonical_name = canonicalize_name(req.name)
        canonical_based = f'{canonical_name}-{dist_version}'
        name_based = f'{req.name}-{dist_version}'

        candidate_bases = [
            # First check if the file is there using the canonically
            # transformed name.
            filename_based,
            # If that didn't work, try the canonical dist name. That's not
            # "correct" but we do see it. (charset-normalizer-3.3.2.tar.gz
            # and setuptools-scm-8.0.4.tar.gz) for example
            canonical_based,
            # If *that* didn't work, try the dist name we've been
            # given as a dependency. That's not "correct", either but we do
            # see it. (oslo.messaging-14.7.0.tar.gz) for example
            name_based,
        ]

        for dirname in work_dir.glob('*'):
            # Case-insensitive globbing was added to Python 3.12, but we
            # have to run with older versions, too, so do our own name
            # comparison.
            for base in candidate_bases:
                if str(dirname.name).lower() == base.lower():
                    # We expect the unpack directory and the source
                    # root directory to be the same. We don't know
                    # what case they have, but the pattern matched, so
                    # use the base name of the unpack directory to
                    # extend the path 1 level.
                    return dirname / dirname.name
        candidates = [
            work_dir / base / base
            for base in candidate_bases
        ]

    work_dir_contents = list(str(e) for e in work_dir.glob('*'))
    raise RuntimeError(
        f'Cannot find source directory for {req.name} version {dist_version} using any of {candidates} in {work_dir_contents}'
    )


@requires_context
def do_prepare_build(args, ctx):
    server.start_wheel_server(ctx)
    req = Requirement(f'{args.dist_name}=={args.dist_version}')
    source_root_dir = _find_source_dir(pathlib.Path(args.work_dir), req, args.dist_version)
    logger.info('preparing build environment for %s', req)
    sdist.prepare_build_environment(ctx, req, source_root_dir)


@requires_context
def do_build(args, ctx):
    req = Requirement(f'{args.dist_name}=={args.dist_version}')
    logger.info('building for %s', req)
    source_root_dir = _find_source_dir(pathlib.Path(args.work_dir), req, args.dist_version)
    build_env = wheels.BuildEnvironment(ctx, source_root_dir.parent, None)
    wheel_filename = wheels.build_wheel(ctx, req, source_root_dir, build_env)
    print(wheel_filename)


if __name__ == '__main__':
    main()
