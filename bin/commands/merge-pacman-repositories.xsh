#!/usr/bin/env xonsh


if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')

    import pathlib
    def parse_p(p_raw):
        p = pathlib.Path(p_raw).absolute()
        if p.exists():
            return p
        else:
            raise NotADirectoryError(f"directory does not exist: {str(p)}")

    MINICLUSTER.ARGPARSE.add_argument('--source_db_dir', type=parse_p, required=True, help="The directory in which the databases reside, e.g. core.db, default /var/lib/pacman/sync/")
    ## TODO: ensure that name.db exists
    MINICLUSTER.ARGPARSE.add_argument('--db_names', default=[], nargs='+', required=True, help="The database names, ex core, multilib, extra")
    MINICLUSTER.ARGPARSE.add_argument('--source_pkg_cache', type=parse_p, required=True, help="The directory containing the pkg files, default: /var/cache/pacman/pkg/")
    MINICLUSTER.ARGPARSE.add_argument('--dest_db_name', required=True, help="How you want to name the resulting package database")
    MINICLUSTER.ARGPARSE.add_argument('--dest_db_dir', required=True, type=lambda p: pathlib.Path(p).absolute(), help="The location where the new package database should be stored")
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import random
import string
import sys
import logging
import tempfile
import tarfile
import itertools
import re
import shutil
from cluster.functions import ZstdTarFile, pushd

def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"build-tmp-{handle}-{r}"


def get_pkg_info(pkg_path, kv_reg, db_extracted_dir, db_names, logger):
    if pkg_path.suffix == '.zst':
        pkgf = ZstdTarFile(pkg_path)
    elif pkg_path.suffix == '.xz':
        pkgf = tarfile.open(pkg_path)
    else:
        raise Exception(f"Unhandled compression for '{pkg_path}'")
    pkginfof = pkgf.extractfile('.PKGINFO')
    pkginfo = {}
    for line in pkginfof.readlines():
        line = line.decode('utf-8').rstrip()
        groups = kv_reg.match(line)
        if not groups:
            continue
        groups = {k: v.lstrip() for k,v in groups.groupdict().items()}
        k = groups['var']
        v = groups['val']
        pkginfo[k] = v
        #logger.info(f"{groups=}")
        #TODO: a var can occur multiple times in the same .PKGINFO file, current bug is that only the last value is kept per variable as string
    expected_d_name=f"{pkginfo['pkgname']}-{pkginfo['pkgver']}"
    found = False
    exp_path = None
    db_name = None
    files = ['%FILES%']
    for d in db_names:
        exp_path = db_extracted_dir / d / expected_d_name
        if exp_path.exists():
            logger.debug(f"{expected_d_name} in {d}")
            found = True
            db_name = d
            break
    for f in pkgf:
        if f.name.startswith('.'):
            logger.warn(f"TODO: parse {f} and put info into sqlite")
            continue
        suffix = ''
        if f.isdir():
            suffix = '/'
        f = f.name + suffix
        files.append(f)
    files.sort()
    return (pkginfo, db_name, files)

def command_merge_pacman_repositories_xsh(logger, source_db_dir, db_names, source_pkg_cache, dest_db_name, dest_db_dir):
    db_extracted_dir = pathlib.Path(tempfile.mkdtemp(prefix="minicluster-merge-pacman-repositories-"))
    logger.info(f"{db_extracted_dir=}")
    for d in db_names:
        d_file = source_db_dir / f"{d}.db"
        with tarfile.open(d_file, mode='r:*') as d_archive:
            d_archive_dir = db_extracted_dir / d
            d_archive_dir.mkdir()
            d_archive.extractall(d_archive_dir)
    #go through each pkg file in source_pkg_cache
    kv_reg = re.compile('^(?P<var>[a-zA-Z_]+).*=(?P<val>.+)')
    temp_db_uncompressed_dir = pathlib.Path(f"{dest_db_dir}/{dest_db_name}").absolute()
    if dest_db_dir.exists():
        shutil.rmtree(dest_db_dir)
    temp_db_uncompressed_dir.mkdir(parents=True)
    logger.info(f"{temp_db_uncompressed_dir=}")
    logger.info(f"creating database {dest_db_name} in {dest_db_dir}")
    to_remove = []
    all_files = []
    for pkg_path in itertools.chain(source_pkg_cache.glob('*.pkg.tar.zst'), source_pkg_cache.glob('*.pkg.tar.xz')):
        # TODO: collect in a sqlite db all other information from the package
        (pkginfo, db_name, files) = get_pkg_info(pkg_path, kv_reg, db_extracted_dir, db_names, logger)
        expected_d_name=f"{pkginfo['pkgname']}-{pkginfo['pkgver']}"
        exp_path = db_extracted_dir / db_name / expected_d_name
        assert db_name, f"Could not find {expected_d_name} in {db_extracted_dir}/*/"
        #logger.info(f"copy {exp_path} to {temp_db_uncompressed_dir}")
        shutil.copytree(exp_path, temp_db_uncompressed_dir / exp_path.name)
        shutil.copy(pkg_path, dest_db_dir)
        shutil.copy(f"{pkg_path}.sig", dest_db_dir)
        files_f = temp_db_uncompressed_dir / exp_path.name / "files"
        #logger.info(f"{files_f=}")
        to_remove.append(files_f)
        with open(files_f, "w") as files_fp:
            for line in files:
                files_fp.write(line + "\n")
        all_files.extend(files[1:])

    # now create database files
    cwd = pathlib.Path(f"{dest_db_dir}/{dest_db_name}")
    root_dir = "."
    base_dir = "."
    with pushd(cwd):
        base_name = pathlib.Path(f"{dest_db_dir}/{dest_db_name}.files")
        files_db = shutil.make_archive(base_name=base_name, format='tar', root_dir=root_dir, base_dir=base_dir, verbose=True, dry_run=False, logger=logger)
        logger.info(f"{files_db=}")
        base_name.symlink_to(f"{dest_db_name}.files.tar")
        for to_rem in to_remove:
            pathlib.Path(to_rem).unlink()
        base_name = pathlib.Path(f"{dest_db_dir}/{dest_db_name}.db")
        db = shutil.make_archive(base_name=base_name, format='tar', root_dir=root_dir, base_dir=base_dir, verbose=True, dry_run=False, logger=logger)
        logger.info(f"{db=}")
        base_name.symlink_to(f"{dest_db_name}.db.tar")
        all_files.sort()
        # TODO: write to a sqlite db instead
        with open(f"../{dest_db_name}.files.lst", "w") as fp:
            for line in all_files:
                fp.write(line + "\n")

    shutil.rmtree(cwd)
    shutil.rmtree(db_extracted_dir)
    cwd = pathlib.Path(f"{dest_db_dir}")

if __name__ == '__main__':
    source_db_dir = MINICLUSTER.ARGS.source_db_dir
    db_names = MINICLUSTER.ARGS.db_names
    source_pkg_cache = MINICLUSTER.ARGS.source_pkg_cache
    dest_db_name = MINICLUSTER.ARGS.dest_db_name
    dest_db_dir = MINICLUSTER.ARGS.dest_db_dir

    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    $RAISE_SUBPROC_ERROR = True
    command_merge_pacman_repositories_xsh(logger, source_db_dir, db_names, source_pkg_cache, dest_db_name, dest_db_dir)
