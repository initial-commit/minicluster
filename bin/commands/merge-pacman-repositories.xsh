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

    from cluster.functions import str2bool_exc as strtobool

    MINICLUSTER.ARGPARSE.add_argument('--source_db_dir', type=parse_p, required=True, help="The directory in which the databases reside, e.g. core.db, default /var/lib/pacman/sync/")
    ## TODO: ensure that name.db exists
    MINICLUSTER.ARGPARSE.add_argument('--db_names', default=[], nargs='+', required=True, help="The database names, ex core, multilib, extra")
    MINICLUSTER.ARGPARSE.add_argument('--source_pkg_cache', type=parse_p, required=True, help="The directory containing the pkg files, default: /var/cache/pacman/pkg/")
    MINICLUSTER.ARGPARSE.add_argument('--dest_db_name', required=True, help="How you want to name the resulting package database")
    MINICLUSTER.ARGPARSE.add_argument('--dest_db_dir', required=True, type=lambda p: pathlib.Path(p).absolute(), help="The location where the new package database should be stored")
    MINICLUSTER.ARGPARSE.add_argument('--only_explicit', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=False, metavar='true|false')
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
import sqlite3
import contextlib
from cluster.functions import ZstdTarFile, pushd, make_archive
import time

def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"merge-tmp-{handle}-{r}"

def collect_list(k, v):
    if not hasattr(collect_list, "lst"):
        collect_list.lst = set()
    if isinstance(v, list) and k not in collect_list.lst:
        collect_list.lst.add(k)
    return v

LIST_FIELDS = ['license', 'backup', 'replaces', 'conflict', 'depend', 'optdepend', 'makedepend', 'checkdepend', 'provides', ]
DICT_FIELDS = []

def ensure_fields(pkginfo):
    normalized = {}
    for f in LIST_FIELDS:
        if f not in pkginfo:
            normalized[f] = []
        else:
            if not isinstance(pkginfo[f], list):
                raise Exception(f"field {f} should be a list, it is instead {pkginfo[f]=}")
    for f in DICT_FIELDS:
        if f not in pkginfo:
            normalized[f] = {}
        else:
            if not isinstance(pkginfo[f], dict):
                raise Exception(f"field {f} should be a list, it is instead {pkginfo[f]=}")
    return pkginfo | normalized

def depend_parse(raw_vals):
    regex = (
            "^(?P<otherpkg>[^=<>: ]+)"
            "((?P<operator>[=<>]+)(?P<version>[^:]+?))?"
            "(: (?P<reason>.+))?$"
        )
    e=re.compile(regex)
    results = []
    for raw_v in raw_vals:
        m = e.match(raw_v)
        assert m is not None, f"Could not parse depends-type value {raw_v=}"
        results.append(m.groupdict())
    return results

def contact_parse(raw_val):
    regex = r"^(?P<realname>[^<]+)<(?P<email>[^@]+@.+)>"
    e = re.compile(regex)
    m = e.match(raw_val)
    assert m is not None, f"Could not parse contact for {raw_val=}"
    data = m.groupdict()
    data['realname'] = data['realname'].strip()
    data['email'] = data['email'].strip()
    return data

def normalize_field(fname, fval):
    cbs = {
        'pkgname': None, # pkginfo
        'pkgbase': None, # pkginfo
        'pkgver': None, # pkginfo
        'pkgdesc': None, # pkginfo
        'url': None, # pkginfo
        'builddate': lambda v: int(v), # pkginfo
        'packager': contact_parse, # Extract name and email
        'size': lambda v: int(v), # pkginfo
        'arch': None, # pkginfo
        'license': None, # pkginfo
        'provides': depend_parse, # DEP
        'depend': depend_parse, # DEP
        'optdepend': depend_parse, # DEP
        'makedepend': depend_parse, # DEP
        'checkdepend': depend_parse, # DEP
        'conflict': depend_parse, # DEP
        'backup': None, # backs_up
        'replaces': depend_parse, # DEP
        'group': None, # pkginfo
    }
    if fname not in cbs:
        raise Exception(f"field not handled: {fname=} with {fval=}")

    if fname in LIST_FIELDS and not isinstance(fval, list):
        fval = [fval]

    if cbs[fname] is None:
        return fval

    return cbs[fname](fval)

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
        if k in LIST_FIELDS or k in DICT_FIELDS:
            if k in LIST_FIELDS:
                if k not in pkginfo:
                    pkginfo[k] = []
                pkginfo[k].append(v)
            elif k in DICT_FIELDS:
                if k not in pkginfo:
                    pkginfo[k] = {}
                pkginfo[k][v] = None
        else:
            if k in pkginfo:
                raise Exception(f"field {k} already exists and is not a DICT_FIELD or LIST_FIELD")
            pkginfo[k] = v
    normalized_info = {}
    expected_d_name=f"{pkginfo['pkgname']}-{pkginfo['pkgver']}"
    for k, v in pkginfo.items():
        normalized_info[k] = normalize_field(k, v)
    pkginfo = normalized_info
    pkginfo = ensure_fields(pkginfo)
    found = False
    exp_path = None
    db_name = None
    files = [('%FILES%', None)]
    for d in db_names:
        exp_path = db_extracted_dir / d / expected_d_name
        if exp_path.exists():
            logger.debug(f"{expected_d_name} in {d}")
            found = True
            db_name = d
            break
        else:
            logger.warning(f"{exp_path=} not found, continue searching")
    assert db_name is not None, "No db found for package {expected_d_name=}"
    type_indicators = {tarfile.REGTYPE: 'f', tarfile.AREGTYPE: 'f', tarfile.SYMTYPE: 'l', tarfile.DIRTYPE: 'd', tarfile.LNKTYPE: ''}
    for f in pkgf:
        if f.name.startswith('.'):
            # logger.warn(f"TODO: parse {f} and put info into sqlite")
            continue
        t = type_indicators[f.type]
        f = f.name
        files.append((f, t))
    files.sort()
    return (pkginfo, db_name, files)

def create_pkg_sqlitedb(logger, dest_db_dir, dest_db_name):
    db_file = pathlib.Path(f"{dest_db_dir}/{dest_db_name}.sqlite3").absolute()
    logger.info(f"creating sqlite db for repository: {db_file=}")
    if db_file.exists():
        db_file.unlink()
    db = sqlite3.connect(db_file)
    with contextlib.closing(db.cursor()) as cur:
        cur.execute("CREATE TABLE files (pkgname, file, type)")
        cur.execute('CREATE TABLE pkginfo (pkgname, pkgbase, pkgver, pkgdesc, url, builddate, size, arch, license, "group", packager_name, packager_email)')
        cur.execute('CREATE TABLE dependencies (pkgname, deptype, otherpkg, operator, version, reason)')
        cur.execute('CREATE TABLE backs_up (pkgname, path)')
        cur.execute('CREATE TABLE meta (key, value)')
    return db

def store_pkg_info(logger, db, pkginfo, db_name, files):
    pkgname = pkginfo['pkgname']
    logger.debug(f"store in db {pkgname=}")
    with db:
        with contextlib.closing(db.cursor()) as cur:
            values = []
            for (f, type_flag) in files:
                values.append((pkgname, f, type_flag))
                if len(values) >= 1000:
                    cur.executemany(f"INSERT INTO files VALUES(?, ?, ?)", values)
                    values = []
            if values:
                cur.executemany(f"INSERT INTO files VALUES(?, ?, ?)", values)
            # pkginfo
            values = pkginfo
            values['license'] = ' '.join(values['license'])
            if 'group' not in values:
                values['group'] = ''
            values['packager_name'] = values['packager']['realname']
            values['packager_email'] = values['packager']['email']
            sql = "INSERT INTO pkginfo VALUES(:pkgname, :pkgbase, :pkgver, :pkgdesc, :url, :builddate, :size, :arch, :license, :group, :packager_name, :packager_email)"
            cur.execute(sql, values)
            # pkginfo dependencies
            dependency_fields = ['provides', 'depend', 'optdepend', 'makedepend', 'checkdepend', 'conflict', 'replaces', ]
            for deptype in dependency_fields:
                if deptype in pkginfo and pkginfo[deptype]:
                    for values in pkginfo[deptype]:
                        values['pkgname'] = pkgname
                        values['deptype'] = deptype
                        cur.execute("INSERT INTO dependencies VALUES(:pkgname, :deptype, :otherpkg, :operator, :version, :reason)", values)
            # pkginfo backup
            sql = "INSERT INTO backs_up VALUES(:pkgname, :path)"
            if 'backup' in pkginfo and pkginfo['backup']:
                for path in pkginfo['backup']:
                    values = {'pkgname': pkgname, 'path': path}
                    cur.execute(sql, values)
            # TODO: safety net to ensure that all fields from pkginfo have been stored somewhere
    logger.debug(f"package stored in db {pkgname=}")

def command_merge_pacman_repositories_xsh(logger, source_db_dir, db_names, source_pkg_cache, dest_db_name, dest_db_dir, only_installed=False, root_dir=None):
    if root_dir is None:
        root_dir = pathlib.Path('/')
    logger.info((f"CALL command_merge_pacman_repositories_xsh("
    f"{source_db_dir=}\n"
    f"{db_names=}\n"
    f"{source_pkg_cache=}\n"
    f"{dest_db_name=}\n"
    f"{dest_db_dir=}\n"
    f"{only_installed=}\n"
    f"{root_dir=})"))

    #where we extract the .files.tar.gz files
    t_files_dir = pathlib.Path(tempfile.mkdtemp(prefix="minicluster-files-unarchived-"))
    #where we extract the .db files, each db in a subdirectory (core/, extra/ ...)
    t_db_dir = pathlib.Path(tempfile.mkdtemp(prefix="minicluster-db-unarchived-"))
    kv_reg = re.compile('^(?P<var>[a-zA-Z_]+)[^=]*=(?P<val>.+)')

    logger.info(f"{t_files_dir=}")
    logger.info(f"{t_db_dir=}")
    files_dirs = {}
    db_dirs = {}

    for d in db_names:
        d_file = source_db_dir / f"{d}.db"
        f_file = source_pkg_cache / f"{d}.files.tar.gz"
        assert d_file.exists()
        assert f_file.exists()
        db_dirs[d] = pathlib.Path("/tmp/minicluster-db-new")
        files_dirs[d] = pathlib.Path("/tmp/minicluster-files-new")
        if not db_dirs[d].exists():
            db_dirs[d].mkdir()
        if not files_dirs[d].exists():
            files_dirs[d].mkdir()
        # heavier operations, you can arrange for these to be skipped on subsequent runs during development
        # by commenting out these lines
        with tarfile.open(d_file, mode='r:*') as d_archive:
            d_archive_dir = t_db_dir / d
            if not d_archive_dir.exists():
                d_archive_dir.mkdir()
            # TODO: once python 3.11.4 is out, add filter='data'
            d_archive.extractall(path=d_archive_dir)
            logger.info(f"{d_archive_dir=}")
        with pushd(t_files_dir), tarfile.open(f_file, mode='r:*') as f_archive:
            # TODO: once python 3.11.4 is out, add filter='data'
            f_archive.extractall(path=t_files_dir)

    dest_db_dir.mkdir(parents=True)
    db = create_pkg_sqlitedb(logger, dest_db_dir, dest_db_name)

    # decide which pkg files to consider
    pkg_iter = itertools.chain(source_pkg_cache.glob('*.pkg.tar.zst'), source_pkg_cache.glob('*.pkg.tar.xz'))
    if only_installed:
        if root_dir:
            installed = $(pacman --root @(root_dir) -Q).splitlines()
        else:
            installed = $(pacman -Q).splitlines()
        installed_raw = [v.replace(' ', '-', 1) for v in installed]
        pkg_iter = [pf`{source_pkg_cache}/{v}.+?\\.pkg\\.tar\\.((zst)|(xz))$` for v in installed_raw]
        pkg_iter = [item for sublist in pkg_iter for item in sublist]
        logger.debug(f"{installed=}")
        logger.debug(f"{installed_raw=}")
        logger.debug(f"{pkg_iter=}")
        logger.info(f"{len(pkg_iter)=} {len(installed_raw)=} {len(installed)=}")
        assert len(installed_raw) > 0
        assert len(pkg_iter) > 0
        assert len(pkg_iter) == len(installed)

    # go through each considered pkg file
    for pkg_path in pkg_iter:
        (pkginfo, db_name, files) = get_pkg_info(pkg_path, kv_reg, t_db_dir, db_names, logger)
        assert db_name is not None, f"No db found for package {pkg_path}"
        logger.info(f"PROCESSING: {pkg_path}")
        expected_d_name=f"{pkginfo['pkgname']}-{pkginfo['pkgver']}"
        shutil.copy(pkg_path, dest_db_dir)
        shutil.copy(f"{pkg_path}.sig", dest_db_dir)
        store_pkg_info(logger, db, pkginfo, db_name, files[1:])
        # db files
        db_dir = db_dirs[db_name]
        src = t_db_dir / f"{db_name}/{expected_d_name}"
        assert src.exists()
        logger.info(f"COPY TREE: {src=} to {db_dir=}")
        shutil.copytree(src, db_dir / src.name)
        # files files
        files_dir = files_dirs[db_name]
        src = t_files_dir / expected_d_name
        assert src.exists()
        logger.debug(f"COPY TREE: {src=} to {files_dir=}")
        shutil.copytree(src, files_dir / src.name)
    db.close()

    with pushd("/tmp/minicluster-files-new"):
        res = make_archive("/tmp/minicluster-files-new", f"{dest_db_name}.files.tar.gz", f"{dest_db_dir}/", only_dirs=True)
        rm -rf *
        cd @(dest_db_dir)
        base_name = pathlib.Path(f"{dest_db_dir}/{dest_db_name}.files")
        base_name.symlink_to(f"{dest_db_name}.files.tar.gz")
    with pushd("/tmp/minicluster-db-new"):
        res = make_archive("/tmp/minicluster-db-new", f"{dest_db_name}.db.tar.gz", f"{dest_db_dir}/", only_dirs=True)
        rm -rf *
        cd @(dest_db_dir)
        base_name = pathlib.Path(f"{dest_db_dir}/{dest_db_name}.db")
        base_name.symlink_to(f"{dest_db_name}.db.tar.gz")

    logger.info(f"repo {dest_db_name} created at {dest_db_dir}/")
    shutil.rmtree(t_files_dir)
    shutil.rmtree(t_db_dir)
    for d in set(files_dirs.values()):
        shutil.rmtree(d)
    for d in set(db_dirs.values()):
        shutil.rmtree(d)
    return True


if __name__ == '__main__':
    source_db_dir = MINICLUSTER.ARGS.source_db_dir
    db_names = MINICLUSTER.ARGS.db_names
    source_pkg_cache = MINICLUSTER.ARGS.source_pkg_cache
    dest_db_name = MINICLUSTER.ARGS.dest_db_name
    dest_db_dir = MINICLUSTER.ARGS.dest_db_dir
    only_explicit = MINICLUSTER.ARGS.only_explicit

    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    $RAISE_SUBPROC_ERROR = True
    command_merge_pacman_repositories_xsh(logger, source_db_dir, db_names, source_pkg_cache, dest_db_name, dest_db_dir, only_installed=only_explicit)
