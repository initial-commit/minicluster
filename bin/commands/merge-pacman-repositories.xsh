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
import sqlite3
import contextlib
from cluster.functions import ZstdTarFile, pushd

def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"build-tmp-{handle}-{r}"

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
            # logger.warn(f"TODO: parse {f} and put info into sqlite")
            continue
        suffix = ''
        if f.isdir():
            suffix = '/'
        f = f.name + suffix
        files.append(f)
    files.sort()
    return (pkginfo, db_name, files)

def create_pkg_sqlitedb(logger, dest_db_dir, dest_db_name, schema_path):
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
    with db:
        with contextlib.closing(db.cursor()) as cur:
            values = []
            for f in files:
                type_flag = 'f'
                if f.endswith('/'):
                    type_flag = 'd'
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
    kv_reg = re.compile('^(?P<var>[a-zA-Z_]+)[^=]*=(?P<val>.+)')
    temp_db_uncompressed_dir = pathlib.Path(f"{dest_db_dir}/{dest_db_name}").absolute()
    if dest_db_dir.exists():
        shutil.rmtree(dest_db_dir)
    temp_db_uncompressed_dir.mkdir(parents=True)
    logger.info(f"{temp_db_uncompressed_dir=}")
    logger.info(f"creating database {dest_db_name} in {dest_db_dir}")
    to_remove = []
    schema_path = None
    db = create_pkg_sqlitedb(logger, dest_db_dir, dest_db_name, schema_path)
    for pkg_path in itertools.chain(source_pkg_cache.glob('*.pkg.tar.zst'), source_pkg_cache.glob('*.pkg.tar.xz')):
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
        store_pkg_info(logger, db, pkginfo, db_name, files[1:])

    db.close()
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

    shutil.rmtree(cwd)
    shutil.rmtree(db_extracted_dir)
    # TODO save db_create_start, db_create_end, the params to this function

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
