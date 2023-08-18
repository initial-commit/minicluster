#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--aur_clone', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--db_file', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import pathlib
import pygit2
import repobuilder.functions
import sqlite3

def create_pkg_sqlitedb(logger, db_file):
    logger.info(f"creating sqlite db for repository: {db_file=}")
    if db_file.exists():
        db_file.unlink()
    db = sqlite3.connect(db_file)
    with contextlib.closing(db.cursor()) as cur:
        cur.execute("CREATE TABLE files (pkgname, file, type)")
        cur.execute('CREATE TABLE pkginfo (pkgname, pkgbase, pkgver, pkgrel, epoch, pkgdesc, url, builddate, options, size, arch, source, license, "group", packager_name, packager_email)')
        cur.execute('CREATE TABLE dependencies (pkgname, deptype, otherpkg, operator, version, reason)')
        cur.execute('CREATE TABLE backs_up (pkgname, path)')
        cur.execute('CREATE TABLE meta (key, value)')
    return db

def normalize_meta(pkgid, meta):
    newmeta = {}
    handled_keys = []
    pkginfo_keys = ['pkgname', 'pkgbase', 'pkgver', 'pkgdesc', 'url', 'arch', 'license', 'pkgrel', 'epoch',
                'options', 'source', 'install', 'validpgpkeys', 'groups', 'changelog', ]
    pkginfo = {}
    dependency_keys = ['requires', 'makedepends', 'depends', 'provides', 'conflicts', 'optdepends', 'checkdepends', 'replaces', 'depend', ]
    dependencies = {}
    t = []
    for arch in repobuilder.functions.ARCHITECTURES:
        for reltype in dependency_keys:
            t.append(f"{reltype}_{arch}")
    dependency_keys.extend(t)

    checksum_keys = ['md5sums', 'sha224sums', 'sha256sums', 'sha265sums', 'sha384sums', 'sha512sums', 'sha1sums', 'b2sums', 'cksums', ]
    for pref in repobuilder.functions.METALIST_CHECKSUM_PREFIXES:
        for arch in repobuilder.functions.ARCHITECTURES:
            checksum_keys.append(f"{pref}{arch}")
    checksums = {}
    sources = {}
    noextract = {}
    backups = {}
    for k, v in meta.items():
        if k in pkginfo_keys:
            pkginfo[k] = v
        elif k in dependency_keys:
            dependencies[k] = v
        elif k in checksum_keys:
            checksums[k] = v
        elif k == 'sources' or k.startswith('source_'):
            sources[k] = v
        elif k == 'noextract':
            noextract[k] = v
        elif k == 'backup':
            backups[k] = v
        else:
            raise Exception(f"unhandled key for {pkgid=} {k=} with {v=}")
    newmeta['pkginfo'] = pkginfo
    newmeta['dependencies'] = dependencies
    newmeta['checksums'] = checksums
    newmeta['sources'] = sources
    return (True, newmeta)

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)

    aur_clone = pathlib.Path(MINICLUSTER.ARGS.aur_clone)
    db_file = pathlib.Path(MINICLUSTER.ARGS.db_file)

    if pf"{aur_clone}/.git".exists():
        aur_clone = aur_clone / '.git'

    assert not db_file.exists()

    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True

    repo = pygit2.Repository(aur_clone)
    i = 0
    for (pkgid, meta) in repobuilder.functions.aur_repo_iterator(repo):
        print(i, pkgid, meta)
        (success, newmeta) = normalize_meta(pkgid, meta)
        print(f"{newmeta=}")
        print("=======================")
        i += 1
