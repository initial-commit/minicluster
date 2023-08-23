#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    from cluster.functions import str2bool_exc as strtobool
    MINICLUSTER.ARGPARSE.add_argument('--aur_clone', required=False, default=None)
    MINICLUSTER.ARGPARSE.add_argument('--aurweb', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--db_file', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import pathlib
import pygit2
import repobuilder.functions
import cluster.functions
import sqlite3
import contextlib
import re
import json

def create_pkg_sqlitedb(logger, db_file):
    logger.info(f"creating sqlite db for repository: {db_file=}")
    if db_file.exists():
        db_file.unlink()
    db = sqlite3.connect(db_file)
    with contextlib.closing(db.cursor()) as cur:
        cur.execute(('CREATE TABLE pkginfo '
        '(pkgid TEXT, reponame TEXT, pkgname TEXT, pkgbase TEXT, pkgver TEXT, pkgrel REAL, epoch INTEGER, '
        'pkgdesc TEXT, url TEXT, builddate TEXT, options TEXT, size INTEGER, arch TEXT, source TEXT, license TEXT, '
        'checksums TEXT, noextract TEXT, '
        '"group" TEXT, packager_name TEXT, packager_email TEXT, sources TEXT, popularity REAL, '
        'votes INTEGER, lastupdated TEXT, flagged INTEGER'
        ') STRICT'))
        cur.execute('CREATE TABLE dependencies (pkgname TEXT, deptype TEXT, otherpkg TEXT, operator TEXT, version TEXT, reason TEXT)')
        cur.execute('CREATE TABLE backs_up (pkgname, path)')
        cur.execute('CREATE TABLE meta (key, value)')
        cur.execute("""CREATE VIEW packages_removed_from_aurweb AS with aur as (
select
	pkgname
from
	pkginfo p
where
	p.reponame = 'aur'),
aurweb as (
select
	pkgname
from
	pkginfo p
where
	p.reponame = 'aurweb')

select aur.pkgname from aur
left join aurweb on aur.pkgname = aurweb.pkgname
where aurweb.pkgname is null;""")
    return db

def normalize_meta(pkgid, meta, known_packages, logger):
    newmeta = {}
    handled_keys = []
    pkginfo_keys = ['pkgname', 'pkgbase', 'pkgver', 'pkgdesc', 'url', 'arch', 'license', 'pkgrel', 'epoch',
                'options', 'source', 'install', 'validpgpkeys', 'groups', 'changelog', ]
    pkginfo = {'pkgid': pkgid}
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
            # if we split by words and flatten and if all are known packages, then use that instead, but issue an error for the package
            # TODO: this whole logic needs to be disabled and re-checked against aur once we have all packages from all repos
            newv = []
            for potentially_broken in v:
                potentially_broken = potentially_broken.strip()
                potentially_broken = re.sub(r'\s*(=|<=|>=)\s*', r'\1', potentially_broken)
                raw_vals = set(filter(None, re.split(r'[\s,]+', potentially_broken)))
                if raw_vals.issubset(known_packages):
                    newv.extend(list(raw_vals))
                    #TODO: this is a hard error
                    logger.error(f"HAS SPACES! {k=} {v=} {meta=}")
                else:
                    if k in ['makedepends', 'conflicts', 'depends', 'provides']:
                        #TODO: this is a hard error
                        potentially_broken = potentially_broken.split()
                        newv.extend(potentially_broken)
                    else:
                        newv.append(potentially_broken)
            dependencies[k] = newv
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

    if 'source' not in pkginfo:
        pkginfo['source'] = []
    sources['source'] = pkginfo['source']
    sources = {k:v for k,v in sources.items() if v}
    pkginfo['source'] = sources

    pkginfo['pkgrel'] = pkginfo.get('pkgrel', 1.0)
    try:
        pkginfo['pkgrel'] = float(pkginfo['pkgrel'])
    except ValueError:
        pkginfo['pkgrel'] = float(re.sub(r'[^0-9.]+', '', pkginfo['pkgrel']))
    newmeta['pkginfo'] = pkginfo
    newmeta['dependencies'] = dependencies
    newmeta['checksums'] = checksums
    newmeta['noextract'] = noextract
    newmeta['backups'] = backups
    return (True, newmeta)

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)

    aur_clone = MINICLUSTER.ARGS.aur_clone
    if aur_clone:
        aur_clone = pathlib.Path(aur_clone)
    db_file = pathlib.Path(MINICLUSTER.ARGS.db_file)
    aurweb = MINICLUSTER.ARGS.aurweb

    existing_db = db_file.exists()
    if not existing_db:
        db = create_pkg_sqlitedb(logger, db_file)
    else:
        db = sqlite3.connect(db_file)

    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True

    def upsert_aurweb_package(rawbatch, db):
        packages = []
        buffer = []
        for rawdata in rawbatch:
            values = {
                'pkgid': rawdata['pkgid'],
                'reponame': 'aurweb',
                'pkgname': rawdata['name'],
                'pkgver': rawdata['pkgver'],
                'pkgrel': rawdata['pkgrel'],
                'epoch': rawdata.get('epoch', None),
                'pkgdesc': rawdata['description'],
                'packager_name': rawdata['maintainer'],
                'popularity': rawdata['popularity'],
                'votes': rawdata['votes'],
                'lastupdated': rawdata['lastupdated'],
                'flagged': rawdata['flagged'],
            }
            packages.append(values['pkgname'])
            buffer.append(values)
        with db:
            with contextlib.closing(db.cursor()) as cur:
                sql = ("INSERT INTO pkginfo(pkgid, reponame, pkgname, pkgver, pkgrel, epoch, pkgdesc, packager_name, popularity, votes, lastupdated, flagged)"
                "VALUES(:pkgid, :reponame, :pkgname, :pkgver, :pkgrel, :epoch, :pkgdesc, :packager_name, :popularity, :votes, :lastupdated, :flagged)")
                cur.executemany(sql, buffer)
        return packages

    def upsert_aur_package(rawbatch, db, logger):
        buffer = []
        buffer_dependencies = []
        for rawdata in rawbatch:
            pkginfo = rawdata.pop('pkginfo')
            dependencies = rawdata.pop('dependencies')
            logger.info(f"{dependencies=}")
            for deptype, depvals in dependencies.items():
                logger.info(f"===========================================")
                logger.info(f"{pkginfo['pkgname']}\t{deptype}\t{depvals=}")
                depvals = list(filter(None, depvals))
                try:
                    vals = cluster.functions.depend_parse(depvals)
                    logger.info(f"INITIAL {vals=}")
                except AssertionError:
                    logger.error(f"{deptype=} {depvals=} {pkginfo=}")
                    raise
                #logger.info(f"{vals=}")
                for depdict in vals:
                    depdict['deptype'] = deptype
                    depdict['pkgname'] = pkginfo['pkgname']
                    buffer_dependencies.append(depdict)
            #logger.info(f"{dependencies=}")
            checksums = rawdata.pop('checksums')
            noextract = rawdata.pop('noextract')
            backups = rawdata.pop('backups')
            assert len(rawdata) == 0, f"Unhandled keys in rawdata: {rawdata.keys()}"
            values = {
                'pkgid': pkginfo['pkgid'],
                'reponame': 'aur',
                'pkgname': pkginfo['pkgname'],
                'pkgbase': pkginfo['pkgbase'],
                'pkgver': pkginfo['pkgver'],
                'pkgrel': float(pkginfo['pkgrel']),
                'epoch': pkginfo.get('epoch', None),
                'pkgdesc': pkginfo.get('pkgdesc', None),
                'url': pkginfo.get('url', None),
                'arch': json.dumps(pkginfo.get('arch', None)),
                'license': json.dumps(pkginfo.get('license', None)),
                'options': json.dumps(pkginfo.get('options', None)),
                'source': json.dumps(pkginfo.get('source', None)),
                'checksums': json.dumps(checksums),
                'noextract': json.dumps(pkginfo.get('noextract', None)),
            }
            logger.info(f"{values=}")
            buffer.append(values)
        with db:
            with contextlib.closing(db.cursor()) as cur:
                sql = ("INSERT INTO pkginfo(pkgid, reponame, pkgname, pkgbase, pkgver, pkgrel, epoch, pkgdesc, url, arch, license, options, source, checksums, noextract)"
                "VALUES(:pkgid, :reponame, :pkgname, :pkgbase, :pkgver, :pkgrel, :epoch, :pkgdesc, :url, :arch, :license, :options, :source, :checksums, :noextract)")
                cur.executemany(sql, buffer)
        with db:
            with contextlib.closing(db.cursor()) as cur:
                sql = ("INSERT INTO dependencies(pkgname, deptype, otherpkg, operator, version, reason)"
                "VALUES(:pkgname, :deptype, :otherpkg, :operator, :version, :reason)")
                #logger.info(f"{buffer_dependencies=}")
                cur.executemany(sql, buffer_dependencies)

    def get_removed_packages(db, prev_db_file, fromrepo):
        with db:
            with contextlib.closing(db.cursor()) as cur:
                sql = f"attach database '{prev_db_file}' as before;"
                cur.execute(sql)
            with contextlib.closing(db.cursor()) as cur:
                sql = f"SELECT t0.pkgname AS pkgname_before FROM before.pkginfo AS t0 LEFT JOIN pkginfo AS t1 ON t0.pkgname = t1.pkgname WHERE t1.pkgname IS NULL AND t0.reponame = '{fromrepo}'"
                for row in cur.execute(sql):
                    yield row[0]
            with contextlib.closing(db.cursor()) as cur:
                sql = f"detach database before;"
                cur.execute(sql)
    def get_packages_ops(db, prev_db_file):
        pass

    def get_common_packages(db, repo1, repo2):
        pass

    #for x in get_removed_packages(db, 'test.sqlite3', 'aurweb', 'aurweb'):
    #    print(x)

    known_packages = []
    if aurweb:
        since_limit = None
        buffer = []
        i = 0
        for (data, last) in repobuilder.functions.aurweb_pkg_iterator(since_limit=since_limit):
            if not last:
                buffer.append(data)
            if len(buffer) == 2500 or last:
                pkgnames = upsert_aurweb_package(buffer, db)
                known_packages.extend(pkgnames)
                buffer = []
                logger.info(f"{i=}")
            i += 1
    else:
        res = db.execute("SELECT pkgname FROM pkginfo WHERE reponame='aurweb'")
        known_packages = [v[0] for v in res.fetchall()]
    known_packages = set(known_packages)

    if aur_clone:
        if pf"{aur_clone}/.git".exists():
            aur_clone = aur_clone / '.git'

        if aur_clone.exists():
            repo = pygit2.Repository(aur_clone)
            i = 0
            buffer = []
            for (pkgid, meta, last) in repobuilder.functions.aur_repo_iterator(repo):
                #if pkgid not in ['0ad-git-1:A26.r920.gc4a0ae4ff-1']:
                #    continue
                if not last:
                    (success, newmeta) = normalize_meta(pkgid, meta, known_packages, logger)
                    if newmeta['pkginfo']['pkgname'] in known_packages:
                        buffer.append(newmeta)
                if len(buffer) == 2500 or last:
                    upsert_aur_package(buffer, db, logger)
                    buffer = []
                i += 1
