#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    source_command 'make-empty-image.xsh'
    source_command 'boot-image.xsh'
    source_command 'poweroff-image.xsh'
    source_command 'copy-files.xsh'
    from cluster.functions import str2bool_exc as strtobool
    import sys
    MINICLUSTER.ARGPARSE.add_argument('--aur_clone', required=False, default=None)
    MINICLUSTER.ARGPARSE.add_argument('--aurweb', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--db_names', default=[], nargs='+', required=False, help="The database names, ex core, multilib, extra")
    MINICLUSTER.ARGPARSE.add_argument('--db_file', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--image', help="The image to use to execute unsafe operations in isolation, e.g. makepkg", required='--aur_clone' in sys.argv)
    MINICLUSTER.ARGPARSE.add_argument('--name', help="The name of the VM", required='--aur_clone' in sys.argv)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import logging.handlers
import pathlib
import pygit2
import repobuilder.functions
import cluster.functions
import sqlite3
import contextlib
import re
import json
from datetime import datetime
import pytz
import threading
import queue
import base64
import io
import requests
import tarfile
import difflib
import stat
import psutil


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
        'filename TEXT, download_size INTEGER, install_size INTEGER, package_md5sum TEXT, package_shasum TEXT, pgpsig TEXT, package_arch TEXT, '
        'votes INTEGER, lastupdated TEXT, flagged INTEGER'
        ') STRICT'))
        cur.execute('CREATE TABLE dependencies (pkgname TEXT, deptype TEXT, otherpkg TEXT, operator TEXT, version TEXT, reason TEXT)')
        cur.execute('CREATE TABLE backs_up (pkgname TEXT, path TEXT)')
        cur.execute('CREATE TABLE meta (key TEXT, reponame TEXT, value TEXT)')
        cur.execute(("CREATE VIEW packages_removed_from_aurweb AS "
            "WITH aur AS (SELECT pkgname FROM pkginfo p WHERE p.reponame = 'aur'), "
            "aurweb AS (SELECT pkgname FROM pkginfo p WHERE p.reponame = 'aurweb') "
            "SELECT aur.pkgname FROM aur "
            "LEFT JOIN aurweb ON aur.pkgname = aurweb.pkgname "
            "WHERE aurweb.pkgname IS NULL;")
        )
        cur.execute(('CREATE TABLE files'
            '(pkgid TEXT, reponame TEXT, fpath TEXT, mime_short TEXT, mime_long TEXT)'
        ))
        cur.execute(('CREATE TABLE links'
            '(pkgid TEXT, reponame TEXT, link TEXT)'
        ))
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

def upsert_aurweb_package(rawbatch, db):
    packages = []
    buffer = []
    for rawdata in rawbatch:
        values = {
            'pkgid': rawdata['pkgid'],
            'reponame': 'aurweb',
            'pkgbase': rawdata['name'],
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
        packages.append(values['pkgbase'])
        buffer.append(values)
    with db:
        with contextlib.closing(db.cursor()) as cur:
            sql = ("INSERT INTO pkginfo(pkgid, reponame, pkgbase, pkgver, pkgrel, epoch, pkgdesc, packager_name, popularity, votes, lastupdated, flagged)"
            "VALUES(:pkgid, :reponame, :pkgbase, :pkgver, :pkgrel, :epoch, :pkgdesc, :packager_name, :popularity, :votes, :lastupdated, :flagged)")
            cur.executemany(sql, buffer)
    return packages


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


if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)

    aur_clone = MINICLUSTER.ARGS.aur_clone
    if aur_clone:
        aur_clone = pathlib.Path(aur_clone)
    db_file = pathlib.Path(MINICLUSTER.ARGS.db_file)
    aurweb = MINICLUSTER.ARGS.aurweb
    db_names = MINICLUSTER.ARGS.db_names
    image = MINICLUSTER.ARGS.image
    name = MINICLUSTER.ARGS.name

    existing_db = db_file.exists()
    if not existing_db:
        db = create_pkg_sqlitedb(logger, db_file)
    else:
        db = sqlite3.connect(db_file)

    if image:
        image = pathlib.Path(image).resolve()
        assert image.exists()

    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True

    def get_packages_ops(db, prev_db_file):
        pass

    def get_common_packages(db, repo1, repo2):
        pass

    #for x in get_removed_packages(db, 'test.sqlite3', 'aurweb', 'aurweb'):
    #    print(x)

    def extract_desc(fp, package):
        verparse = re.compile(r'((?P<epoch>\d+):)?(?P<version>.+?)-(?P<rel>\d+)')
        tr_list = lambda v: v if isinstance(v, list) else [v]
        tr_version = lambda v: verparse.match(v).groupdict()['version']
        def tr_rel(v):
            v = verparse.match(v).groupdict()['rel']
            if v:
                return int(v)
            return None
        def tr_epoch(v):
            v = verparse.match(v).groupdict()['epoch']
            if v:
                return int(v)
            return None
        tr_int = lambda v: int(v) if v is not None else None
        tr_timestamp = lambda v: datetime.fromtimestamp(int(v), pytz.utc)
        tr_packager_name = lambda v: cluster.functions.contact_parse(v)['realname']
        tr_packager_email = lambda v: cluster.functions.contact_parse(v)['email']
        tr_deps = lambda v: cluster.functions.depend_parse(tr_list(v))
        key_transforms = {
            'FILENAME': {'name': 'filename'},
            'NAME': {'name': 'pkgname'},
            'DESC': {'name': 'pkgdesc'},
            'BASE': {'name': 'pkgbase'},
            'VERSION': {'name': 'pkgver', 'tr': tr_version, },
            'PKGREL': {'name': 'pkgrel', 'tr': tr_rel, },
            'EPOCH': {'name': 'epoch', 'tr': tr_epoch, },
            'CSIZE': {'name': 'download_size', 'tr': tr_int, },
            'ISIZE': {'name': 'install_size', 'tr': tr_int, },
            'MD5SUM': {'name': 'package_md5sum'},
            'SHA256SUM': {'name': 'package_shasum'},
            'LICENSE': {'name': 'license', 'tr': tr_list},
            'ARCH': {'name': 'package_arch'},
            'PGPSIG': {'name': 'pgpsig'},
            'URL': {'name': 'url'},
            'BUILDDATE': {'name': 'builddate', 'tr': tr_timestamp, }, # format timestamp with TZ
            'PACKAGER': {'name': 'packager_name', 'tr': tr_packager_name, },
            'PACKAGER_EMAIL': {'name': 'packager_email', 'tr': tr_packager_email, },
            'GROUPS': {'name': 'group', 'tr': tr_list},
            'PROVIDES': {'name': 'provides', 'tr': tr_deps},
            'DEPENDS': {'name': 'depends', 'tr': tr_deps},
            'MAKEDEPENDS': {'name': 'makedepends', 'tr': tr_deps},
            'REPLACES': {'name': 'replaces', 'tr': tr_deps},
            'CONFLICTS': {'name': 'conflicts', 'tr': tr_deps},
            'CHECKDEPENDS': {'name': 'checkdepends', 'tr': tr_deps},
            'OPTDEPENDS': {'name': 'optdepends', 'tr': tr_deps},
        }
        #PACKAGER
        f = map(lambda v: v.decode('utf-8').strip(), fp.readlines())
        lines = [l for l in f if l]
        vals = {}
        k = None
        for line in lines:
            if line.startswith('%') and line.endswith('%'):
                k = line[1:-1]
                continue
            if k:
                if k not in vals:
                    vals[k] = line # we have the first value
                else:
                    if not isinstance(vals[k], list):
                        backup = vals[k] # we have the second value
                        vals[k] = [backup, line]
                    else:
                        vals[k].append(line) # we have subsequent values
        vals['PACKAGER_EMAIL'] = vals['PACKAGER']
        vals['EPOCH'] = vals['VERSION']
        vals['PKGREL'] = vals['VERSION']
        norm_vals = {}
        for k, v in vals.items():
            assert k in key_transforms, f"spec for key not available in package {package=} {k=}"
            spec = key_transforms[k]
            new_k = spec['name']
            if 'tr' in spec:
                v = spec['tr'](v)
            norm_vals[new_k] = v
        dependencies = {
            'provides': norm_vals.pop('provides', None),
            'depends': norm_vals.pop('depends', None),
            'makedepends': norm_vals.pop('makedepends', None),
            'replaces': norm_vals.pop('replaces', None),
            'conflicts': norm_vals.pop('conflicts', None),
            'checkdepends': norm_vals.pop('checkdepends', None),
            'optdepends': norm_vals.pop('optdepends', None),
        }
        return (norm_vals, dependencies)

    def pkg_db_file_desc_iter(f_in_mem, logger):
        tar = tarfile.open(fileobj=f_in_mem)
        for tinfo in tar.getmembers():
            if not tinfo.isfile():
                continue
            name = tinfo.name.split('/')[0]
            pkginfo, dependencies = extract_desc(tar.extractfile(tinfo), name)
            yield (name, pkginfo, dependencies, False)
        yield (None, None, None, True)


    def upsert_binary_package(db_name, buffer, db, logger):
        pkginfos = []
        dependencies = []
        for (pkgid, pkginfo, dep) in buffer:
            pkginfo['reponame'] = db_name
            pkginfo['pkgid'] = pkgid
            for k, v in pkginfo.items():
                if isinstance(v, list):
                    pkginfo[k] = json.dumps(v)
            pkginfos.append(pkginfo)
            for deptype, depvals in dep.items():
                if not depvals:
                    continue
                for depdict in depvals:
                    depdict['deptype'] = deptype
                    depdict['pkgname'] = pkginfo['pkgname']
                    dependencies.append(depdict)
        with db:
            with contextlib.closing(db.cursor()) as cur:
                sql = ("INSERT INTO pkginfo(pkgid, reponame, pkgname, pkgbase, pkgver, pkgrel, epoch, pkgdesc, url, package_arch, license, packager_name, packager_email, builddate, filename, download_size, install_size, package_md5sum, package_shasum, pgpsig, package_arch)"
                "VALUES(:pkgid, :reponame, :pkgname, :pkgbase, :pkgver, :pkgrel, :epoch, :pkgdesc, :url, :package_arch, :license, :packager_name, :packager_email, :builddate, :filename, :download_size, :install_size, :package_md5sum, :package_shasum, :pgpsig, :package_arch)")
                cur.executemany(sql, pkginfos)
        with db:
            with contextlib.closing(db.cursor()) as cur:
                sql = ("INSERT INTO dependencies(pkgname, deptype, otherpkg, operator, version, reason)"
                "VALUES(:pkgname, :deptype, :otherpkg, :operator, :version, :reason)")
                cur.executemany(sql, dependencies)

    for db_name in db_names:
        logger.info(f"import db {db_name=}")
        # TODO: use mirror here
        base_link = f"https://geo.mirror.pkgbuild.com/{db_name}/os/x86_64/"
        files_link = f"{base_link}{db_name}.files.tar.gz"
        db_link = f"{base_link}{db_name}.db.tar.gz"
        links_link = f"{base_link}{db_name}.links.tar.gz"
        ### db_link .db file
        res = requests.get(db_link, allow_redirects=True)
        assert res.status_code == 200, f"Response for {dblink=} is not 200"
        #TODO: assert bytes
        #TODO: cache and add If-Newer-Than
        f_in_mem = io.BytesIO(res.content)
        buffer = []
        for pkgid, pkginfo, dependencies, last in pkg_db_file_desc_iter(f_in_mem, logger):
            if not last:
                buffer.append((pkgid, pkginfo, dependencies))
            if len(buffer) == 2500 or last:
                upsert_binary_package(db_name, buffer, db, logger)
                buffer = []
        ### others:
        logger.info(files_link)
        logger.info(links_link)

    # TODO: also set known_packages

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
                logger.info(f"aurweb packages processed: {i+1}")
            i += 1
    else:
        with db:
            with contextlib.closing(db.cursor()) as cur:
                res = cur.execute("SELECT pkgbase FROM pkginfo WHERE reponame='aurweb'")
                known_packages = [v[0] for v in res.fetchall()]
    known_packages = set(known_packages)

    if aur_clone:
        if pf"{aur_clone}/.git".exists():
            aur_clone = aur_clone / '.git'

        use_vm = True
        if aur_clone.exists():
            cwd_image = str(image.parent)
            extractor = lambda v: None
            base_handle = image.stem


            BATCH_SIZE = 200
            WORKER_THREADS = int(psutil.cpu_count()*3)
            WORKDIR = '/tmp'
            queue_for_vm_input = queue.Queue(WORKER_THREADS)
            queue_for_vm_output = queue.Queue(BATCH_SIZE)
            extractor = repobuilder.functions.Extractor(None, logger, WORKDIR)
            if use_vm:
                new_img = command_make_derived_image_xsh(cwd_image, logger, base_handle, name)
                assert new_img is not None
                cwd_image = str(new_img.parent)
                new_img = str(new_img)
                booted = command_boot_image_xsh(cwd_image, logger, new_img, name, 4096, 8, True, False)
                assert booted
                s = f"{cwd_image}/qga-{name}.sock"
                conn = cluster.qmp.Connection(s, logger)
                written = command_copy_files_xsh(cwd_image, logger, "{DIR_M}/printsrcinfo.sh", '{name}:/{WORKDIR}/printsrcinfo.sh', additional_env={'name': name, 'WORKDIR': WORKDIR})
                assert written > 1
                st = conn.guest_exec_wait('rm -rf /root/.gnupg')
                st = conn.guest_exec_wait(f'chmod +x /{WORKDIR}/printsrcinfo.sh')
                assert st['exitcode'] == 0
                if not conn.guest_has_package('base-devel'):
                    logger.info("syncing databases")
                    conn.guest_sync_pacman_databases()
                    logger.info("installing base-devel")
                    conn.guest_install_package('base-devel')

                extractor = repobuilder.functions.Extractor(conn, logger, WORKDIR)

            errorlogger = logger
            repo = pygit2.Repository(aur_clone)
            i = 0
            buffer = []
            cond = threading.Condition()
            threads = [repobuilder.functions.ExtractorThread(queue_for_vm_input, queue_for_vm_output, extractor, logger, cond) for i in range(WORKER_THREADS)]
            stored_items = 0
            git_processed_items = 0
            monitoring_thread = repobuilder.functions.MonitoringThread(queue_for_vm_input, queue_for_vm_output, logger)
            monitoring_thread.start()
            storage_thread = repobuilder.functions.StorageThread(db, queue_for_vm_output, logger)
            storage_thread.start()
            for th in threads:
                #logger.info(f"STARTING THREAD {th.name}")
                th.start()
            put_duration = 0
            for (pkgbase, files, last) in repobuilder.functions.aur_repo_iterator_simple(repo, known_packages):
                if not last:
                    before = time.time()
                    queue_for_vm_input.put((pkgbase, files, last))
                    put_duration += time.time() - before
                    qsize_in = queue_for_vm_input.qsize()
                    qsize_out = queue_for_vm_output.qsize()
                    git_processed_items += 1
                    monitoring_thread.git_processed = git_processed_items
                    monitoring_thread.put_duration = put_duration
                    logger.info(f"got for processing: {pkgbase=} {git_processed_items=} {qsize_in=} {qsize_out=}")
                else:
                    alive_workers = len([1 for t in threads if t.is_alive()])
                    logger.info(f"sending number of sentinels: {alive_workers=} monitoring")
                    for i in range(alive_workers):
                        queue_for_vm_input.put((None, None, True))
            logger.info("joining input threads")
            for th in threads:
                logger.info(f"JOINING THREAD {th.name}")
                th.join()
                logger.info(f"JOINED THREAD {th.name}")
            logger.info("joined input threads")

            logger.info(f"join input queue: {queue_for_vm_input.qsize()}")
            queue_for_vm_input.join()
            logger.info(f"joined input queue")

            logger.info(f"join output queue: {queue_for_vm_output.qsize()}")
            queue_for_vm_output.join()
            logger.info(f"joined output queue")

            logger.info(f"join storage thread")
            storage_thread.do_store.clear()
            storage_thread.join()
            logger.info(f"joined storage thread")

            logger.info(f"join monitoring thread")
            monitoring_thread.keep_monitoring.clear()
            monitoring_thread.join()
            logger.info(f"joined monitoring thread")

            if storage_thread.items_stored != git_processed_items:
                qsize_in = queue_for_vm_input.qsize()
                qsize_out = queue_for_vm_output.qsize()
                logger.error(f"not all items stored: {git_processed_items=} {storage_thread.items_stored=} {qsize_in=} {qsize_out=}")

            if use_vm:
                command_poweroff_image_xsh(cwd_image, logger, name)
