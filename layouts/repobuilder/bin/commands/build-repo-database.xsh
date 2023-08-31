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
        import requests
        import tarfile
        import io
        logger.info(f"import db {db_name=}")
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

            class Extractor(object):
                def __init__(self, conn, logger):
                    self.conn = conn
                    self.logger = logger.getChild(self.__class__.__name__)
                    self.lock = threading.Lock()
                    self.lock_ns = 0
                    self.lock_count = 0

                def __call__(self, pkgbuild_data):
                    st = self.conn.guest_exec_wait('/tmp/printsrcinfo.sh', input_data=pkgbuild_data)
                    if st['exitcode'] != 0 or len(st['err-data'].strip()) > 0:
                        logger.error(f"cannot extract srcinfo from pkgbuild: {st=}")
                        return None
                    return st['out-data']

                def exec_start(self, pkgbuild_data):
                    t1 = time.thread_time_ns()
                    with self.lock:
                        self.lock_ns += time.thread_time_ns() - t1
                        self.lock_count += 1
                        st = self.conn.guest_exec('/tmp/printsrcinfo.sh', input_data=pkgbuild_data)
                        return (int(st), True)

                def exec_result(self, pid):
                    t1 = time.thread_time_ns()
                    with self.lock:
                        self.lock_ns += time.thread_time_ns() - t1
                        self.lock_count += 1
                        st = self.conn.guest_exec_status(pid)
                        return st

            class ExtractorThread(threading.Thread):
                # TODO: conn instead of extractor
                def __init__(self, queue_in, queue_out, extractor, logger, cond):
                    self.queue_in = queue_in
                    self.queue_out = queue_out
                    self.extractor = extractor
                    self.cond = cond
                    self.logger = logger.getChild(self.__class__.__name__)
                    super().__init__()

                def run(self):
                    last = False
                    #local = threading.local()
                    while not last:
                        (pkgbase, files, last)= self.queue_in.get()
                        if last:
                            self.logger.info(f"THREAD {self.name} SENTINEL DETECTED, NOOP")
                            self.queue_in.task_done()
                            self.queue_in.put((pkgbase, files, True))
                            return
                        # TODO: here process with extractor
                        #local.t1 = time.thread_time()
                        (pid, success) = self.extractor.exec_start(files['PKGBUILD'])
                        if success:
                            exited = False
                            while not exited:
                                st = self.extractor.exec_result(pid)
                                exited = st['exited']
                                if not exited:
                                    time.sleep(0.050)
                            #local.dur = time.thread_time() - local.t1
                            #self.logger.info(f"duration: {local.dur=}")
                            if 'err-data' in st:
                                st['err-data'] = base64.b64decode(st['err-data'])
                                if len(st['err-data']):
                                    logger.warning(f"problems for pkg {pkgbase=} {st['err-data']}")
                            if st['exitcode'] == 0:
                                data = base64.b64decode(st['out-data'])
                                files['.SRCINFO-ORIGINAL'] = files['.SRCINFO']
                                files['.SRCINFO'] = data

                        # TODO: further processing with a function
                        #self.logger.info(f"THREAD {self.name} before putting in output: {pkgbase}")
                        self.queue_out.put((pkgbase, files))
                        #self.logger.info(f"THREAD {self.name} after putting in output: {pkgbase}")
                        self.queue_in.task_done()
                        #self.logger.info(f"THREAD {self.name} PROCESSED ITEM IN queue: {pkgbase}")

            BATCH_SIZE = 200
            WORKER_THREADS = 10
            queue_for_vm_input = queue.Queue(int(BATCH_SIZE*1.1+1))
            queue_for_vm_output = queue.Queue(BATCH_SIZE)
            extractor = Extractor(None, logger)
            if use_vm:
                new_img = command_make_derived_image_xsh(cwd_image, logger, base_handle, name)
                assert new_img is not None
                cwd_image = str(new_img.parent)
                new_img = str(new_img)
                booted = command_boot_image_xsh(cwd_image, logger, new_img, name, 1024, True, False)
                assert booted
                s = f"{cwd_image}/qga-{name}.sock"
                conn = cluster.qmp.Connection(s, logger)
                # copy script to /tmp
                written = command_copy_files_xsh(cwd_image, logger, "{DIR_M}/printsrcinfo.sh", '{name}:/tmp/printsrcinfo.sh', additional_env={'name': name})
                assert written > 1
                st = conn.guest_exec_wait('chmod +x /tmp/printsrcinfo.sh')
                assert st['exitcode'] == 0
                # execute script inside with input-data

                extractor = Extractor(conn, logger)

            errorlogger = logger
            repo = pygit2.Repository(aur_clone)
            i = 0
            buffer = []
            cond = threading.Condition()
            threads = [ExtractorThread(queue_for_vm_input, queue_for_vm_output, extractor, logger, cond) for i in range(WORKER_THREADS)]
            stored_items = 0
            processed_items = 0
            for th in threads:
                #logger.info(f"STARTING THREAD {th.name}")
                th.start()
            for (pkgbase, files, last) in repobuilder.functions.aur_repo_iterator_simple(repo, known_packages):
                if last:
                    # TODO: use a condition instead of the sentinel value
                    queue_for_vm_input.put((pkgbase, files, last))
                    break
                i += 1
                #logger.info(f"processing {pkgbase=}")
                processed_items += 1
                queue_for_vm_input.put((pkgbase, files, last))
                if queue_for_vm_output.qsize() >= BATCH_SIZE:
                    qsize_in = queue_for_vm_input.qsize()
                    logger.info(f"Flushing, input at size {qsize_in=}")
                    #queue_for_vm_input.join()
                    logger.info("no more input, storing output")
                    while queue_for_vm_output.qsize() >= 1:
                        buffer.append(queue_for_vm_output.get())
                        stored_items += 1
                        queue_for_vm_output.task_done()
                        buffer_size = len(buffer)
                        if buffer_size % 100 == 0:
                            qsize_in = queue_for_vm_input.qsize()
                            qsize_out = queue_for_vm_output.qsize()
                            logger.info(f"before join stats: {qsize_in=} {qsize_out=} {buffer_size=} {stored_items=} {processed_items=}")
            #logger.info("joining input threads")
            qsize_in = queue_for_vm_input.qsize()
            qsize_out = queue_for_vm_output.qsize()
            buffer_size = len(buffer)
            logger.info(f"before join stats: {qsize_in=} {qsize_out=} {buffer_size=} {stored_items=} {processed_items=}")
            #logger.info(f"emptying output queue")
            while queue_for_vm_input.qsize() + queue_for_vm_output.qsize() > 1:
                #logger.info(f"getting one item")
                item = queue_for_vm_output.get()
                #logger.info(f"got item {item=}")
                buffer.append(item)
                stored_items += 1
                queue_for_vm_output.task_done()
                buffer_size = len(buffer)
                if buffer_size % 100 == 0:
                    qsize_in = queue_for_vm_input.qsize()
                    qsize_out = queue_for_vm_output.qsize()
                    logger.info(f"during loop stats: {qsize_in=} {qsize_out=} {buffer_size=} {stored_items=} {processed_items=}")
            qsize_in = queue_for_vm_input.qsize()
            qsize_out = queue_for_vm_output.qsize()
            buffer_size = len(buffer)
            lock_cont_ms = extractor.lock_ns / 1000
            lock_cont_avg = lock_cont_ms / extractor.lock_count
            logger.info(f"after loop stats: {qsize_in=} {qsize_out=} {buffer_size=} {stored_items=} {processed_items=} {lock_cont_ms=} {lock_cont_avg=}")
            assert queue_for_vm_output.qsize() == 0
            #cond.notify_all()
            for th in threads:
                #logger.info(f"JOINING THREAD {th.name}")
                th.join()
                #logger.info(f"JOINED THREAD {th.name}")
            #logger.info("joined input threads")
            #TODO: save buffer
            assert queue_for_vm_input.qsize() == 1
            (pkgname, files, last_sentinel) = queue_for_vm_input.get()
            assert last_sentinel
            assert pkgname is None
            assert files is None
            qsize_in = queue_for_vm_input.qsize()
            qsize_out = queue_for_vm_output.qsize()
            buffer_size = len(buffer)
            logger.info(f"after join stats: {qsize_in=} {qsize_out=} {buffer_size=} {stored_items=} {processed_items=}")
            #logger.info(f"{buffer=}")
            #queue_for_vm_input.join()
            #for (pkgid, meta, last) in repobuilder.functions.aur_repo_iterator(repo, extractor, errorlogger):
            #    #if pkgid and 'mediasort' not in pkgid:
            #    #    continue
            #    #logger.info(f"{pkgid=} {meta=}")
            #    if not last:
            #        (success, newmeta) = normalize_meta(pkgid, meta, known_packages, logger)
            #        if newmeta['pkginfo']['pkgname'] in known_packages:
            #            buffer.append(newmeta)
            #    if len(buffer) == 2500 or last:
            #        upsert_aur_package(buffer, db, logger)
            #        buffer = []
            #    i += 1

            if use_vm:
                command_poweroff_image_xsh(cwd_image, logger, name)
