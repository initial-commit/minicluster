#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    from cluster.functions import str2bool_exc as strtobool
    import sys
    MINICLUSTER.ARGPARSE.add_argument('--checksum', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=False, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--db_file', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)


import logging
import pathlib
import repobuilder.functions
import cluster.functions
import sqlite3
import contextlib
import re
import json
import io
import requests
from urllib.parse import urlparse
import collections
import time

def map_sources_and_checksum_keys(sources, checksums, algos, logger):
    s_keys = list(sources.keys())
    c_keys = list(checksums.keys())
    pairs = []
    if len(s_keys) == 1 and len(c_keys) == 1:
        pairs.append((s_keys[0], (c_keys[0],), None))
    else:
        if len(s_keys) == 1:
            p = (s_keys[0], tuple(c_keys), None)
            pairs.append(p)
        else:
            if len(s_keys) == len(c_keys):
                for i in range(len(s_keys)):
                    p = (s_keys[i], (c_keys[i],), None)
                    pairs.append(p)
            else:
                for s_index in range(len(s_keys)):
                    s_key = s_keys[s_index]
                    s_arch = None
                    c_keys_buffer = []
                    if '_' in s_key:
                        (_, s_arch) = s_key.split('_', 1)
                    for c_index in range(len(c_keys)):
                        c_key = c_keys[c_index]
                        c_arch = None
                        if '_' in c_key:
                            (_, c_arch) = c_key.split('_', 1)
                        if c_arch == s_arch:
                            c_keys_buffer.append(c_key)
                    c_keys_buffer.sort(key=lambda val: algos[val])
                    p = (s_key, tuple(c_keys_buffer), s_arch is not None)
                    pairs.append(p)
    return pairs

def aur_sources_iter(db, logger):
    algos = repobuilder.functions.get_package_checksum_algos().keys()
    algos = dict(zip(algos, range(len(algos))))
    with db:
        with contextlib.closing(db.cursor()) as cur:
            sql = r"""
SELECT
	pkginfo.pkgbase,
	pkginfo.pkgname,
	pkginfo.sources,
	pkginfo.checksums
FROM
	pkginfo
LEFT JOIN logs ON logs.pkgbase = pkginfo.pkgbase AND logs.reponame = 'aur'
WHERE
	pkginfo.reponame = 'aur'
	AND logs.pkgbase IS NULL
        AND pkginfo.sources != '{}'
        AND pkginfo.sources IS NOT NULL
            """
            res = cur.execute(sql)
            failed_packages = []
            for (pkgbase, pkgname, sources, checksums) in res.fetchall():
                sources = json.loads(sources)
                checksums = json.loads(checksums)
                filtered_checksums = {}
                for algo, algo_c_list in checksums.items():
                    v_buffer = []
                    for c in algo_c_list:
                        #if c in ['SKIP']:
                        #    continue
                        v_buffer.append(c)
                    if len(v_buffer):
                        filtered_checksums[algo] = v_buffer
                checksums = filtered_checksums
                if len(sources) == 0 and len(checksums) == 0:
                    continue
                pairs = map_sources_and_checksum_keys(sources, checksums, algos, logger)
                try:
                    assert len(pairs), f"{len(pairs)=} {pkgbase=} {pkgname=} {sources=} {checksums=}"
                    logger.debug(f"{pkgbase=} {pkgname=} {pairs=}")
                    for src_key, checksum_keys, _ in pairs:
                        result_sources = sources[src_key]
                        result_checksums = [(k, checksums[k]) for k in checksum_keys]
                        yield (pkgbase, pkgname, src_key, result_sources, result_checksums)
                except:
                    failed_packages.append(pkgbase)
            logger.error(f"FAILED PACKAGES {len(failed_packages)=} {failed_packages=}")
            assert len(failed_packages) == 0

def iter_checksum_sources(db, logger):
    cache = {}
    prev_pkgbase = None
    for pkgbase, pkgname, src_key, sources, checksums in aur_sources_iter(db, logger):
        if prev_pkgbase and pkgbase != prev_pkgbase:
            data_flush = cache[prev_pkgbase]
            del cache[prev_pkgbase]
            yield (prev_pkgbase, data_flush)
        if not pkgbase:
            continue
        if pkgbase not in cache:
            cache[pkgbase] = {}
        for src_pos, src in enumerate(sources):
            if src in cache[pkgbase]:
                continue
            src_checksums = {}
            for algo, checksum_list in checksums:
                if src_pos < len(checksum_list):
                    checksum = checksum_list[src_pos]
                else:
                    checksum = ['SKIP']
                src_checksums[algo] = checksum
            cache[pkgbase][src] = src_checksums
        prev_pkgbase = pkgbase
    yield (pkgbase, cache[pkgbase])

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)

    db_file = pathlib.Path(MINICLUSTER.ARGS.db_file)

    existing_db = db_file.exists()
    if not existing_db:
        raise Exception("db file does not exist")
    db = sqlite3.connect(db_file, check_same_thread=False)

    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True

    schemes = collections.defaultdict(int)
    # current histo: {'git+https': 30436, 'https': 60653, '': 36050, 'http': 12303, 'ftp': 452, 'manual': 45, 'git+git': 194, 'hg+https': 117, 'local': 199, 'gogdownloader': 101, 'git': 1955, 'svn+https': 123, 'file': 1077, 'git+ssh': 16, 'bzr+https': 18, 'hg+http': 47, 'git+http': 210, 'svn': 78, 'bzr+http': 16, 'gog': 59, 'fossil+https': 11, 'hib': 123, 'svn+svn': 13, 'svn+http': 66, 'localfile': 2007, 'bzr+lp': 21, 'scp': 1, 'nxp': 4, 'wosign': 1, 'getceleste': 1, 'getfmod': 1, 'celeste': 1, 'error': 3, 'https+git': 1, 'fusion-studio': 1, 'gogicon': 2, 'humblestore': 1, 'humble': 2, 'moddb': 3, 'nssdfile': 1, 'git-lfs+https': 1, 'sgdcfile': 1, 'snap': 2, 'ipfs': 2, 'yandex': 5, 'fossil+http': 1, 'te4': 3, 'rsync': 3}
    total_est = 0
    hostnames = collections.defaultdict(int)
    for pkgbase, data in iter_checksum_sources(db, logger):
        for url, checksums in data.items():
            if '::' in url:
                local_fname, url = url.split('::', 1)
            o = urlparse(url)
            schemes[o.scheme] += 1
            if not o.hostname:
                continue
            hostnames[o.hostname] += 1
            if o.scheme in ['http', 'https']:
                total_est += 1
                time.sleep(0.050)
                logger.info(f"REQUESTING {total_est}/72947 {pkgbase=} {url}")
                try:
                    with requests.get(url, allow_redirects=True, stream=True, timeout=1) as res:
                        logger.info(f"{pkgbase=} {url=} {res.status_code=}")
                        assert res.status_code in [200, ]
                except:
                    logger.exception(f"FAILED GET {pkgbase=} {url=}")
    logger.info(f"{len(schemes)=} {schemes=}")
    hostnames = {k:v for k,v in hostnames.items() if v > 2000}
    logger.info(f"{len(hostnames)=} {hostnames=}")
    logger.info(f"{total_est=}")
