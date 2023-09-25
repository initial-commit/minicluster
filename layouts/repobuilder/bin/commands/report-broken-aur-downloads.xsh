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

def map_sources_and_checksum_keys(sources, checksums, logger):
    s_keys = list(sources.keys())
    c_keys = list(checksums.keys())
    pairs = []
    if len(s_keys) == 1 and len(c_keys) == 1:
        pairs.append((s_keys[0], (c_keys[0],)))
    else:
        if len(s_keys) == 1:
            p = (s_keys[0], tuple(c_keys))
            pairs.append(p)
        else:
            if len(s_keys) == len(c_keys):
                for i in range(len(s_keys)):
                    p = (s_keys[i], (c_keys[i],))
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
                    p = (s_key, tuple(c_keys_buffer))
                    pairs.append(p)
    return pairs

def aur_sources_iter(db, logger):
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
LEFT JOIN logs ON logs.pkgbase = pkginfo.pkgbase and logs.reponame = 'aur'
WHERE
	pkginfo.reponame = 'aur'
	and logs.pkgbase IS NULL
            """
            res = cur.execute(sql)
            for (pkgbase, pkgname, sources, checksums) in res.fetchall():
                sources = json.loads(sources)
                checksums = json.loads(checksums)
                filtered_checksums = {}
                for algo, algo_c_list in checksums.items():
                    v_buffer = []
                    for c in algo_c_list:
                        if c in ['SKIP']:
                            continue
                        v_buffer.append(c)
                    if len(v_buffer):
                        filtered_checksums[algo] = v_buffer
                checksums = filtered_checksums
                if len(sources) == 0 and len(checksums) == 0:
                    continue
                pairs = map_sources_and_checksum_keys(sources, checksums, logger)
                assert len(pairs), f"{len(pairs)=} {pkgbase=} {pkgname=} {sources=} {checksums=}"
                logger.info(f"{pkgbase=} {pkgname=} {pairs=}")


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
    aur_sources_iter(db, logger)
