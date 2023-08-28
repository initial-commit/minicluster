#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    from cluster.functions import str2bool_exc as strtobool
    MINICLUSTER.ARGPARSE.add_argument('--db_names', default=[], nargs='+', required=False, help="The database names: core, extra, aur, custom, ...")
    MINICLUSTER.ARGPARSE.add_argument('--db_file', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import pathlib
import sqlite3
import contextlib
import re
import json
from datetime import datetime
from urllib.parse import urlparse
from collections import defaultdict


if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    db_file = pathlib.Path(MINICLUSTER.ARGS.db_file)
    db_names = MINICLUSTER.ARGS.db_names
    db = sqlite3.connect(db_file)

    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True

    schemes = defaultdict(int)
    with contextlib.closing(db.cursor()) as cur:
        res = cur.execute('SELECT pkgname, pkgid, pkgbase, source FROM pkginfo WHERE reponame IN (?)', db_names)
        for (pkgname, pkgid, pkgbase, src_raw) in res.fetchall():
            if not src_raw:
                continue
            src_raw = json.loads(src_raw)
            for src_key, src_list in src_raw.items():
                for src_val in src_list:
                    local_name = None
                    if '::' in src_val:
                        local_name = src_val.split('::', 1)[0]
                        src_val = src_val.split('::', 1)[1]
                    url_p = urlparse(src_val)
                    schemes[url_p.scheme] += 1
                    if url_p.scheme in ['git+https', '', 'https', 'http', 'ftp', 'git', ]:
                        continue
                    print(pkgid, src_key, src_val, url_p)
                    # scheme gogdownloader must have lgogdownloader in makedepends
                    # scheme nxp needs nxp-dlagent
                    #
    print(schemes)
