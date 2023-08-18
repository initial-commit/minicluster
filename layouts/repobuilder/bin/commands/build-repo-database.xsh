#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--aur_clone', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import pathlib
import pygit2
import repobuilder.functions


if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    aur_clone = pathlib.Path(MINICLUSTER.ARGS.aur_clone)
    if pf"{aur_clone}/.git".exists():
        aur_clone = aur_clone / '.git'

    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True

    repo = pygit2.Repository(aur_clone)
    i = 0
    for (pkgid, meta) in repobuilder.functions.aur_repo_iterator(repo):
        print(i, pkgid, meta)
        i += 1
