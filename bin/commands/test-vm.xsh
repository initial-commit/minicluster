#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.qmp
import json

def command_test_vm_xsh(cwd, logger, name):
    reached_targets = []
    mounted = []
    finished = []
    inserted_module = [] # Inserted module
    skipped_unmet_conditions = []
    started = []
    kernel = []
    missed_kernel = []
    modprobe = []
    startup_finished = []

    s = f"/tmp/qga-{name}.sock"
    c = cluster.qmp.Connection(s, logger)
    status = c.guest_exec_wait('journalctl --boot --lines=all -o export --output=json')
    # TODO: assert partitions matching the disk spec
    lines = status['out-data'].splitlines()
    for idx, line in enumerate(lines):
        line = json.loads(line)
        handled = False
        if line['MESSAGE'].startswith('Reached target '):
            handled = True
            reached_targets.append(idx)
        if line['MESSAGE'].startswith('Mounted ') or line['MESSAGE'].startswith('Mounting '):
            handled = True
            mounted.append(idx)
        if line['MESSAGE'].startswith('Finished '):
            handled = True
            finished.append(idx)
        if line['MESSAGE'].startswith('Inserted module '):
            handled = True
            inserted_module.append(idx)
        if line['MESSAGE'].startswith('Started ') or line['MESSAGE'].startswith('Starting '):
            handled = True
            started.append(idx)
        if ' was skipped because of an unmet condition check ' in line['MESSAGE']:
            handled = True
            skipped_unmet_conditions.append(idx)
        if line['SYSLOG_IDENTIFIER'] == 'kernel' and line['_TRANSPORT'] == 'kernel':
            handled = True
            kernel.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'systemd-journald.service' and line['MESSAGE'].startswith('Missed '):
            handled = True
            missed_kernel.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'init.scope' and line['MESSAGE'].startswith('modprobe@'):
            handled = True
            modprobe.append(idx)
        if line['MESSAGE'].startswith('Startup finished '):
            handled = True
            startup_finished.append(idx)

        if not handled:
            print("\t", idx, line['MESSAGE'])
            print(line)

    tagged = {
        'reached_targets': reached_targets,
        'mounted': mounted,
        'finished': finished,
        'inserted_module': inserted_module,
        'skipped_unmet_conditions': skipped_unmet_conditions,
        'started': started,
        'kernel': kernel,
        'missed_kernel': missed_kernel,
        'modprobe': modprobe,
        'startup_finished': startup_finished,
    }
    counts = { k: len(v) for k,v in tagged.items() }
    print(f"{counts=}")
    assert(len(reached_targets) == 12)
    assert(len(finished) == 24)
    assert(len(started) == 31)
    assert(len(startup_finished) == 1)
    #status = c.guest_exec_wait('dmesg -x -k -J --time-format iso -T -c')
    #print(status['out-data'])

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    name = MINICLUSTER.ARGS.name
    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True
    command_test_vm_xsh(cwd, logger, name)
