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
    logging_messages = []
    init_scope = []
    fsck = []
    qemu = []
    networking = []
    login = []
    udev = []
    dbus = []
    untagged = []

    s = f"{cwd}/qga-{name}.sock"
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
        if line['SYSLOG_IDENTIFIER'] == 'systemd-fsck':
            handled = True
            fsck.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'systemd-journald.service' and line['MESSAGE'].startswith('Missed '):
            handled = True
            missed_kernel.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'systemd-journald.service':
            handled = True
            logging_messages.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'init.scope' and line['MESSAGE'].startswith('modprobe@'):
            handled = True
            modprobe.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'init.scope':
            handled = True
            init_scope.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'qemu-guest-agent.service':
            handled = True
            qemu.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'systemd-logind.service':
            handled = True
            login.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'systemd-udevd.service':
            handled = True
            udev.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] == 'dbus.service':
            handled = True
            dbus.append(idx)
        if '_SYSTEMD_UNIT' in line and line['_SYSTEMD_UNIT'] in ['systemd-networkd.service', 'systemd-resolved.service']:
            handled = True
            networking.append(idx)
        if line['MESSAGE'].startswith('Startup finished '):
            handled = True
            startup_finished.append(idx)

        if not handled:
            logger.warning(line)
            untagged.append(idx)

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
        'logging_messages': logging_messages,
        'init_scope': init_scope,
        'fsck': fsck,
        'qemu': qemu,
        'networking': networking,
        'login': login,
        'udev': udev,
        'dbus': dbus,
        'untagged': untagged,
    }
    counts = { k: len(v) for k,v in tagged.items() }
    logger.info(f"{counts=}")
    assert len(startup_finished) == 1, "startup finished"
    assert len(reached_targets) == 12, "reached targets"
    assert len(finished) >= 18, "finished targets"
    assert len(started) >= 25, "started units"
    assert len(untagged) == 0, "untagged journal lines"
    #TODO: test for no .pacnew files in /etc
    #TODO: test for all tmpfiles cleaned, see systemd-tmpfiles
    #status = c.guest_exec_wait('dmesg -x -k -J --time-format iso -T -c')
    #print(status['out-data'])

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    name = MINICLUSTER.ARGS.name
    $RAISE_SUBPROC_ERROR = True
    $XONSH_SHOW_TRACEBACK = True
    command_test_vm_xsh(cwd, logger, name)
