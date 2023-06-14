#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import sys

import os
import time
import json
import logging
import cluster.functions
import re
import shlex

def parse_size(size):
	if not isinstance(size, str):
		return size
	units = {'B': 1, 'KB': 2**10, 'MB': 2**20, 'GB': 2**30, 'TB': 2**40}
	size = size.upper()
	if not re.match(r' ', size):
		size = re.sub(r'([KMGT]?B)', r' \1', size)
	number, unit = [string.strip() for string in size.split()]
	return int(float(number)*units[unit])

def calculate_disk_sectors(d):
	parts = []
	prev_end_sector = 0
	size_covered = 0
	size_total = parse_size(d['size'])
	sector = d['sector']
	sector_total = int(size_total / sector)
	max_sector = sector_total - 1
	logger.info(f"{sector_total=} {max_sector=}")
	covered = 0
	first = True
	last = False
	for i, p in enumerate(d['partitions']):
		if i == len(d['partitions'])-1:
			last = True
		start_sector = int(parse_size(p['start']) / sector)
		size_sector = int(parse_size(p['size']) / sector)
		logger.info(f"{size_sector=}")
		if size_sector < 1 and last:
			end_sector = max_sector
		else:
			end_sector = start_sector + size_sector
		if start_sector == prev_end_sector:
			start_sector += 1
		if last:
			end_sector = end_sector-33 # mbr to gpt conversion causes last 33 sectors to be needed by gpt (given size 512)
		# sector calculation done
		p['start_sector'] = start_sector
		p['end_sector'] = end_sector
		parts.append(p)
		if p['mountpoint'] == '/':
			d['root_part_index'] = i+1

		# before loop end
		logger.info(f"====== {i=} {start_sector=} {end_sector=} {prev_end_sector=}")
		prev_end_sector = end_sector
		first = False
	d['partitions'] = parts

def command_make_empty_image_xsh(cwd, logger, handle, d):
    disk_file = f"{cwd}/{handle}.qcow2"
    mountpoint = f"{cwd}/{handle}"
    rm -rf @(disk_file)
    calculate_disk_sectors(d)
    out=$(qemu-img create -f qcow2 @(disk_file) 10G).rstrip()
    logger.info(out)

    guestfish_pid=$(guestfish --listen)
    guestfish_pid=re.findall(r'[0-9]+', guestfish_pid)
    guestfish_pid=int(guestfish_pid[0])

    commands = create_bootstrap_guestfish_commands(disk_file, d)

    for c in commands:
        logger.info(c)
        guestfish @(f'--remote={guestfish_pid}') @(shlex.split(c))

    raw=$(virt-filesystems --filesystems --uuids --long --csv -a @(disk_file)  | tail -n +2 | cut -d, -f1,3,7).splitlines()
    fstab_lines = []
    for i, uid_raw in enumerate(raw):
        uid_raw=uid_raw.split(',')
        p_data = d['partitions'][i]
        mnt = p_data['mountpoint']
        t=uid_raw[1]
        uuid=uid_raw[2]
        fsck_ord = 1 if mnt == '/' else 2
        line = f"UUID={uuid}\t{mnt}\t{t}\trw,relatime\t0\t{fsck_ord}"
        fstab_lines.append(line)

    with open(f"/{cwd}/fstab-{handle}", "w") as f:
        f.write("\n".join(fstab_lines))


def create_bootstrap_guestfish_commands(disk_file, d):
    commands = [
        f"add-drive {disk_file}",
        "run",
        f"part-init /dev/sda {d['type']}",
    ]

    part_commands = []
    for i, p in enumerate(d['partitions']):
        current_idx = i+1
        c = f"part-add /dev/sda primary {p['start_sector']} {p['end_sector']}"
        part_commands.append(c)
        if 'bootable' in p and p['bootable']:
            part_commands.append(f"part-set-bootable /dev/sda {current_idx} true")
        c = f"mkfs {p['fs']} /dev/sda{current_idx} blocksize:4096"
        part_commands.append(c)

    commands.extend(part_commands)
    commands.append(f"mount /dev/sda{d['root_part_index']} /")
    for i, p in enumerate(d['partitions']):
        if p['mountpoint'] == '/':
            continue
        current_idx = i+1
        c = f"mkdir {p['mountpoint']}"
        commands.append(c)
        c = f"mount /dev/sda{current_idx} {p['mountpoint']}"
        commands.append(c)

    commands.extend(["sync", "shutdown", "quit"])
    return commands

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle


    d = {
        'type': 'mbr',
        'size': '10GB',
        'sector': 512,
        'partitions': [
            {'type': 'primary', 'start': '1MB', 'size': '199MB', 'bootable': True, 'fs': 'ext4', 'mountpoint': '/boot', },
            {'type': 'primary', 'start': '200MB', 'size': -1, 'fs': 'ext4', 'mountpoint': '/', },
        ],
    }
    $RAISE_SUBPROC_ERROR = True
    command_make_empty_image_xsh(cwd, logger, handle, d)

