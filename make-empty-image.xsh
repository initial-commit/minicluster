#!/usr/bin/env xonsh

source 'functions.xsh'

import os
import sys
import time
import json

cwd = os.getcwd()

p = CommandArgumentParser()
p.add_argument('--handle')

logger = p.get_logger(__name__)
(args, unknown) = p.parse_known_args()
handle = args.handle

devices = nbd_block_devices()

nbd_dev = None

for d,i in devices.items():
	nbd_dev = d
	if len(i.mountpoints) == 0:
		break

disk_file = f"{cwd}/{handle}.qcow2"
nbd_dev = f"/dev/{nbd_dev}"
mountpoint = f"{cwd}/{handle}"
nbd_pidfile = f"/tmp/minicluster-nbd-pid-{handle}"

$RAISE_SUBPROC_ERROR = False
if os.path.isfile(nbd_pidfile):
	pid=$(lsof -t @(nbd_pidfile)).rstrip()
	kill -9 @(pid)

$RAISE_SUBPROC_ERROR = True

blk_info = json.loads($(lsblk -J --tree @(nbd_dev)))
for blk_dev in blk_info['blockdevices']:
	if 'children' in blk_dev:
		#TODO: loop each child and umount if necessary
		qemu-nbd --disconnect @(nbd_dev)

qemu-img create -f qcow2 @(disk_file) 32G
$RAISE_SUBPROC_ERROR = True
qemu-nbd --pid-file @(nbd_pidfile) --connect=@(nbd_dev) @(disk_file)
while not os.path.isfile(nbd_pidfile):
	logger.info("waiting for nbd to start")
	time.sleep(0.2)
parted --align optimal --script --machine --fix @(nbd_dev) -- mklabel msdos mkpart primary 1049kB 200MB set 1 boot on mkpart primary 200MB '100%'
p1 = f"{nbd_dev}p1"
p2 = f"{nbd_dev}p2"
mkfs.ext2 @(p1)
mkfs.ext4 @(p2)

mkdir -p @(mountpoint)
mount @(p2) @(mountpoint)
mkdir -p @(mountpoint)/boot
mount @(p1) @(mountpoint)/boot


sync -d @(p2)
sync -f @(p2)
sync -d @(p1)
sync -f @(p1)

umount @(mountpoint)/boot
umount @(mountpoint)
logger.info("disconnecting nbd")
qemu-nbd --disconnect @(nbd_dev)
logger.info("nbd disconnected")
$RAISE_SUBPROC_ERROR = False

if os.path.isfile(nbd_pidfile):
	rm @(nbd_pidfile)
