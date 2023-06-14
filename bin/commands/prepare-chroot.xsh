#!/usr/bin/env xonsh

d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.functions
import os
import sys
import time
import re
import shlex

source f'{d}/umount-image.xsh'

cwd = MINICLUSTER.CWD_START

logger = logging.getLogger(__name__)
handle = MINICLUSTER.ARGS.handle

mountpoint = f"{cwd}/{handle}"
disk_file = f"{cwd}/{handle}.qcow2"

# TODO: mount here

$RAISE_SUBPROC_ERROR = True

mkdir -p @(mountpoint)/var/cache/pacman/pkg/
mkdir -p @(mountpoint)/var/lib/pacman/
mkdir -p @(mountpoint)/etc/

#pacstrap -N -M -K -G @(mountpoint)/ base linux mkinitcpio grub linux-headers linux-firmware arch-install-scripts fuse2 os-prober
#pacstrap -N -M -K -G @(mountpoint)/ pacman archlinux-keyring base linux mkinitcpio grub linux-headers linux-firmware qemu-guest-agent audit qemu-base arch-install-scripts fuse2 os-prober
#TODO: list of packages to install
pacstrap -N -M -K -G @(mountpoint)/ base linux mkinitcpio syslinux linux-firmware qemu-guest-agent qemu-base arch-install-scripts

cp -a @(MINICLUSTER.DIR_R)/bootstrap-overlay/* @(mountpoint)/
echo cp @(f"/tmp/fstab-{handle}") @(mountpoint)/etc/
cp @(f"/tmp/fstab-{handle}") @(mountpoint)/etc/fstab

cp /etc/resolv.conf @(mountpoint)/etc/
sed -i 's/^#Server = /Server = /g'  @(mountpoint)/etc/pacman.d/mirrorlist

command_umount_image_xsh(cwd, logger, handle)

guestfish_pid=$(guestfish --listen --network -a @(disk_file) -x)
guestfish_pid=re.findall(r'[0-9]+', guestfish_pid)
guestfish_pid=int(guestfish_pid[0])
logger.info(f"{guestfish_pid=}")

#TODO: get values from minicluster spec
tz="Europe/Vienna"
locale="en_US.UTF-8 UTF-8"
lang="LANG=en_US.UTF-8"
hostname = handle

commands = [
	["set-memsize", "4096"],
	["set-smp", "4"],
	["set-pgroup", "false"],
	["time", "run"],
	["mount", "/dev/sda2", "/"],
	["mount", "/dev/sda1", "/boot"],
	["time", "command", "pacman-key --init"],
	["time", "command", "pacman-key --populate archlinux"],
	["time", "command", "pacman -Syy --noconfirm"],
	#["time", "command", "pacman --noconfirm -S base linux grub mkinitcpio qemu-guest-agent linux-headers linux-firmware audit qemu-base arch-install-scripts"],
	#["sh", "genfstab -U / | grep -vw '# ' | sed '/^$/d' | sed 's/sd/vd/g' > /etc/fstab"], #TODO: use only uuids and get rid of this
	["time", "command", "pacman --noconfirm -S linux"],
	["time", "sync"],
	["time", "drop-caches", "3"],
	["ln-sf", f"/usr/share/zoneinfo/{tz}", "/etc/localtime"],
	#["command", "hwclock", "--systohc", ],
	["write", "/etc/locale.gen", locale],
	["time", "command", "locale-gen"],
	["write", "/etc/locale.conf", lang],
	["write", "/etc/hostname", hostname],
	["time", "command", "mkinitcpio -P"],
	#["time", "mkdir-p", "/boot/grub"],
	["time", "command", "syslinux-install_update -i -m -a"],
	#["time", "command", "grub-install --target=i386-pc --recheck /dev/sda"],
	#["time", "command", "grub-mkconfig -o /boot/grub/grub.cfg"],
	["command", "passwd -d root"],
	["time", "sh", "yes | pacman -S iptables-nft"],
	["command", "systemctl enable serial-getty@ttyS0.service",],
	["command", "systemctl enable systemd-networkd.service",],
	["command", "systemctl enable systemd-resolved",],
	#["command", "systemctl enable auditd.service",],
	#["command", "findmnt",],
	["-time", "sh", "chown -R root:root /",],
	["time", "sync"],
	#["sleep", "60"],
	["time", "drop-caches", "3"],
	#TODO: copy to var path of project
	["copy-out", "/boot/initramfs-linux.img", "/boot/vmlinuz-linux", f"{cwd}/"],
	#TODO: umount based on the layout in the spec
	["umount", "/boot"],
	["-umount", "/"],
	["-shutdown"],
	["exit"],
]

for c in commands:
	logger.info(f"{c=}")
	time.sleep(1)
	guestfish -x @(f'--remote={guestfish_pid}') -- @(c)

sys.exit(0)

if os.path.exists(f"{mountpoint}/etc/resolv.conf.pacnew"):
	rm @(mountpoint)/etc/resolv.conf.pacnew

