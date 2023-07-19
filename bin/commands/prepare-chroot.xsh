#!/usr/bin/env xonsh

if __name__ == '__main__':
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

def command_prepare_chroot_xsh(cwd, logger, handle, cache):
    mountpoint = f"{cwd}/{handle}"
    disk_file = f"{cwd}/{handle}.qcow2"
    unshare_pid=("--fork", "--pid", "--mount-proc",)
    unshare_mount=("--mount", "--map-auto", "--map-root-user", "--setuid", "0", "--setgid", "0")

    if not pf"archlinux-bootstrap-x86_64.tar".exists():
        #TODO: fetch the mirrors as json from archlinux and use that information
        curl -o archlinux-bootstrap-x86_64.tar.gz -C - https://geo.mirror.pkgbuild.com/iso/latest/archlinux-bootstrap-x86_64.tar.gz
        curl -o archlinux-bootstrap-x86_64.tar.gz.sig -C - https://geo.mirror.pkgbuild.com/iso/latest/archlinux-bootstrap-x86_64.tar.gz.sig
        sq --force wkd get pierre@archlinux.org -o release-key.pgp
    sq verify --signer-file release-key.pgp --detached archlinux-bootstrap-x86_64.tar.gz.sig archlinux-bootstrap-x86_64.tar.gz
    if not pf"archlinux-bootstrap-x86_64.tar".exists():
        gunzip -c archlinux-bootstrap-x86_64.tar.gz > archlinux-bootstrap-x86_64.tar
        # These symlinks just create problems. We just pick up some files from the bootstrap and use it to generate the archlinux keyring but we don't have much use for this image other than that
        # This approach makes the whole system more self-contained and isolated
        # Failed solution: playing with pacstrap's options to initialize the keyring does not work in the grand scheme of things because of bugs, hardcoded paths in pacman-key and the like
        tar --delete -f archlinux-bootstrap-x86_64.tar 'root.x86_64/etc/ca-certificates/extracted/cadir/'
        unshare @(unshare_pid) @(unshare_mount) tar hxf archlinux-bootstrap-x86_64.tar --no-same-owner --no-same-permissions --warning=no-unknown-keyword

    # START: prepare root
    r=pf"{cwd}/root.x86_64"
    if pf"{r}".exists():
        rm -rf pf"{r}"

    unshare @(unshare_pid) @(unshare_mount) tar hxf archlinux-bootstrap-x86_64.tar --no-same-owner --no-same-permissions --warning=no-unknown-keyword

    sed -i 's/^#Server = /Server = /g' root.x86_64/etc/pacman.d/mirrorlist

    unshare @(unshare_pid) @(unshare_mount) mount @(r) @(r) --bind
    unshare @(unshare_pid) @(unshare_mount) mount proc @(pf"{r}/proc") -t proc -o nosuid,noexec,nodev
    unshare @(unshare_pid) @(unshare_mount) mount /sys @(pf"{r}/sys") --rbind
    ln -sf @(pf"{r}/proc/self/fd") @(pf"{r}/dev/fd")
    ln -sf @(pf"{r}/proc/self/fd/0") @(pf"{r}/dev/stdin")
    ln -sf @(pf"{r}/proc/self/fd/1") @(pf"{r}/dev/stout")
    ln -sf @(pf"{r}/proc/self/fd/2") @(pf"{r}/dev/sterr")

    cp -a @(MINICLUSTER.DIR_R)/bootstrap-overlay/tmp/bootstrap-rootimage.sh @(r)/
    unshare --fork --pid --mount-proc --kill-child=SIGTERM --map-auto --map-root-user --setuid 0 --setgid 0 -w @(r) env -i ./bootstrap-rootimage.sh
    # END: root prepared

    cp -a @(MINICLUSTER.DIR_R)/bootstrap-overlay @(cwd)/
    cp -a @(r)/etc/pacman.d/mirrorlist @(cwd)/bootstrap-overlay/etc/pacman.d/
    cp -a @(r)/etc/pacman.conf @(cwd)/bootstrap-overlay/etc/
    cp -a @(r)/var/cache/pacman/pkg/* @(cwd)/bootstrap-overlay/var/cache/pacman/pkg/
    #cp -a @(cwd)/bootstrap-overlay/* @(cwd)/
    gnupg_files_to_copy = [
        'gpg-agent.conf',
        'gpg.conf',
        'openpgp-revocs.d',
        'private-keys-v1.d',
        'pubring.gpg',
        'secring.gpg',
        'tofu.db',
        'trustdb.gpg',
    ]
    for f in gnupg_files_to_copy:
        echo cp -a @(r)/etc/pacman.d/gnupg/@(f) @(cwd)/bootstrap-overlay/etc/pacman.d/gnupg/@(f)
        cp -a @(r)/etc/pacman.d/gnupg/@(f) @(cwd)/bootstrap-overlay/etc/pacman.d/gnupg/@(f)

    find @(cwd)/bootstrap-overlay/ -type f -name '.keep' -delete

    # TODO: mount here instead of outside

    # TODO: overlays from the project
    echo cp -a @(cwd)/bootstrap-overlay/* @(mountpoint)/
    cp -a @(cwd)/bootstrap-overlay/* @(mountpoint)/
    echo ls -ltrah @(mountpoint)/etc/pacman.d/gnupg/
    ls -ltrah @(mountpoint)/etc/pacman.d/gnupg/
    cp @(f"{cwd}/fstab-{handle}") @(mountpoint)/etc/fstab

    cp /etc/resolv.conf @(mountpoint)/etc/
    #unshare @(unshare_pid) @(unshare_mount) pacstrap.xsh @(mountpoint) archlinux-keyring
    unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) mkdir -p @(mountpoint)/var/cache/pacman/pkg/
    if cache:
        unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) rsync -azp --progress /var/cache/pacman/pkg/ @(mountpoint)/var/cache/pacman/pkg/
        unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) sync
    unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) pacstrap.xsh @(mountpoint) base linux mkinitcpio linux-firmware qemu-guest-agent python

    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/umount-image.xsh'
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
        ["set-memsize", "1024"],
        ["set-smp", "2"],
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
        #["time", "command", "syslinux-install_update -i -m -a"],
        #["time", "command", "grub-install --target=i386-pc --recheck /dev/sda"],
        #["time", "command", "grub-mkconfig -o /boot/grub/grub.cfg"],
        ["command", "passwd -d root"],
        ["time", "sh", "yes | pacman -S iptables-nft"],
        ["command", "systemctl enable serial-getty@ttyS0.service",],
        ["command", "systemctl enable systemd-networkd.service",],
        ["command", "systemctl enable systemd-resolved",],
        ["command", "systemctl enable qemu-guest-agent",],
        #["command", "systemctl enable auditd.service",],
        #["command", "findmnt",],
        ["time", "sh", 'echo -e "shopt -s extglob\nchown -R root:root /!(sys|proc|run|boot)" | bash',],
        #TODO: do we need to exclude boot in the line above?
        ["time", "systemd-tmpfiles --create --clean --remove --boot"],
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
        p=![guestfish -x @(f'--remote={guestfish_pid}') -- @(c)]
        code = p.rtn
        logger.info(f"{code=} {c=}")
        #TODO: handle exit code

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle
    $RAISE_SUBPROC_ERROR = True
    command_prepare_chroot_xsh(cwd, logger, handle)

