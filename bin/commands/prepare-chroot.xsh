#!/usr/bin/env xonsh

d = pf"{__file__}".resolve().parent

if __name__ == '__main__':
    source f'{d}/bootstrap.xsh'
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--cache', action="store_true", default=False, help="Use local package cache")
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.functions
import os
import sys
import time
import re
import shlex
import json

source @(f'{d}/rank-mirrors.xsh')

def command_prepare_chroot_xsh(cwd, logger, handle, cache):
    # TODO: list of packages to bootstrap from should be a parameter
    # TODO: from those packages, extract the names of the repositories instead of hard-coding them here:
    repositories = ["core", "extra"]
    mountpoint = f"{cwd}/{handle}"
    disk_file = f"{cwd}/{handle}.qcow2"
    unshare_pid=("--fork", "--pid", "--mount-proc",)
    unshare_mount=("--mount", "--map-auto", "--map-root-user", "--setuid", "0", "--setgid", "0")
    mirror_cache = fp"{cwd}/mirrors.json".resolve()
    if mirror_cache.exists():
        with mirror_cache.open() as f:
            mirrors = json.load(f)
    else:
        logger.info(f"mirror cache not found, ranking mirrors first")
        mirrors = command_get_ranked_mirrors_xsh(logger)

    fastest_mirror = mirrors[-1]['url']
    logger.info(f"using mirror {fastest_mirror=}")

    if not pf"archlinux-bootstrap-x86_64.tar.gz".exists():
        curl -o archlinux-bootstrap-x86_64.tar.gz -C - @(fastest_mirror)/iso/latest/archlinux-bootstrap-x86_64.tar.gz
        curl -o archlinux-bootstrap-x86_64.tar.gz.sig -C - @(fastest_mirror)/iso/latest/archlinux-bootstrap-x86_64.tar.gz.sig
        sq --force wkd get pierre@archlinux.org -o release-key.pgp
        sq verify --signer-file release-key.pgp --detached archlinux-bootstrap-x86_64.tar.gz.sig archlinux-bootstrap-x86_64.tar.gz
    if not pf"archlinux-bootstrap-x86_64.tar".exists():
        gunzip -c archlinux-bootstrap-x86_64.tar.gz > archlinux-bootstrap-x86_64.tar
        # These symlinks just create problems. We just pick up some files from the bootstrap and use it to generate the archlinux keyring but we don't have much use for this image other than that
        # This approach makes the whole system more self-contained and isolated
        # Failed solution: playing with pacstrap's options to initialize the keyring does not work in the grand scheme of things because of bugs, hardcoded paths in pacman-key and the like
        tar --delete -f archlinux-bootstrap-x86_64.tar 'root.x86_64/etc/ca-certificates/extracted/cadir/'
    #if not pf"{cwd}/root.x86_64".exists():
    #    unshare @(unshare_pid) @(unshare_mount) tar hxf archlinux-bootstrap-x86_64.tar --no-same-owner --no-same-permissions --warning=no-unknown-keyword

    # START: prepare root
    r=pf"{cwd}/root.x86_64"
    if pf"{r}".exists():
        rm -rf pf"{r}"

    unshare @(unshare_pid) @(unshare_mount) tar hxf archlinux-bootstrap-x86_64.tar --no-same-owner --no-same-permissions --warning=no-unknown-keyword

    with open(f"{r}/etc/pacman.d/mirrorlist", 'w') as f:
        for srvspec in mirrors:
            srvspec['local_download_speed'] = round(srvspec['local_download_speed']/1024/1024, 2)
            f.write("# speed: {local_download_speed} MB/s\n".format(**srvspec))
            f.write("Server = {url}$repo/os/$arch\n".format(**srvspec))
    #sed -i 's/^#Server = /Server = /g' root.x86_64/etc/pacman.d/mirrorlist

    #unshare @(unshare_pid) @(unshare_mount) mount @(r) @(r) --bind
    #unshare @(unshare_pid) @(unshare_mount) mount proc @(pf"{r}/proc") -t proc -o nosuid,noexec,nodev
    #unshare @(unshare_pid) @(unshare_mount) mount /sys @(pf"{r}/sys") --rbind
    ln -sf @(pf"{r}/proc/self/fd") @(pf"{r}/dev/fd")
    ln -sf @(pf"{r}/proc/self/fd/0") @(pf"{r}/dev/stdin")
    ln -sf @(pf"{r}/proc/self/fd/1") @(pf"{r}/dev/stout")
    ln -sf @(pf"{r}/proc/self/fd/2") @(pf"{r}/dev/sterr")

    cp -a @(MINICLUSTER.DIR_R)/bootstrap-overlay/tmp/bootstrap-rootimage.sh @(r)/
    if cache:
        prepare_overlay_commands = [
            ("mkdir", "-p", "var/lib/pacman/sync/"),
            ("cp", "/var/lib/pacman/sync/core.db", "./var/lib/pacman/sync/"),
            ("cp", "/var/lib/pacman/sync/extra.db", "./var/lib/pacman/sync/"),
            ("find", "/var/cache/pacman/pkg/", "-name", "archlinux-keyring-*-any.pkg.tar.zst", "-exec", "cp", "{}", "./var/cache/pacman/pkg/", ";"),
        ]
        #$DOTGLOB = True
        for c in prepare_overlay_commands:
            echo unshare ... -w @(r) @(c)
            unshare --fork --pid --mount-proc --kill-child=SIGTERM --map-auto --map-root-user --setuid 0 --setgid 0 -w @(r) @(c)
        $DOTGLOB = False
    unshare --fork --pid --mount-proc --kill-child=SIGTERM --map-auto --map-root-user --setuid 0 --setgid 0 -w @(r) env -i ./bootstrap-rootimage.sh
    # END: root prepared

    cp -a @(MINICLUSTER.DIR_R)/bootstrap-overlay @(cwd)/
    cp -a @(r)/etc/pacman.d/mirrorlist @(cwd)/bootstrap-overlay/etc/pacman.d/
    cp -a @(r)/etc/pacman.conf @(cwd)/bootstrap-overlay/etc/
    cp -a @(r)/var/cache/pacman/pkg/* @(cwd)/bootstrap-overlay/var/cache/pacman/pkg/
    cp -a @(r)/var/lib/pacman/sync/* @(cwd)/bootstrap-overlay/var/lib/pacman/sync/
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

    rm -rf @(r)

    find @(cwd)/bootstrap-overlay/ -type f -name '.keep' -delete

    # TODO: mount here instead of outside

    # TODO: overlays from the project
    cp -a @(cwd)/bootstrap-overlay/* @(mountpoint)/
    rm -rf @(cwd)/bootstrap-overlay
    cp @(f"{cwd}/fstab-{handle}") @(mountpoint)/etc/fstab

    cp /etc/resolv.conf @(mountpoint)/etc/
    #unshare @(unshare_pid) @(unshare_mount) pacstrap.xsh @(mountpoint) archlinux-keyring
    unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) mkdir -p @(mountpoint)/var/cache/pacman/pkg/
    pacstrap_flags = ''
    if cache:
        unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) rsync -az /var/cache/pacman/pkg/ @(mountpoint)/var/cache/pacman/pkg/
        #unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) rsync -azp --progress /var/cache/pacman/pkg/ @(mountpoint)/var/cache/pacman/pkg/
        unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) sync
        pacstrap_flags = '--cache'
    else:
        ping -c 1 8.8.8.8 -w 1
        for repo in repositories:
            unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) wget -O @(f"{mountpoint}/var/cache/pacman/pkg/{repo}.files.tar.gz") https://geo.mirror.pkgbuild.com/@(repo)/os/x86_64/@(repo).files.tar.gz
    unshare @(unshare_pid) @(unshare_mount) -w @(mountpoint) pacstrap.xsh @(mountpoint) @(pacstrap_flags) base linux mkinitcpio linux-firmware qemu-guest-agent

    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/umount-image.xsh'
    command_umount_image_xsh(cwd, logger, handle)

    guestfish_pid=$(guestfish --listen --network -a @(disk_file))
    guestfish_pid=re.findall(r'[0-9]+', guestfish_pid)
    guestfish_pid=int(guestfish_pid[0])
    logger.info(f"{guestfish_pid=}")

    #TODO: get values from minicluster spec
    tz="Europe/Vienna"
    locale="en_US.UTF-8 UTF-8"
    lang="LANG=en_US.UTF-8"
    hostname = handle

    e_tmp_dir = f"{cwd}/extracted-tmp-{handle}"
    mkdir -p @(e_tmp_dir)

    commands = [
        ["set-trace", "false"],
        ["set-verbose", "false"],
        ["set-memsize", "1024"],
        ["set-smp", "2"],
        ["set-pgroup", "false"],
        ["time", "run"],
        ["mount", "/dev/sda2", "/"],
        ["mount", "/dev/sda1", "/boot"],
        ["time", "command", "pacman-key --init"],
        ["time", "command", "pacman-key --populate archlinux"],
        (["time", "command", "pacman -Syy --noconfirm"], None),
        #["time", "command", "pacman --noconfirm -S base linux grub mkinitcpio qemu-guest-agent linux-headers linux-firmware audit qemu-base arch-install-scripts"],
        #["sh-lines", "genfstab -U / | grep -vw '# ' | sed '/^$/d' | sed 's/sd/vd/g' > /etc/fstab"], #TODO: use only uuids and get rid of this
        ["time", "command", "pacman --noconfirm -S linux"], # reinstallation seems to be necessary (?)
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
        ["time", "sh-lines", "yes | pacman -S iptables-nft"],
        ["command", "systemctl enable serial-getty@ttyS0.service",],
        ["command", "systemctl enable systemd-networkd.service",],
        ["command", "systemctl enable systemd-resolved",],
        ["command", "systemctl enable qemu-guest-agent",],
        #["command", "systemctl enable auditd.service",],
        #["command", "findmnt",],
        ["time", "sh-lines", 'echo -e "shopt -s extglob\nchown -R root:root /!(sys|proc|run|boot)" | bash',],
        #TODO: do we need to exclude boot in the line above?
        ["command", "systemd-tmpfiles --create --clean --remove --boot"],
        ["time", "sync"],
        #["sleep", "60"],
        ["time", "drop-caches", "3"],
        #TODO: copy to var path of project
        ["copy-out", "/boot/initramfs-linux.img", "/boot/vmlinuz-linux", f"{e_tmp_dir}/"],
        #TODO: umount based on the layout in the spec
        ["umount", "/boot"],
        ["-umount", "/"],
        ["-shutdown"],
        ["exit"],
    ]

    for c in commands:
        if isinstance(c, tuple):
            if cache:
                c = c[1]
            else:
                c = c[0]
        if not c:
            continue
        logger.info(f"{c=}")
        p=![guestfish @(f'--remote={guestfish_pid}') -- @(c)]
        code = p.rtn
        logger.info(f"{code=} {c=}")
        if code != 0:
            return False
        #time.sleep(1)
        #TODO: handle exit code
    mv @(f"{e_tmp_dir}/initramfs-linux.img") @(f"{cwd}/{handle}-initramfs-linux.img")
    mv @(f"{e_tmp_dir}/vmlinuz-linux") @(f"{cwd}/{handle}-vmlinuz-linux")
    rmdir @(e_tmp_dir)
    return True

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle
    cache = MINICLUSTER.ARGS.cache
    $RAISE_SUBPROC_ERROR = True
    command_prepare_chroot_xsh(cwd, logger, handle, cache)

