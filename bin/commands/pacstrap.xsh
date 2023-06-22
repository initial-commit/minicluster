#!/usr/bin/env xonsh

# While arch provides a pacstrap command, at the time of writing it has bugs

import time

r=$ARG1
r=pf"{r}".resolve(strict=False)
pkgs=$ARGS[2:]

#$RAISE_SUBPROC_ERROR = True
#XONSH_TRACE_SUBPROC = True
#trace on

#TODO: turn into a reusable command

unshare_pid=("--fork", "--pid", "--mount-proc",)
unshare_mount=("--mount", "--map-auto", "--map-root-user", "--setuid", "0", "--setgid", "0")

curl -o archlinux-bootstrap-x86_64.tar.gz -C - https://geo.mirror.pkgbuild.com/iso/latest/archlinux-bootstrap-x86_64.tar.gz
curl -o archlinux-bootstrap-x86_64.tar.gz.sig -C - https://geo.mirror.pkgbuild.com/iso/latest/archlinux-bootstrap-x86_64.tar.gz.sig

sq --force wkd get pierre@archlinux.org -o release-key.pgp

sq verify --signer-file release-key.pgp --detached archlinux-bootstrap-x86_64.tar.gz.sig archlinux-bootstrap-x86_64.tar.gz

unshare @(unshare_pid) @(unshare_mount) tar hxfz archlinux-bootstrap-x86_64.tar.gz --no-same-owner --no-same-permissions root.x86_64/etc/pacman.conf --warning=no-unknown-keyword
unshare @(unshare_pid) @(unshare_mount) tar hxfz archlinux-bootstrap-x86_64.tar.gz --no-same-owner --no-same-permissions root.x86_64/etc/pacman.d --warning=no-unknown-keyword
unshare @(unshare_pid) @(unshare_mount) tar hxfz archlinux-bootstrap-x86_64.tar.gz --no-same-owner --no-same-permissions root.x86_64/usr/share/pacman/keyrings --warning=no-unknown-keyword

sed -i 's/^#Server = /Server = /g' root.x86_64/etc/pacman.d/mirrorlist

mkdir -p @(pf"{r}/")
mkdir -m 0755 -p pf"{r}/var/cache/pacman/pkg"
mkdir -m 0755 -p pf"{r}/var/lib/pacman"
mkdir -m 0755 -p pf"{r}/var/log"
mkdir -m 0755 -p pf"{r}/dev"
mkdir -m 0755 -p pf"{r}/run"
mkdir -m 0755 -p pf"{r}/etc/pacman.d"
mkdir -m 1777 -p pf"{r}/tmp"
mkdir -m 0555 -p pf"{r}/sys"
mkdir -m 0555 -p pf"{r}/proc"

if pf"{r}/etc/pacman.d/gnupg".exists():
    rm -rf pf"{r}/etc/pacman.d/gnupg"


def bind_device(r, d):
    touch @(pf"{r}/dev/{d}")
    unshare @(unshare_pid) @(unshare_mount) mount @(pf"/dev/{d}") @(pf"{r}/dev/{d}") --bind

unshare @(unshare_pid) @(unshare_mount) mount @(r) @(r) --bind
unshare @(unshare_pid) @(unshare_mount) mount proc @(pf"{r}/proc") -t proc -o nosuid,noexec,nodev
unshare @(unshare_pid) @(unshare_mount) mount /sys @(pf"{r}/sys") --rbind
unshare @(unshare_pid) @(unshare_mount) ln -sf @(pf"{r}/proc/self/fd") @(pf"{r}/dev/fd")
unshare @(unshare_pid) @(unshare_mount) ln -sf @(pf"{r}/proc/self/fd/0") @(pf"{r}/dev/stdin")
unshare @(unshare_pid) @(unshare_mount) ln -sf @(pf"{r}/proc/self/fd/1") @(pf"{r}/dev/stout")
unshare @(unshare_pid) @(unshare_mount) ln -sf @(pf"{r}/proc/self/fd/2") @(pf"{r}/dev/sterr")
bind_device(r, "full")
bind_device(r, "null")
bind_device(r, "random")
bind_device(r, "tty")
bind_device(r, "random")
bind_device(r, "tty")
bind_device(r, "urandom")
bind_device(r, "zero")
unshare @(unshare_pid) @(unshare_mount) mount run @(pf"{r}/run") -t tmpfs -o nosuid,nodev,mode=0755
unshare @(unshare_pid) @(unshare_mount) mount tmp @(pf"{r}/tmp") -t tmpfs -o mode=1777,strictatime,nodev,nosuid

cp -a root.x86_64/* @(pf"{r}/")

unshare @(unshare_pid) @(unshare_mount) /usr/bin/pacman-key --config @(pf"{r}/etc/pacman.conf") --gpgdir @(pf"{r}/etc/pacman.d/gnupg") --init
unshare @(unshare_pid) @(unshare_mount) pacman-key --config @(pf"{r}/etc/pacman.conf") --gpgdir @(pf"{r}/etc/pacman.d/gnupg") --populate


ls -ltrah @(pf"{r}/var/cache/pacman/pkg")

unshare @(unshare_pid) @(unshare_mount) pacman --verbose -Sy --overwrite "*" -r @(r) --noconfirm --cachedir @(pf"{r}/var/cache/pacman/pkg") --hookdir @(pf"{r}/usr/share/libalpm/hooks") --gpgdir @(pf"{r}/etc/pacman.d/gnupg") --config root.x86_64/etc/pacman.conf @(pkgs)
