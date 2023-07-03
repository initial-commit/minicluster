#!/usr/bin/env xonsh

# While arch provides a pacstrap command, at the time of writing it has bugs

import time

#TODO: trap

r=$ARG1
r=pf"{r}".resolve(strict=False)
pkgs=$ARGS[2:]

#$RAISE_SUBPROC_ERROR = True
#XONSH_TRACE_SUBPROC = True
#trace on

#TODO: turn into a reusable command

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

#if pf"{r}/etc/pacman.d/gnupg".exists():
#    rm -rf pf"{r}/etc/pacman.d/gnupg"


def bind_device(r, d):
    touch @(pf"{r}/dev/{d}")
    mount @(pf"/dev/{d}") @(pf"{r}/dev/{d}") --bind

#mount @(r) @(r) --bind
mount proc @(pf"{r}/proc") -t proc -o nosuid,noexec,nodev
mount /sys @(pf"{r}/sys") --rbind
ln -sf @(pf"{r}/proc/self/fd") @(pf"{r}/dev/fd")
ln -sf @(pf"{r}/proc/self/fd/0") @(pf"{r}/dev/stdin")
ln -sf @(pf"{r}/proc/self/fd/1") @(pf"{r}/dev/stout")
ln -sf @(pf"{r}/proc/self/fd/2") @(pf"{r}/dev/sterr")
bind_device(r, "full")
bind_device(r, "null")
bind_device(r, "random")
bind_device(r, "tty")
bind_device(r, "random")
bind_device(r, "tty")
bind_device(r, "urandom")
bind_device(r, "zero")
mount run @(pf"{r}/run") -t tmpfs -o nosuid,nodev,mode=0755
mount tmp @(pf"{r}/tmp") -t tmpfs -o mode=1777,strictatime,nodev,nosuid

ping -c 1 8.8.8.8 -w 1
ls -ltrah etc/pacman.d/
ls -ltrah etc/pacman.d/gnupg/

pacman --verbose -Syy --overwrite "*" -r @(r) --noconfirm --cachedir @(pf"{r}/var/cache/pacman/pkg") --hookdir @(pf"{r}/usr/share/libalpm/hooks") --gpgdir @(pf"{r}/etc/pacman.d/gnupg") --config @(pf"{r}/etc/pacman.conf")
pacman --verbose -S --overwrite "*" -r @(r) --noconfirm --cachedir @(pf"{r}/var/cache/pacman/pkg") --hookdir @(pf"{r}/usr/share/libalpm/hooks") --gpgdir @(pf"{r}/etc/pacman.d/gnupg") --config @(pf"{r}/etc/pacman.conf") @(pkgs)
