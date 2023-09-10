#!/usr/bin/env xonsh

# While arch provides a pacstrap command, at the time of writing it has bugs

import time
import argparse
import sys

argparser = argparse.ArgumentParser()
argparser.add_argument('--cache', action='store_true', default=False)
argparser.add_argument('--rootdir', required=True)
argparser.add_argument('--packages', nargs='+', required=True)
#args = argparser.parse_known_args(sys.argv)
args = argparser.parse_args(sys.argv[1:])

cache = args.cache
packages = args.packages
packages = list(filter(len, packages))
r = pf"{args.rootdir}".resolve(strict=False)

print(f"{cache=} {r=} {packages=}")


#TODO: trap

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

new_pacman_conf = []
original_pacman_conf = []

if not cache:
    with open(f"{r}/etc/pacman.conf", 'r') as fp:
	original_pacman_conf = fp.readlines()
	new_pacman_conf = []
	for line in original_pacman_conf:
	    if '/etc/pacman.d/mirrorlist' in line and str(r) not in line:
		line = line.replace('/etc/pacman.d/mirrorlist', f"{r}/etc/pacman.d/mirrorlist")
	    if 'ParallelDownloads' in line:
		line = 'ParallelDownloads = 8'
	    new_pacman_conf.append(line)

    if new_pacman_conf:
	with open(f"{r}/etc/pacman.conf", 'w') as fp:
	    for line in new_pacman_conf:
		fp.write(line)

if not cache:
    pacman --verbose -Syy --overwrite "*" -r @(r) --noconfirm --cachedir @(pf"{r}/var/cache/pacman/pkg") --hookdir @(pf"{r}/usr/share/libalpm/hooks") --gpgdir @(pf"{r}/etc/pacman.d/gnupg") --config @(pf"{r}/etc/pacman.conf")

pacman --verbose -S --overwrite "*" -r @(r) --noconfirm --cachedir @(pf"{r}/var/cache/pacman/pkg") --hookdir @(pf"{r}/usr/share/libalpm/hooks") --gpgdir @(pf"{r}/etc/pacman.d/gnupg") --config @(pf"{r}/etc/pacman.conf") @(packages) --ignore iptables

if original_pacman_conf:
    with open(f"{r}/etc/pacman.conf", 'w') as fp:
	for line in original_pacman_conf:
	    fp.write(line)
