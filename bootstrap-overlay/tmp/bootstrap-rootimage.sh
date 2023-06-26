#!/usr/bin/env bash


init () {
	set -e
	whoami
	id

	mount -t proc /proc proc/
	mount /sys sys/ --rbind
	mount --rbind /dev dev/
	mount . . --bind
	#ls -ltrah proc/self/
	rm ./etc/mtab
	cp /etc/mtab ./etc/
	cp /etc/resolv.conf ./etc/
	#cat etc/mtab
	set +e
}

bootstrap () {
	set -e
	set -x
	pwd
	ping -c 1 8.8.8.8 -w 1
	ping -c 1 archlinux.org -w 1
	pacman-key --init
	pacman-key --populate archlinux
	pacman -Syy --noconfirm
	df -h
	mkdir -p /var/cache/pacman/pkg
	pacman -S --noconfirm --overwrite "*" archlinux-keyring
	set +x
}

declare_all () {
	declare -p | grep -Fvf <(declare -rp)
	declare -pf
}

init

exec chroot . bash -c "$(declare_all); bootstrap"
