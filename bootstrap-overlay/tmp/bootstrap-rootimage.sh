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
	pacman-key --init
	pacman-key --populate archlinux
	set +e
	ip -j -p addr show | grep -Po '"operstate": "[^"]+"' | cut -d':' -f 2 | grep -w '"UP"' && pacman -Syy --noconfirm
	set -e
	mkdir -p /var/cache/pacman/pkg
	if ip -j -p addr show | grep -Po '"operstate": "[^"]+"' | cut -d':' -f 2 | grep -w '"UP"' ; then
		pacman -S --noconfirm --overwrite "*" archlinux-keyring
	else
		find var/cache/pacman/pkg/ -name 'archlinux-keyring-*-any.pkg.tar.zst' -exec pacman -U --noconfirm --overwrite '*' {} \;
	fi
	set +x
}

declare_all () {
	declare -p | grep -Fvf <(declare -rp)
	declare -pf
}

init

exec chroot . bash -c "$(declare_all); bootstrap"
