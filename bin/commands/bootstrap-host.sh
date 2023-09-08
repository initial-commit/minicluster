#!/usr/bin/env bash

set -e
set -x

# TODO: get rid of overwrite --overwrite "*" 
pacman -Qi python-pip &>/dev/null || { pacman --verbose -S --noconfirm --overwrite "*" python python-pip xonsh python-dateutil python-requests python-psutil python-pygments python-pyzstd libguestfs guestfs-tools qemu-base arch-install-scripts sequoia-sq which virtiofsd lxc
# TODO: get current user
# TODO: we depend on lxc just for lxc-usernsexec + virtiofsd, which should be doable with standard unshare
echo 'root:100000:65536' > /etc/subuid
echo 'root:100000:65536' > /etc/subgid
virt-host-validate qemu
}
