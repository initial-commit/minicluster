#!/usr/bin/env bash

set -e
set -x

# TODO: get rid of overwrite --overwrite "*" 
pacman -Qi python-pip &>/dev/null || { pacman -S --noconfirm --overwrite "*" python python-pip xonsh python-dateutil python-psutil python-pygments libguestfs guestfs-tools qemu-base arch-install-scripts sequoia-sq which
# TODO: get current user
echo 'root:100000:65536' > /etc/subuid
echo 'root:100000:65536' > /etc/subgid
virt-host-validate qemu
}
