#!/usr/bin/env bash

set -e
set -x

# TODO: get rid of overwrite --overwrite "*" 
pacman -S --noconfirm python-pip xonsh python-dateutil python-psutil python-pygments libguestfs guestfs-tools qemu-base arch-install-scripts
echo 'root:100000:65536' > /etc/subuid
echo 'root:100000:65536' > /etc/subgid
