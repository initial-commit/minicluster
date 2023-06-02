#!/usr/bin/env xonsh

d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
MINICLUSTER.ARGPARSE.add_argument('--image')
MINICLUSTER.ARGPARSE.add_argument('--name')
MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.functions
import os
import json

cwd = MINICLUSTER.CWD_START

logger = logging.getLogger(__name__)
image = MINICLUSTER.ARGS.image
name = MINICLUSTER.ARGS.name

logger.info(f"{image=} {name=}")

#qemu-system-x86_64 -enable-kvm -m 2048 -boot c -nic user,model=virtio -drive file=disk.qcow2,media=disk,if=virtio -nographic \
generic = ['--enable-kvm', '-boot', 'menu=on', '-m', '2048', '-nic', 'user,model=virtio', '-drive', f'file={image}.qcow2,media=disk,if=virtio', '-nographic', '-vga', 'none']
generic = ['--enable-kvm', '-boot', 'menu=on', '-m', '2048', '-nic', 'user,model=virtio', '-drive', f'file={image}.qcow2,media=disk,if=virtio', ]
generic = ['--enable-kvm', '-boot', 'menu=on', '-m', '2048', '-nic', 'user,model=virtio', '-drive', f'file={image}.qcow2,media=disk,if=virtio', '-display', 'none', '-vga', 'none', '-nographic']
kernel = ['-kernel', 'vmlinuz-linux', '-initrd', 'initramfs.img', '-append', 'console=ttyS0 root=/dev/vda2 rw nopat nokaslr norandmaps printk.devkmsg=on printk.time=y edd=off transparent_hugepage=never']
cpu = ['-cpu', 'host', '-smp', 'cores=4,threads=1,sockets=1', '-machine', 'virt,q35,vmport=off,kernel_irqchip=on,hpet=off']
cpu = ['-cpu', 'host', '-smp', 'cores=4,threads=1,sockets=1', ]
boot = ['-boot', 'order=c,strict=on']
devices = ['-device', 'virtio-serial',
	'-chardev', 'socket,path=/tmp/qga.sock,server=on,wait=off,id=qga0',
	'-device', 'virtserialport,chardev=qga0,name=org.qemu.guest_agent.0']
#devices = []
host = ['-pidfile', f'/tmp/minicluster-name-{name}.pid', '--name', name]
#append = ['-append', 'panic=1 edd=off']
append = []

qemu-system-x86_64 @(generic) @(kernel) @(cpu) @(boot) @(devices) @(host) @(append)

