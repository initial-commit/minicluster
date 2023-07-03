#!/usr/bin/env xonsh

if __name__ == '__main__':
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
ram="2048"

logger.info(f"{image=} {name=}")

#qemu-system-x86_64 -enable-kvm -m ram -boot c -nic user,model=virtio -drive file=disk.qcow2,media=disk,if=virtio -nographic \
generic = ['--enable-kvm', '-boot', 'menu=on', '-m', ram, '-nic', 'user,model=virtio', '-drive', f'file={image}.qcow2,media=disk,if=virtio', '-nographic', '-vga', 'none']
generic = ['--enable-kvm', '-boot', 'menu=on', '-m', ram, '-nic', 'user,model=virtio', '-drive', f'file={image}.qcow2,media=disk,if=virtio', ]
generic = ['--enable-kvm', '-boot', 'menu=on', '-m', ram, '-nic', 'user,model=virtio', '-drive', f'file={image}.qcow2,media=disk,if=virtio', '-display', 'none', '-vga', 'none', '-nographic']
generic = ['--enable-kvm', '-boot', 'menu=on', '-m', ram, '-nic', 'user,model=virtio', '-drive', f'file={image}.qcow2,media=disk,if=virtio',
    '-display', 'none', '-vga', 'none', '-nographic',
    ]
generic = ['--enable-kvm', '-m', ram, '-nic', 'user,model=virtio',
    '-device', 'virtio-blk-pci,drive=disk1,bootindex=1,iommu_platform=true,disable-legacy=on',
    '-drive', f'media=disk,if=none,id=disk1,file={image}.qcow2',
    '-nographic', '-serial', 'mon:stdio',
    ]
kernel_append = 'nomodeset console=tty0 console=ttyS0,38400 root=/dev/vda2 rw nopat nokaslr norandmaps printk.devkmsg=on printk.time=y edd=off transparent_hugepage=never systemd.journald.forward_to_kmsg'
kernel_append = 'nomodeset console=tty0 console=ttyS0,9600n8 root=/dev/vda2 rw nopat nokaslr norandmaps printk.devkmsg=on printk.time=y edd=off transparent_hugepage=never systemd.journald.forward_to_kmsg amd_iommu=on cgroup_memory=1 cgroup_enable=cpuset systemd.unified_cgroup_hierarchy=0'
kernel = ['-kernel', 'vmlinuz-linux', '-initrd', 'initramfs-linux.img', '-append', kernel_append, ]
cpu = ['-cpu', 'host', '-smp', 'cores=8,threads=1,sockets=1', ]
cpu = ['-cpu', 'host', '-smp', 'cores=4,threads=1,sockets=1', '-machine', 'virt,q35,vmport=off,kernel_irqchip=on,hpet=off']
cpu = ['-cpu', 'host', '-smp', 'cores=4,threads=1,sockets=1', '-machine', 'type=q35,accel=kvm']
boot = ['-boot', 'order=d,strict=off,menu=on']
devices = ['-device', 'virtio-serial',
	'-chardev', f'socket,path={cwd}/qga-{name}.sock,server=on,wait=off,id=qga0',
	'-device', 'virtserialport,chardev=qga0,name=org.qemu.guest_agent.0',
	
    ]
#devices = []
host = ['-pidfile', f'{cwd}/qemu-{name}.pid', '--name', name]
append = []

echo qemu-system-x86_64 @(generic) @(kernel) @(cpu) @(boot) @(devices) @(host) @(append)
qemu-system-x86_64 @(generic) @(kernel) @(cpu) @(boot) @(devices) @(host) @(append)
