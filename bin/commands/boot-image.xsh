#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    MINICLUSTER.ARGPARSE.add_argument('--image', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    import psutil
    import math
    def_ram = 2**int(math.log2(psutil.virtual_memory().available // 2**20 * 2/3))
    MINICLUSTER.ARGPARSE.add_argument('--ram', default=def_ram)
    MINICLUSTER.ARGPARSE.add_argument('--network', default=False, action="store_true")
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.functions
import os
import json
from abc import ABC, abstractmethod


class Handler(ABC):
    cmd_args = {}
    next = None
    prev = None
    cmdline = []
    logger = None
    def __init__(self, logger, cmd_args, next=None):
        self.logger = logger.getChild(self.__class__.__name__)
	self.cmd_args = cmd_args
	self.next = next
	if next:
	    self.prev = self

    def first(self):
	if self.prev:
	    return self.prev.first()
	return self

    def tail_call_next(self, p):
	if self.next:
	    p.extend(self.next.handle())
	return p

    @abstractmethod
    def handle(self):
	"""Rules:
	if I need values from other handlers, call them first.
	otherwise, tail-call them (preferred)
	"""
	pass

class GenericParameters(Handler):
    def handle(self):
	ram = self.cmd_args['ram']
	cpu = ['-cpu', 'host', '-smp', 'cores=4,threads=1,sockets=1', '-machine', 'type=q35,accel=kvm']
	boot = ['-boot', 'order=d,strict=off,menu=on']
	kvm = ['--enable-kvm', '-m', ram,]
	p = []
	p.extend(cpu)
	p.extend(boot)
	p.extend(kvm)
	return self.tail_call_next(p)

class NetworkingParameters(Handler):
    def handle(self):
	p = ['-nic', 'user,model=virtio',]
	return self.tail_call_next(p)

class UiParameters(Handler):
    def handle(self):
	name = self.cmd_args['name']
	cwd = self.cmd_args['cwd']
	p = [
	    '-nographic',
	    '-serial', 'mon:stdio',
	    '-device', 'virtio-serial',
	    '-chardev', f'socket,path={cwd}/qga-{name}.sock,server=on,wait=off,id=qga0',
	    '-device', 'virtserialport,chardev=qga0,name=org.qemu.guest_agent.0',
	    '-pidfile', f'{cwd}/qemu-{name}.pid',
	    '--name', name,
	]
	return self.tail_call_next(p)

class MediaParameters(Handler):
    # TODO: no floppy, no cd
    def handle(self):
	image = self.cmd_args['image']
	p = [
	    '-device', 'virtio-blk-pci,drive=disk1,bootindex=1,iommu_platform=true,disable-legacy=on',
	    '-drive', f'media=disk,if=none,id=disk1,file={image}.qcow2',
	]
	return self.tail_call_next(p)

class KernelParameters(Handler):
    def handle(self):
	p = self.next.handle()
	# TODO: read root device from MediaParameters
	kernel_append = 'nomodeset console=tty0 console=ttyS0,9600n8 root=/dev/vda2 rw norandmaps printk.devkmsg=on printk.time=y transparent_hugepage=never systemd.journald.forward_to_kmsg amd_iommu=on systemd.unified_cgroup_hierarchy=0'
	kernel = ['-kernel', 'vmlinuz-linux', '-initrd', 'initramfs-linux.img', '-append', kernel_append, ]
	p.extend(kernel)
	return p

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    cmd_args = {
	'image': MINICLUSTER.ARGS.image,
	'name': MINICLUSTER.ARGS.name,
	'ram': MINICLUSTER.ARGS.ram,
	'cwd': cwd,
	'network': MINICLUSTER.ARGS.network,
    }

    generic = GenericParameters(logger, cmd_args)
    net = NetworkingParameters(logger, cmd_args, generic)
    ui = UiParameters(logger, cmd_args, net)
    media = MediaParameters(logger, cmd_args, ui)
    kernel = KernelParameters(logger, cmd_args, media)

    params = kernel.handle()
    append = []
    params.extend(append)
    logger.info(f"{params=}")
    qemu-system-x86_64 @(params)
