#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    MINICLUSTER.ARGPARSE.add_argument('--image', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    import psutil
    import math
    from cluster.functions import str2bool_exc as strtobool
    def_ram = 2**int(math.log2(psutil.virtual_memory().available // 2**20 * 2/3))
    MINICLUSTER.ARGPARSE.add_argument('--ram', default=def_ram)
    MINICLUSTER.ARGPARSE.add_argument('--network', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--interactive', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.functions
import cluster.qmp
import os
import json
from abc import ABC, abstractmethod
import time
import pathlib


class Handler(ABC):
    cmd_args = {}
    next = None
    prev = None
    cmdline = []
    logger = None
    prepare_commands = []
    post_commands = []
    chained_commands = []
    def __init__(self, logger, cmd_args, next=None):
        self.logger = logger.getChild(self.__class__.__name__)
	self.cmd_args = cmd_args
	self.next = next
	if self.next:
	    self.chained_commands = self.next.chained_commands
	self.chained_commands.append(self)
	if next:
	    self.prev = self

    def first(self):
	if self.prev:
	    return self.prev.first()
	return self

    def tail_call_next(self, p, pre_commands=[], post_commands=[]):
	self.prepare_commands = pre_commands
	self.post_commands = post_commands
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

    def get_pre_commands(self):
	cmds = []
	for cmd in self.chained_commands:
	    if cmd.prepare_commands:
		cmds.extend(cmd.prepare_commands)
	cmds = sorted(list(set(cmds)))
	return cmds

    def get_post_commands(self):
	cmds = []
	for cmd in self.chained_commands:
	    if cmd.post_commands:
		cmds.extend(cmd.post_commands)
	cmds.reverse()
	return cmds

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
	has_network = self.cmd_args['network']
	self.logger.info(f"{has_network=}")
	if has_network:
	    p = [
		'-netdev', 'user,id=mynet0',
		'-device', 'virtio-net-pci-non-transitional,netdev=mynet0',
	    ]
	else:
	    p = ['-nic', 'none']
	return self.tail_call_next(p)

class UiParameters(Handler):
    def handle(self):
	# TODO: also use -nodefaults -no-user-config 

	name = self.cmd_args['name']
	cwd = self.cmd_args['cwd']
	interactive = self.cmd_args['interactive']
	if interactive:
	    interactive_p = ['-nographic', '-serial', 'mon:stdio']
	else:
	    interactive_p = ['--daemonize']
	p = [
	    '-vnc', 'none',
	    '-device', 'virtio-serial',
	    '-chardev', f'socket,path={cwd}/qga-{name}.sock,server=on,wait=off,id=qga0',
	    '-device', 'virtserialport,chardev=qga0,name=org.qemu.guest_agent.0',
	    '-pidfile', f'{cwd}/qemu-{name}.pid',
	    #'-monitor', f'unix:{cwd}/qemu-monitor-{name}.sock,server,nowait',
	    '-chardev', f'socket,path={cwd}/monitor-{name}.sock,server=on,wait=off,id=mon0', '-mon', 'chardev=mon0,mode=control,pretty=on',
	    #'-chardev', f'socket,path={cwd}/monitor-{name}.sock,server=on,wait=off,id=mon0', '-mon', 'chardev=mon0',
	    '--name', name,
	    #'-chardev', 'pty,id=p1',
	    #'-serial', 'pty',
	    #'-chardev', f'socket,id=s1,server=off,path={cwd}/pci-serial.sock', '-device', 'pci-serial-2x,chardev1=s1',
	    #'-chardev', f'file,id=s1,path={cwd}/pci-serial.sock', '-device', 'pci-serial-2x,chardev1=s1', # lspci -d 1b36:0003 -mm -nn -D
		# see IRQ and port from device: setserial -g ttyS* | grep 16550A | grep -w 'IRQ: 20' | grep 'Port
		# see IRQ and port for pci: lspci -d 1b36:* -v
	    #'-chardev', f'file,id=s1,path={cwd}/pci-serial.sock', '-device', 'pci-serial-4x,chardev1=s1', # lspci -d 1b36:0003 -mm -nn -D
	    '-chardev', f'pipe,id=serial1,path={cwd}/pci-serial1.pipe',
	    '-chardev', f'pipe,id=serial2,path={cwd}/pci-serial2.pipe',
	    '-chardev', f'pipe,id=serial3,path={cwd}/pci-serial3.pipe',
	    '-chardev', f'pipe,id=serial4,path={cwd}/pci-serial4.pipe',
	    '-device', 'pci-serial-4x,chardev1=serial1,chardev2=serial2,chardev3=serial3,chardev4=serial4', # lspci -d 1b36:0003 -mm -nn -D
	    # from pci: lspci -d 1b36:* -v
	    # TODO: vhost-user-vsock-pci
	    #-chardev socket,id=mon1,host=localhost,port=4444,server=on,wait=off
	    #-mon chardev=mon1,mode=control,pretty=on
	]
	prepare_commands = []
	rm_commands = []
	for i in range(1,5):
	    if not pf"{cwd}/pci-serial{i}.pipe.in".exists():
		prepare_commands.append(("mkfifo", f"{cwd}/pci-serial{i}.pipe.in"))
		rm_commands.append(("rm", f"{cwd}/pci-serial{i}.pipe.in"))
	    if not pf"{cwd}/pci-serial{i}.pipe.out".exists():
		prepare_commands.append(("mkfifo", f"{cwd}/pci-serial{i}.pipe.out"))
		rm_commands.append(("rm", f"{cwd}/pci-serial{i}.pipe.out"))
	# scenario 1 at runtime
	# mkfifo pci-serial.out
	# mkfifo pci-serial.in
	# chardev-add pipe,id=s1,path=pci-serial
	prepare_commands = list(set(prepare_commands))
	self.logger.info(f"{prepare_commands=}")
	p.extend(interactive_p)
	return self.tail_call_next(p, prepare_commands, rm_commands)

class MediaParameters(Handler):
    # TODO: no floppy, no cd
    def handle(self):
	image = self.cmd_args['image']
	p = [
	    '-device', 'virtio-blk-pci,drive=disk1,bootindex=1,iommu_platform=true,disable-legacy=on',
	    '-drive', f'media=disk,if=none,id=disk1,file={image}.qcow2', #cache=directsync, none, writethrough, unsafe
	]
	return self.tail_call_next(p)

class KernelParameters(Handler):
    def handle(self):
	p = self.next.handle()
	image = self.cmd_args['image']
	kernel = f'{image}-vmlinuz-linux'
	initrd = f'{image}-initramfs-linux.img'
	if '/' in image:
	    img_p = pathlib.Path(image)
	    base_dir = img_p.parent
	    base_name = img_p.name
	    self.logger.info(f"{img_p=} {base_dir=} {base_name=}")
	    kernels = list(base_dir.glob('*vmlinuz-linux'))
	    assert len(kernels) == 1, f"Could not find kernel in {base_dir=}"
	    kernel = str(kernels[0].absolute())
	    initrds = list(base_dir.glob('*initramfs-linux.img'))
	    assert len(initrds) == 1, f"Could not find initrd in {base_dir=}"
	    initrd = str(initrds[0].absolute())
	# TODO: read root device from MediaParameters
	# the order of the console statements determines if the systemd service start protocol is visible or not/screen reset
	kernel_append = 'console=tty0 console=ttyS0,19200n8 root=/dev/vda2 rw norandmaps printk.devkmsg=on printk.time=y transparent_hugepage=never systemd.journald.forward_to_kmsg amd_iommu=on systemd.unified_cgroup_hierarchy=0'
	#console=ttyS0,115200n8 earlyprintk=ttyS0,115200 debug loglevel=0-7
	kernel = ['-kernel', kernel, '-initrd', initrd, '-append', kernel_append, ]
	p.extend(kernel)
	return p

def command_boot_image_xsh(cwd, logger, image, name, ram, network, interactive):
    cmd_args = {
	'image': image,
	'name': name,
	'ram': ram,
	'cwd': cwd,
	'network': network,
	'interactive': interactive,
    }
    generic = GenericParameters(logger, cmd_args)
    net = NetworkingParameters(logger, cmd_args, generic)
    ui = UiParameters(logger, cmd_args, net)
    media = MediaParameters(logger, cmd_args, ui)
    kernel = KernelParameters(logger, cmd_args, media)

    params = kernel.handle()
    append = [
	#'-device', 'virtio-serial-pci,id=ser0',
	#'-device', 'virtio-serial-device',
	#'-chardev', f'socket,path={cwd}/foo,server=off,reconnect=1,id=foo,telnet=off',
	#'-chardev', f'socket,path={cwd}/bar,server,nowait,id=foo,telnet=off',
	#'-device', 'virtserialport,bus=ser0.0,chardev=foo,name=org.fedoraproject.port.0',
	#'-device', 'virtioconsole,bus=ser0.1,chardev=bar,name=org.initial-commit.bar'
	#'-chardev', f'socket,path={cwd}/vsock-pci.sock,id=chr0,server=on,wait=off',
	#'-device', 'vhost-user-vsock-pci,disable-legacy=on,chardev=chr0',
    ]
    pre_commands = generic.get_pre_commands()
    params.extend(append)
    logger.info(f"{params=}")
    prev_err = $RAISE_SUBPROC_ERROR
    $RAISE_SUBPROC_ERROR = False
    for c in pre_commands:
	logger.debug(f"pre command: {c}")
	p=![@(c)]
	if p.rtn != 0:
	    logger.error(f"command failed with exit code {p.rtn}: {c}")
	    return False
    p=![qemu-system-x86_64 @(params)]
    $RAISE_SUBPROC_ERROR = prev_err
    exit_code = p.rtn
    if exit_code != 0:
	logger.error(f"failed to start qemu with {exit_code=}")
	return False
    if interactive:
	post_commands = generic.get_post_commands()
	for c in post_commands:
	    logger.debug(f"post command: {c}")
	    p=![@(c)]
	    if p.rtn != 0:
		logger.error(f"command failed with exit code {p.rtn}: {c}")
		return False
    if not interactive:
	s = f"{cwd}/qga-{name}.sock"
	logger.info(f"establishing connection")
	conn = cluster.qmp.Connection(s, logger)
	logger.info(f"waiting to react to ping")
	online = conn.ping()
	assert online, "machine got online for qmp"
	logger.info(f"waiting for machine {name=} to appear online {online=}")

	startup_finished = False
	while not startup_finished:
	    status = conn.guest_exec_wait('journalctl --boot --lines=all -o export --output=json')
	    lines = status['out-data'].splitlines()
	    for idx, line in enumerate(lines):
		line = json.loads(line)
		if line['MESSAGE'].startswith('Startup finished '):
		    logger.info(line['MESSAGE'])
		    startup_finished = True
		    break
	    time.sleep(1)
    return True


if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    image = MINICLUSTER.ARGS.image
    name = MINICLUSTER.ARGS.name
    ram = MINICLUSTER.ARGS.ram
    network = MINICLUSTER.ARGS.network
    interactive = MINICLUSTER.ARGS.interactive
    command_boot_image_xsh(cwd, logger, image, name, ram, network, interactive)
