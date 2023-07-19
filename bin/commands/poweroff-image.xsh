#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    source @(f'{d}/instance-shell.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import pathlib

def command_poweroff_image_xsh(cwd, logger, name):
    pidfile = f"{cwd}/qemu-{name}.pid"
    command_instance_shell_simple_xsh(cwd, logger, name, "systemctl poweroff", interval=0.005)
    p = pathlib.Path(f"{cwd}/qemu-{name}.pid")
    while p.exists():
	logger.info(f"waiting for machine to power off gracefully")
	time.sleep(0.1)

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    name = MINICLUSTER.ARGS.name
    command_poweroff_image_xsh(cwd, logger, image, name, ram, network, interactive)
