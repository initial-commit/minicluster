#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    source @(f'{d}/instance-shell.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--name', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import pathlib
import time

def command_poweroff_image_xsh(cwd, logger, name, interval=0.005):
    pidfile = f"{cwd}/qemu-{name}.pid"
    try:
	command_instance_shell_simple_xsh(cwd, logger, name, "systemctl poweroff", interval=interval)
    except ConnectionResetError:
	return True
    p = pathlib.Path(f"{cwd}/qemu-{name}.pid")
    printed = 0
    while p.exists():
	if printed != 0:
	    time.sleep(0.1)
	if printed % 20 == 0:
	    logger.info(f"waiting for machine to power off gracefully")
	printed += 1
    return True

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    name = MINICLUSTER.ARGS.name
    command_poweroff_image_xsh(cwd, logger, name)
