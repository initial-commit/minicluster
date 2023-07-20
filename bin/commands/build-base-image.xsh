#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    source @(f'{d}/make-empty-image.xsh')
    source @(f'{d}/mount-image.xsh')
    source @(f'{d}/prepare-chroot.xsh')
    source @(f'{d}/boot-image.xsh')
    source @(f'{d}/test-vm.xsh')
    source @(f'{d}/instance-shell.xsh')
    source @(f'{d}/poweroff-image.xsh')
    source @(f'{d}/copy-files.xsh')
    source @(f'{d}/network-cmd.xsh')
    import math
    import psutil
    from cluster.functions import str2bool_exc as strtobool
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--cache', action="store_true", default=False, help="Use local package cache")
    MINICLUSTER.ARGPARSE.add_argument('--initial_build', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--test_image', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--build_nested', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    def_ram = 2**int(math.log2(psutil.virtual_memory().available // 2**20 * 2/3))
    MINICLUSTER.ARGPARSE.add_argument('--ram', default=def_ram)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import random
import string
import sys

def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"build-tmp-{handle}-{r}"

if __name__ == '__main__':
    do_initial_build = MINICLUSTER.ARGS.initial_build
    # fixing image is not necessary with the latest changes done via guestfish in prepare-chroot
    do_fix_image = False
    do_test_image = MINICLUSTER.ARGS.test_image
    do_build_l2 = MINICLUSTER.ARGS.build_nested
    vm_ram = MINICLUSTER.ARGS.ram
    l2_ram = int(vm_ram / 2)

    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle
    cache = MINICLUSTER.ARGS.cache
    d = {
        'type': 'mbr',
        'size': '20GB',
        'sector': 512,
        'partitions': [
            {'type': 'primary', 'start': '1MB', 'size': '199MB', 'bootable': True, 'fs': 'ext4', 'mountpoint': '/boot', },
            {'type': 'primary', 'start': '200MB', 'size': -1, 'fs': 'ext4', 'mountpoint': '/', },
        ],
    }
    $RAISE_SUBPROC_ERROR = True
    if do_initial_build:
        command_make_empty_image_xsh(cwd, logger, handle, d)
        command_mount_image_xsh(cwd, logger, handle)
        success = command_prepare_chroot_xsh(cwd, logger, handle, cache)
        if not success:
            sys.exit(1)
        if do_build_l2:
            cp -a @(f"{handle}.qcow2") @(f"pristine-{handle}.qcow2")

    if do_fix_image:
        name = get_random_name(handle)
        started = command_boot_image_xsh(cwd, logger, handle, name, 2048, True, False)
        if not started:
            sys.exit(1)
        commands = [
            "systemd-tmpfiles --create --clean --remove --boot",
        # echo -e "shopt -s extglob\nchown -R root:root /!(sys|proc|run)" | bash
            #TODO: this could use the input-data of the qga protocol
            'echo -e "shopt -s extglob\nchown -R root:root /!(sys|proc|run)" | bash',
            'systemctl poweroff',
        ]
        interval = 0.1
        for i, command in enumerate(commands):
            logger.info(f"---------------- {command=}")
            if i == len(commands)-1:
                $RAISE_SUBPROC_ERROR = False
                interval = 0.005
            command_instance_shell_simple_xsh(cwd, logger, name, command, interval=interval)
            # TODO: error handling

    image_started = False
    if do_test_image:
        name = get_random_name(handle)
        started = command_boot_image_xsh(cwd, logger, handle, name, vm_ram, True, False)
        if not started:
            sys.exit(1)
        command_test_vm_xsh(cwd, logger, name)
        command_poweroff_image_xsh(cwd, logger, name)

    if not image_started and do_build_l2:
        name = get_random_name(handle)
        started = command_boot_image_xsh(cwd, logger, handle, name, l2_ram, True, False)
        if not started:
            sys.exit(1)
        image_started = True

    if do_build_l2:
        cwd_inside = '/root/minic'
        command_instance_shell_simple_xsh(cwd, logger, name, "pacman -Qi python >/dev/null || pacman -S --noconfirm --overwrite '*' python")
        command_copy_files_xsh(cwd, logger, '{DIR_R}', f'{name}:/root')
        command_instance_shell_simple_xsh(cwd, logger, name, "/root/minicluster/bin/commands/bootstrap-host.sh")
        command_instance_shell_simple_xsh(cwd, logger, name, f"mkdir -p {cwd_inside}")
        copy_cwd = [
            'archlinux-bootstrap-x86_64.tar.gz',
            'archlinux-bootstrap-x86_64.tar.gz.sig',
            'release-key.pgp',
        ]
        for f in copy_cwd:
            command_copy_files_xsh(cwd, logger, '{CWD_START}/{f}', '{name}:{cwd_inside}/', additional_env={'f': f, 'name': name, 'cwd_inside': cwd_inside})
        # bootstrap L1 image as a minicluster host 
        command_network_cmd_xsh(cwd, logger, name, False)
        # build L2 image without networking
        # build itself with different parameters
        # promote embedded image from being an L2 image to being L1
        command_poweroff_image_xsh(cwd, logger, name)
        # boot the extracted image
        # test the extracted image
        # clean the extracted image
        # poweroff the extracted image
        # move the extracted image L2 to the L1 image qcow2
