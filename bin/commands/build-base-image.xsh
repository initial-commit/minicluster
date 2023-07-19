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
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--cache', action="store_true", default=False)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import random
import string

def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"build-tmp-{handle}-{r}"

if __name__ == '__main__':
    do_initial_build = True
    # fixing image is not necessary with the latest changes done via guestfish in prepare-chroot
    do_fix_image = False
    do_test_image = True
    do_build_l2 = True

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
        command_prepare_chroot_xsh(cwd, logger, handle, cache)
        cp -a @(f"{handle}.qcow2") @(f"pristine-{handle}.qcow2")


    if do_fix_image:
        name = get_random_name(handle)
        command_boot_image_xsh(cwd, logger, handle, name, 2048, True, False)
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

    if do_test_image:
        name = get_random_name(handle)
        # start the image without networking and execute tests, assert success
        command_boot_image_xsh(cwd, logger, handle, name, 2048, True, False)
        command_test_vm_xsh(cwd, logger, name)
        #command_instance_shell_simple_xsh(cwd, logger, name, "systemctl poweroff", interval=0.005)
        command_poweroff_image_xsh(cwd, logger, name)
        #import pathlib
        #p = pathlib.Path(f"{cwd}/qemu-{name}.pid")
        #while p.exists():
        #    logger.info(f"waiting for machine to power off gracefully")
        #    time.sleep(0.1)

    if do_build_l2:
        $XONSH_SHOW_TRACEBACK = True
        name = get_random_name(handle)
        command_boot_image_xsh(cwd, logger, handle, name, 4096, True, False)
        # copy DIR_R into the image
        # build itself with different parameters
        # extract image from within
        command_poweroff_image_xsh(cwd, logger, name)
        # boot the extracted image
        # test the extracted image
        # clean the extracted image
        # poweroff the extracted image
        # move the extracted image L2 to the L1 image qcow2
