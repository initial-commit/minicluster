#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
    source @(f'{d}/make-empty-image.xsh')
    source @(f'{d}/mount-image.xsh')
    source @(f'{d}/umount-image.xsh')
    source @(f'{d}/prepare-chroot.xsh')
    source @(f'{d}/boot-image.xsh')
    source @(f'{d}/test-vm.xsh')
    source @(f'{d}/instance-shell.xsh')
    source @(f'{d}/poweroff-image.xsh')
    source @(f'{d}/copy-files.xsh')
    source @(f'{d}/network-cmd.xsh')
    source @(f'{d}/merge-pacman-repositories.xsh')
    import math
    import psutil
    from cluster.functions import str2bool_exc as strtobool
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--cache', action="store_true", default=False, help="Use local package cache")
    MINICLUSTER.ARGPARSE.add_argument('--initial_build', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--test_image', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--build_nested', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--extract_nested', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    def_ram = 2**int(math.log2(psutil.virtual_memory().available // 2**20 * 2/3))
    MINICLUSTER.ARGPARSE.add_argument('--ram', default=def_ram)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import random
import string
import sys

def extract_l2_assets(cwd, logger, handle, name):
    command_mount_image_xsh(cwd, logger, handle, "ro-build")
    files_to_copy = [
        f"{cwd}/{handle}-ro-build/{cwd_inside}/nested-{handle}.qcow2",
        f"{cwd}/{handle}-ro-build/{cwd_inside}/nested-{handle}-initramfs-linux.img",
        f"{cwd}/{handle}-ro-build/{cwd_inside}/nested-{handle}-vmlinuz-linux",
        f"{cwd}/{handle}-ro-build/{cwd_inside}/fstab-nested-{handle}",
    ]
    for f in files_to_copy:
        cp @(f) @(f"{cwd}/")
    dirs_to_sync = [
        "/var/lib/pacman/sync/",
        "/var/cache/pacman/pkg/",
    ]
    for dr in dirs_to_sync:
        src = pf"{cwd}/{handle}-ro-build/{dr}/"
        target = pf"{cwd}/pacman-mirror-{handle}/{dr}/"
        if not target.exists():
            mkdir -p @(str(target))
        rsync -a --delete --info=stats2,misc1,flist0 @(src) @(target.parent)
    command_umount_image_xsh(cwd, logger, f"{handle}-ro-build")
    
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
        command_instance_shell_simple_xsh(cwd, logger, name, "bash -c \"pacman -Qi python >/dev/null || pacman -S --noconfirm --overwrite '*' python\"")
        command_copy_files_xsh(cwd, logger, '{DIR_R}', f'{name}:/root', additional_env={'name': name})
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
        command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; /root/minicluster/bin/commands/build-base-image.xsh --cache --handle nested-{handle} --build_nested false'")
        # promote embedded image from being an L2 image to being L1
        command_poweroff_image_xsh(cwd, logger, name)
    # This stage operates on two images, called L1 and L2. The host is called L0
    # L2 is created inside L1
    # The procedure is the following
    #
    # boot L1
    # mount L2 read-only and extract: initramfs, kernel, pacman repo, into L1
    # boot L2 inside L1
    # test L2 inside L1
    # cleanup (shrink and clean) L2 inside L1
    # create L2 minicluster pacman repo inside L1
    # assert that all files in L2 are covered by repo inside L1
    # power down L2 inside L1
    # promote all L2 artifacts from L1 to L0: kernel, initramfs, fstab, minicluster pacman repo
    # boot L2 on L0 and test, turn off
    # remove all L2 artifacts from L1
    #
    # cleanup L1 on L0
    # turn off L1 on L0
    # create a pacman package from the minicluster code inside L1 and install it
    # remove all packages inside the L1 minicluster pacman repo also present in the L2 minicluster repo
    # create L1 minicluster pacman repo inside L1
    # promote L1 minicluster pacman repo to L0
    # shut down L1 and remove all temporary artifacts
    # assert that no other files are left in directory except one directory called 'artifacts' where everything is stored
    do_extract = True
    if do_extract:
        cwd_inside = '/root/minic'
        name = get_random_name(handle)
        started = command_boot_image_xsh(cwd, logger, handle, name, l2_ram, True, False)
        if not started:
            sys.exit(1)
        #success = extract_l2_assets(cwd, logger, handle, name)
        #if not success:
        #    sys.exit(1)
        command_poweroff_image_xsh(cwd, logger, name)
    do_extract = False
    if do_extract:
        cwd_inside = '/root/minic'
        name = get_random_name(handle)
        started = command_boot_image_xsh(cwd, logger, handle, name, l2_ram, True, False)
        if not started:
            sys.exit(1)
        # copying over qga is implemented but terribly slow (2G - 30 mins), mounting as RO instead is fast (2G - 10 secs)
        command_mount_image_xsh(cwd, logger, handle, "ro-build")
        files_to_copy = [
            f"{cwd}/{handle}-ro-build/{cwd_inside}/nested-{handle}.qcow2",
            f"{cwd}/{handle}-ro-build/{cwd_inside}/nested-{handle}-initramfs-linux.img",
            f"{cwd}/{handle}-ro-build/{cwd_inside}/nested-{handle}-vmlinuz-linux",
            f"{cwd}/{handle}-ro-build/{cwd_inside}/fstab-nested-{handle}",
        ]
        for f in files_to_copy:
            cp @(f) @(f"{cwd}/")

        dirs_to_sync = [
            "/var/lib/pacman/sync/",
            "/var/cache/pacman/pkg/",
        ]
        for dr in dirs_to_sync:
            src = pf"{cwd}/{handle}-ro-build/{dr}/"
            target = pf"{cwd}/pacman-mirror-{handle}/{dr}/"
            if not target.exists():
                mkdir -p @(str(target))
            echo rsync -a --delete --info=stats2,misc1,flist0 @(f"{src}/") @(target.parent)
            rsync -a --delete --info=stats2,misc1,flist0 @(src) @(target.parent)
        command_umount_image_xsh(cwd, logger, f"{handle}-ro-build")
        # sh -c 'find . \( -type d -printf "%p/\n" , -type f,l -print \) | sed "s|^./||"'
        # END: copy all files from L1 image onto L0
        extracted_db_dir = pathlib.Path(f"{cwd}/pacman-mirror-{handle}/var/lib/pacman/sync/").absolute()
        extracted_pkg_cache = pathlib.Path(f"{cwd}/pacman-mirror-{handle}/var/cache/pacman/pkg/").absolute()
        dest_db_name = f"local-mirror-{handle}"
        dest_db_dir = pathlib.Path(f"{cwd}/local-mirror-{handle}").absolute()
        command_merge_pacman_repositories_xsh(logger, extracted_db_dir, ["core", "extra"], extracted_pkg_cache, dest_db_name, dest_db_dir)
        # mount-image.xsh --handle nested-d1
        # TODO: cleanup image
        command_poweroff_image_xsh(cwd, logger, name)
        # boot the extracted image
        # extract pacman repository to a protected storage pacman-mirror-nested-{handle}
        # clean the extracted image
        # test the extracted image
        # clean the extracted image
        # poweroff the extracted image
        # TODO: md5sum and compare the kernel and the initramfs, should be pairwise identical
        # TODO: md5sum and compare of package dbs /var/lib/pacman/sync/
        # TODO: mount images and extract pacman repositories
        # TODO: clean directories /root, /tmp, /var/log, pacman cache, machine-id, /etc/*.pacnew
        # TODO: make image smaller with: qemu-img convert -O qcow2 d1.qcow2  d1-small.qcow2
        # TODO: compare versions of packages, should be pairwise identical (no lines different, just lines added)
        # TODO: move repositories, images and other assets to a "promoted" area
        # TODO: remove all files in cwd
