#!/usr/bin/env xonsh

# For overview:
# this command downloads all assets necessary on an archlinux system called L0 henceforth.
# L0 is your real, physical machine.
# on L0, an L1 image is built. This will be thrown away, after extracting the pacman repository
# inside L1, we build an L2 image. This image is a barebones archlinux system (with more or less just the qemu agent inside)
#
# Outputs of this process:
#
# * the archlinux appliance
# * the archlinux pacman repository
# * the minicluster pacman repository
#
# The archlinux appliance
# =======================
# * it consists of the files: qcow2 image, initramfs, kernel, fstab (just for reference)
# * the files come from inside L2
# * the files are produced by processes/commands running inside L1
# * does not contain any pacman packages or databases (all mirrors removed)
# * at various points in this script, these files are stored in "the artefacts directory" inside L1 (cwd_inside variable)
#
# The archlinux pacman repository
# ===============================
# * is is a valid "mirror" containing all the packages installed in the archlinux appliance
# * the packages are installed inside the archlinux appliance, but this repository is not available in a VM of archlinux
# * is the base of all other archlinux-based systems in all your clusters
#
# The minicluster pacman repository
# =================================
# * is a derivation of the archlinux pacman repository, packages are deduplicated and this repo contains just minicluster-specific
#   packages
# * will be used by another command to make a minicluster appliance

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
    from cluster.functions import get_linenumber
    from cluster.functions import PipeTailer
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--cache', action="store_true", default=False, help="Use local package cache")
    MINICLUSTER.ARGPARSE.add_argument('--initial_build', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--build_nested', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--extract_nested', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    # TODO: when exiting cleanly, cleanup
    def_ram = 2**int(math.log2(psutil.virtual_memory().available // 2**20 * 3/3))
    MINICLUSTER.ARGPARSE.add_argument('--ram', default=def_ram, type=int)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import random
import string
import sys


def extract_l2_assets(cwd, logger, handle, name, cwd_inside):
    """In this function we want to use a lot of bash execution.

    This ensures that we operate on L2 using the next-state
    version of the underlying system.

    Doing so tests as part of the process the L1 image.
    While the L1 image we operate on here is not used, the package
    repository will be extracted and used later on (when building minicluster)
    """
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"mkdir -p '{cwd_inside}'")
    if not success:
        return False
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; /root/minicluster/bin/commands/extract-image-assets.xsh --handle nested-{handle}'")
    if not success:
        return False
    command_mount_image_xsh(cwd, logger, handle, "ro-build")
    ro_dir_on_l0 = fp"{cwd}/{handle}-ro-build/".absolute()
    artefacts_dir_ro = fp"{ro_dir_on_l0}/{cwd_inside}/artefacts-nested-{handle}".absolute()
    #command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; ls -ltrah'", interval=0.1, env={}, show_out=True)
    # TODO: clean the image, assert that no files are uncovered (except whitelisted files)
    # TODO: shrink the image, assert that the size is indeed smaller
    # TODO: test-vm the L2 image, turn off
    # TODO: assert that L2 image is turned off
    # TODO: assert that the L2 image is not running; copy the image itself
    # TODO: copy the artefacts of L2 inside L1 onto L0
    command_umount_image_xsh(cwd, logger, f"{handle}-ro-build")
    return True

    # TODO: remove below
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

def extract_l1_assets():
    # TODO: mount ro
    # TODO: assert md5 checksums are the same for initramfs and linux-kernel
    # TODO: make L1 pacman repository (fat variant)
    # TODO: assert that L1 repository only contains additions relative to L2 repository
    # TODO: unmount ro
    pass
    
def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"build-tmp-{handle}-{r}"

def proc_initial_build(cwd, logger, handle, diskspec, cache):
    # TODO: re-activate this snippet
    if cache:
        command_make_empty_image_xsh(cwd, logger, handle, d)
        command_mount_image_xsh(cwd, logger, handle)
        success = command_prepare_chroot_xsh(cwd, logger, handle, cache)
        if not success:
            logger.error(f"failed to prepare chroot for {handle=}")
            return None
        # TODO: make a backup
        #cp -a @(f"{handle}.qcow2") @(f"pristine-{handle}.qcow2")
    name = get_random_name(handle) + str(get_linenumber())
    started = command_boot_image_xsh(cwd, logger, handle, name, 2048, True, False)
    if not started:
        logger.error("failed to boot initial image")
        return None
    # TODO: the "commands" below is "fixing image", the question is if this is still necessary
    commands = [
        "systemd-tmpfiles --create --clean --remove --boot",
        #TODO: this could use the input-data of the qga protocol
        #TODO: any other cleanups we could do from the clean-image command?
        'echo -e "shopt -s extglob\nchown -R root:root /!(sys|proc|run)" | bash',
    ]
    prev_raise = $RAISE_SUBPROC_ERROR
    $RAISE_SUBPROC_ERROR = False
    for i, command in enumerate(commands):
        logger.info(f"---------------- {command=}")
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, command)
        $RAISE_SUBPROC_ERROR = prev_raise
        if not success:
            if started:
                command_poweroff_image_xsh(cwd, logger, name)
                started = False
            logger.error(f"failed command: {command=}")
            return None
    $RAISE_SUBPROC_ERROR = prev_raise
    success = command_test_vm_xsh(cwd, logger, name)
    if not success:
        return None
    if started:
        command_poweroff_image_xsh(cwd, logger, name)
    # TODO: here we could make a backup of the image pristine-{handle}.qcow2
    return True


if __name__ == '__main__':
    do_initial_build = MINICLUSTER.ARGS.initial_build
    do_build_nested = MINICLUSTER.ARGS.build_nested
    extract_nested = MINICLUSTER.ARGS.extract_nested
    vm_ram = MINICLUSTER.ARGS.ram
    l2_ram = int(vm_ram / 2)

    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle
    cache = MINICLUSTER.ARGS.cache
    # TODO: this should be a parameter
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
    logger.info(f"starting with RAM L1: {vm_ram} L2: {l2_ram}")

    if do_initial_build:
        success = proc_initial_build(cwd, logger, handle, d, cache)
        if not success:
            logger.error(f"initial build of L1 image failed: {handle=}")
            sys.exit(1)

    def proc_build_nested(cwd, logger, handle, l2_ram):
        # TODO: poweroff would benefit from a context
        name = get_random_name(handle) + str(get_linenumber())
        name = "t1"
        # boot image
        started = command_boot_image_xsh(cwd, logger, handle, name, l2_ram, True, False)
        if not started:
            logger.error(f"failed to start {handle=} with ram {l2_ram=} and {name=}")
            return False
        # install python
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, "bash -c \"pacman -Qi python >/dev/null || pacman -S --noconfirm --overwrite '*' python\"")
        if not success:
            logger.error(f"failed installing python for building nested L2 image {handle=} {name=}")
            command_poweroff_image_xsh(cwd, logger, name)
            started = False
            return False
        # copy code into vm
        written = command_copy_files_xsh(cwd, logger, '{DIR_R}', f'{name}:/root', additional_env={'name': name})
        if not written:
            logger.error(f"failed to copy minicluster into L1, vm {name=}")
            command_poweroff_image_xsh(cwd, logger, name)
            started = False
            return False
        # make the working directory in which we'll be building L2
        cwd_inside = '/root/minic'
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"mkdir -p {cwd_inside}")
        if not success:
            logger.error(f"failed to make temporary build directory")
            command_poweroff_image_xsh(cwd, logger, name)
            started = False
            return False
        # bootstrap L1 as a minicluster-building capable VM
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, "/root/minicluster/bin/commands/bootstrap-host.sh")
        if not success:
            logger.error(f"failed to bootstrap L1 host into a minicluster-capable vm: {name=} {handle=}")
            command_poweroff_image_xsh(cwd, logger, name)
            started = False
            return False
        # copy files from L0 into L1 instead of downloading them again
        copy_cwd = [
            'archlinux-bootstrap-x86_64.tar.gz',
            'archlinux-bootstrap-x86_64.tar.gz.sig',
            'release-key.pgp',
        ]
        for f in copy_cwd:
            written = command_copy_files_xsh(cwd, logger, '{CWD_START}/{f}', '{name}:{cwd_inside}/', additional_env={'f': f, 'name': name, 'cwd_inside': cwd_inside})
            if not written:
                logger.error(f"failed to copy bootstrapping file {f=} into {name=} of disk {handle=}")
                command_poweroff_image_xsh(cwd, logger, name)
                started = False
                return False
        # build L2 inside L1
        success = command_network_cmd_xsh(cwd, logger, name, False)
        if not success:
            logger.error("could not turn off network")
            command_poweroff_image_xsh(cwd, logger, name)
            started = False
            return False
        # TODO: reliably map ttyS4 and pci-serial1.pipe.out to each other
        tailer = PipeTailer(f'{cwd}/pci-serial1.pipe.out', logger)
        tailer.start()
        # TODO: instead of using tee, set up logging to ttyS4 in bootstrap
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; /root/minicluster/bin/commands/build-base-image.xsh --cache --handle nested-{handle} --build_nested false --extract_nested false 2>&1 | tee -a -p /dev/ttyS4'")
        if not success:
            logger.error(f"failed to build nested L2 image nested-{handle} in {name}:{cwd_inside}/")
            command_poweroff_image_xsh(cwd, logger, name)
            started = False
            return False
        # turn off the L1 image
        if started:
            command_poweroff_image_xsh(cwd, logger, name)
        logger.info(f"joining tailer thread")
        tailer.join(5)
        return True

    if do_build_nested:
        success = proc_build_nested(cwd, logger, handle, l2_ram)
        if not success:
            logger.error(f"building of L2 image inside L1 failed: {handle=}")
            sys.exit(2)

    if cache:
        logger.info(f"SUCCESS inside!!!!")
        sys.exit(0)
    raise Exception(f"TODO: migrate and remove below")



























    if not image_started and do_build_l2:
        name = get_random_name(handle) + str(get_linenumber())
        started = command_boot_image_xsh(cwd, logger, handle, name, l2_ram, True, False)
        if not started:
            sys.exit(1)
        image_started = True

    if image_started:
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, "bash -c \"pacman -Qi python >/dev/null || pacman -S --noconfirm --overwrite '*' python\"")
        if not success:
            sys.exit(st['exitcode'])
        command_copy_files_xsh(cwd, logger, '{DIR_R}', f'{name}:/root', additional_env={'name': name})
    if do_build_l2:
        cwd_inside = '/root/minic'
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, "/root/minicluster/bin/commands/bootstrap-host.sh")
        if not success:
            sys.exit(st['exitcode'])
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"mkdir -p {cwd_inside}")
        if not success:
            sys.exit(st['exitcode'])
        copy_cwd = [
            'archlinux-bootstrap-x86_64.tar.gz',
            'archlinux-bootstrap-x86_64.tar.gz.sig',
            'release-key.pgp',
        ]
        for f in copy_cwd:
            command_copy_files_xsh(cwd, logger, '{CWD_START}/{f}', '{name}:{cwd_inside}/', additional_env={'f': f, 'name': name, 'cwd_inside': cwd_inside})
        # bootstrap L1 image as a minicluster host 
        command_network_cmd_xsh(cwd, logger, name, False)
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; /root/minicluster/bin/commands/build-base-image.xsh --cache --handle nested-{handle} --build_nested false'")
        if not success:
            sys.exit(st['exitcode'])
        # promote embedded image from being an L2 image to being L1
        command_poweroff_image_xsh(cwd, logger, name)
    # TODO: check this and confirm that the outcomes are equivalent (minus L1 promotion)
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
    if extract_nested:
        cwd_inside = '/root/minic'
        name = get_random_name(handle) + str(get_linenumber())
        started = command_boot_image_xsh(cwd, logger, handle, name, l2_ram, True, False)
        if not started:
            sys.exit(1)
        (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, "bash -c \"pacman -Qi python >/dev/null || pacman -S --noconfirm --overwrite '*' python\"")
        if not success:
            sys.exit(st['exitcode'])
        command_copy_files_xsh(cwd, logger, '{DIR_R}', f'{name}:/root', additional_env={'name': name})
        success = extract_l2_assets(cwd, logger, handle, name, cwd_inside)
        if not success:
            sys.exit(1)
        # TODO: extract_l1_assets()
        # TODO: cleanup all intermediary files, including the L1 image
        command_poweroff_image_xsh(cwd, logger, name)
    # TODO: remove the section below
    do_extract = False
    if do_extract:
        cwd_inside = '/root/minic'
        name = get_random_name(handle) + str(get_linenumber())
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
