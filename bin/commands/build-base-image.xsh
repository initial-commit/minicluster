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
    source @(f'{d}/extract-image-assets.xsh')
    import math
    import psutil
    from cluster.functions import str2bool_exc as strtobool
    from cluster.functions import get_linenumber
    from cluster.functions import PipeTailer
    import cluster.qmp
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--cache', action="store_true", default=False, help="Use local package cache")
    MINICLUSTER.ARGPARSE.add_argument('--initial_build', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--build_nested', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--extract_nested', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    MINICLUSTER.ARGPARSE.add_argument('--extract_l1_assets', nargs='?', type=lambda b:bool(strtobool(b)), const=False, default=True, metavar='true|false')
    # TODO: when exiting cleanly, cleanup
    def_ram = 2**int(math.log2(psutil.virtual_memory().available // 2**20 * 3/3))
    MINICLUSTER.ARGPARSE.add_argument('--ram', default=def_ram, type=int)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import random
import string
import sys
import sqlite3
import contextlib

def extract_l2_assets(cwd, logger, handle, name, cwd_inside):
    """In this function we want to use a lot of bash execution.

    This ensures that we operate on L2 using the next-state
    version of the underlying system.

    Doing so tests as part of the process the L1 image.
    While the L1 image we operate on here is not used, the package
    repository will be extracted and used later on (when building minicluster)
    """
    nested_handle = f'nested-{handle}'
    # copy code itself into vm
    written = command_copy_files_xsh(cwd, logger, '{DIR_R}', f'{name}:/root', additional_env={'name': name})
    if not written:
        logger.error("could not copy minicluster into vm")
        return False
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"mkdir -p '{cwd_inside}'")
    if not success:
        return False
    # extract assets (kernel, initramfs, fstab, pacman repo)
    tailer = PipeTailer(f'{cwd}/pci-serial1.pipe.out', logger, "extract_l2_assets")
    tailer.start()
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, (
        f"bash -c 'set -o pipefail; cd {cwd_inside};"
        f"/root/minicluster/bin/commands/extract-image-assets.xsh --handle {nested_handle} 2>&1 | "
        "tee -a -p /dev/ttyS4; e=$?; echo -e -n \"\\x0\"{,,,,} | tr -d \" \" >> /dev/ttyS4; exit $e'"))
    if not success:
        logger.error(f"fail to execute extract-image-assets.xsh")
        return False
    logger.info(f"joining tailer for extract-image-assets")
    tailer.join()
    # shrink L2 image
    conn = cluster.qmp.Connection(f'{cwd}/qga-{name}.sock', logger)
    l2_image_path = f'{cwd_inside}/{nested_handle}.qcow2'
    l2_image_path_temp = f'{cwd_inside}/small-{nested_handle}.qcow2'
    l2_image_stat_before = conn.path_stat(l2_image_path)
    assert l2_image_stat_before is not None, "Could not get stat of L2 image: {l2_image_path}"
    l2_size_before = l2_image_stat_before['ST_SIZE']
    logger.info(f"{l2_image_stat_before=}")
    tailer = PipeTailer(f'{cwd}/pci-serial1.pipe.out', logger, "clean L2 image inside L1")
    tailer.start()
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, (
        f"bash -c 'set -o pipefail; cd {cwd_inside};"
        f"/root/minicluster/bin/commands/clean-image.xsh --handle {nested_handle} --repo_db artefacts-{nested_handle}/{nested_handle}-repo/{nested_handle}-repo.sqlite3 2>&1 | "
        "tee -a -p /dev/ttyS4; e=$?; echo -e -n \"\\x0\"{,,,,} | tr -d \" \" >> /dev/ttyS4; exit $e'"))
    logger.info(f"joining tailer after clean image")
    tailer.join()
    if not success:
        logger.error(f"could not clean L2 image")
        return False
    # TODO: read sector size from disk spec
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, (
    f"bash -c 'set -o pipefail; cd {cwd_inside};"
    f"qemu-img convert -S 4096 -o preallocation=off -O qcow2 {l2_image_path} {l2_image_path_temp} 2>&1 | "
    f"tee -a -p /dev/ttyS4 && cp {l2_image_path_temp} {cwd_inside}/artefacts-{nested_handle}/{nested_handle}.qcow2; e=$?; "
    "echo -e -n \"\\x0\"{,,,,} | tr -d \" \" >> /dev/ttyS4; exit $e'"))
    if not success:
        logger.error(f"could not convert image")
        return False
    l2_image_after = f"{cwd_inside}/artefacts-{nested_handle}/{nested_handle}.qcow2"
    l2_image_base = pathlib.Path(l2_image_after).with_suffix('')
    l2_image_stat_after = conn.path_stat(l2_image_after)
    assert l2_image_stat_after is not None, "Could not get stat of L2 image: {l2_image_path}"
    l2_size_after = l2_image_stat_after['ST_SIZE']
    logger.info(f"{l2_size_before=} {l2_size_after=}")
    size_diff = l2_size_before - l2_size_after
    assert size_diff > 0, f"The image has not gotten any smaller: {size_diff=}"
    assert size_diff > 500 * 2**20, f"Cleaning up did not shrink the image by more than 500MB, but: {size_diff / 2**20} MB"
    # sanity checks
    # using the commands like this also serves as systems testing
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; /root/minicluster/bin/commands/boot-image.xsh --image {l2_image_base} --name t1 --interactive false'")
    if not success:
        logger.error(f"could not boot image for testing")
        return False
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; /root/minicluster/bin/commands/test-vm.xsh --name t1'")
    if not success:
        logger.error(f"image testing failed")
        return False
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; /root/minicluster/bin/commands/poweroff-image.xsh --name t1'")
    if not success:
        logger.error(f"image testing failed")
        return False
    # TODO: assert that there are no more files unaccounted for in inside_cwd
    # copy artefacts
    command_mount_image_xsh(cwd, logger, handle, "ro-build")
    ro_dir_on_l0 = fp"{cwd}/{handle}-ro-build/".absolute()
    artefacts_dir_ro = fp"{ro_dir_on_l0}/{cwd_inside}/artefacts-nested-{handle}".absolute()
    rsync --delete -a --info=stats2,misc1,flist0 @(artefacts_dir_ro) @(f"{cwd}/")
    command_umount_image_xsh(cwd, logger, f"{handle}-ro-build")
    return True

def extract_l1_assets(cwd, logger, handle, name, l2_artefacts_dir):
    cwd_inside = '/root/minic'
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, f"bash -c 'cd {cwd_inside}; rm -rf {handle}-repo; mkdir {handle}-repo'")
    assert success, f"Making directory for {cwd_inside}/{handle}-repo"
    logger.info(f"building aggregated pacman repo for L1, please wait, it takes around 60 seconds")
    tailer = PipeTailer(f'{cwd}/pci-serial1.pipe.out', logger, "build L2 pacman repo")
    tailer.start()
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, (
        f"bash -c 'set -o pipefail; cd {cwd_inside}; "
        f"/root/minicluster/bin/commands/merge-pacman-repositories.xsh --source_db_dir /var/lib/pacman/sync/ --db_names core extra --source_pkg_cache /var/cache/pacman/pkg/ --dest_db_name {handle}-repo --dest_db_dir {handle}-repo --only_explicit true 2>&1 | "
        "tee -a -p /dev/ttyS4; e=$?; echo -e -n \"\\x0\"{,,,,} | tr -d \" \" >> /dev/ttyS4; exit $e'"))
    if not success:
        logger.error(f"failed to create the L1 package repo")
        return False
    logger.info(f"joining tailer thread")
    tailer.join()
    mountpoint = command_mount_image_xsh(cwd, logger, handle, "ro-build")
    assert mountpoint is not None, f"Mountpoint not there: {mountpoint=}"
    logger.debug(f"{mountpoint=} for extracting L1 repo for minicluster")

    l1_db_dir = fp"{mountpoint}/{cwd_inside}/{handle}-repo".absolute()
    l1_db_name = f"{handle}-repo"
    l1_sqlite_p = l1_db_dir / f"{l1_db_name}.sqlite3"
    nested_handle = f"nested-{handle}"
    l2_db_dir = pf"{l2_artefacts_dir}" / f"{nested_handle}-repo/"
    l2_sqlite_p = pf"{l2_artefacts_dir}" / f"{nested_handle}-repo/{nested_handle}-repo.sqlite3"

    assert l1_sqlite_p.exists(), f"L1 db file does not exist: {l1_sqlite_p=}"
    assert l2_sqlite_p.exists(), f"L2 db file does not exist: {l2_sqlite_p=}"
    # TODO: also copy /var/lib/pacman/local/{pkg} in a file smallrepo.files.tar and symlink smallrepo.files to it
    echo rsync --delete -av --info=stats2,misc1,flist0 @(l1_db_dir) @(f"{cwd}/tmp/")
    time.sleep(10)
    rsync --delete -av --info=stats2,misc1,flist0 @(l1_db_dir) @(f"{cwd}/tmp/")
    l1_db_dir = pf"{cwd}/tmp/{handle}-repo".absolute()
    l1_sqlite_p = l1_db_dir / f"{l1_db_name}.sqlite3"
    assert l1_sqlite_p.exists(), f"L1 db file does not exist: {l1_sqlite_p=}"
    #time.sleep(5)
    command_umount_image_xsh(cwd, logger, f"{handle}-ro-build")
    #time.sleep(5)
    db = sqlite3.connect(':memory:')
    db.execute(f"attach '{l1_sqlite_p}' as \"l1\";")
    db.execute(f"attach '{l2_sqlite_p}' as \"l2\";")
    #with contextlib.closing(db.cursor()) as cur:
    #    cur.execute(f"attach 'file:{l1_sqlite_p}' as \"l1\";")
    #    cur.execute(f"attach 'file:{l2_sqlite_p}' as \"l2\";")
    with contextlib.closing(db.cursor()) as cur:
        res = cur.execute(f"SELECT COUNT(*) FROM l1.pkginfo;")
        (cnt_pkg_l1, ) = res.fetchone()
        logger.info(f"{cnt_pkg_l1=}")
    with contextlib.closing(db.cursor()) as cur:
        res = cur.execute(f"SELECT COUNT(*) FROM l2.pkginfo;")
        (cnt_pkg_l2, ) = res.fetchone()
        logger.info(f"{cnt_pkg_l2=}")
    with contextlib.closing(db.cursor()) as cur:
        res = cur.execute(f"select COUNT(*) FROM l1.pkginfo INNER JOIN l2.pkginfo ON l1.pkginfo.pkgname = l2.pkginfo.pkgname AND l1.pkginfo.pkgver = l2.pkginfo.pkgver;")
        (cnt_pkg_common, ) = res.fetchone()
        logger.info(f"{cnt_pkg_common=}")
    assert cnt_pkg_l1 > 0
    assert cnt_pkg_l2 > 0
    assert cnt_pkg_common > 0
    assert cnt_pkg_l1 > cnt_pkg_l2
    assert cnt_pkg_common == cnt_pkg_l2
    return True
    
def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"build-tmp-{handle}-{r}"

def proc_initial_build(cwd, logger, handle, diskspec, cache):
    command_make_empty_image_xsh(cwd, logger, handle, d)
    command_mount_image_xsh(cwd, logger, handle)
    success = command_prepare_chroot_xsh(cwd, logger, handle, cache)
    if not success:
        logger.error(f"failed to prepare chroot for {handle=}")
        return None
    # TODO: make a backup
    cp -a @(f"{handle}.qcow2") @(f"pristine-{handle}.qcow2")
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

def proc_build_nested(cwd, logger, handle, l2_ram):
    # TODO: poweroff would benefit from a context
    name = get_random_name(handle) + str(get_linenumber())
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
    tailer = PipeTailer(f'{cwd}/pci-serial1.pipe.out', logger, "build L2 inside L1")
    tailer.start()
    # TODO: instead of using tee, set up logging to ttyS4 in bootstrap
    (success, st) = command_instance_shell_simple_xsh(cwd, logger, name, (
        f"bash -c 'set -o pipefail; cd {cwd_inside};"
        f"/root/minicluster/bin/commands/build-base-image.xsh --cache --handle nested-{handle} --build_nested false --extract_nested false --extract_l1_assets false 2>&1 | "
        "tee -a -p /dev/ttyS4; e=$?; echo -e -n \"\\x0\"{,,,,} | tr -d \" \" >> /dev/ttyS4; exit $e'"))
    if not success:
        logger.error(f"failed to build nested L2 image nested-{handle} in {name}:{cwd_inside}/")
        command_poweroff_image_xsh(cwd, logger, name)
        started = False
        return False
    # turn off the L1 image
    if started:
        command_poweroff_image_xsh(cwd, logger, name)
    logger.info(f"joining tailer thread")
    tailer.join()
    return True

if __name__ == '__main__':
    do_initial_build = MINICLUSTER.ARGS.initial_build
    do_build_nested = MINICLUSTER.ARGS.build_nested
    extract_nested = MINICLUSTER.ARGS.extract_nested
    extract_assets = MINICLUSTER.ARGS.extract_l1_assets
    vm_ram = MINICLUSTER.ARGS.ram
    l2_ram = int(vm_ram / 2)
    if do_build_nested:
        assert vm_ram >= 8192, f"Not enough RAM to build L1 and L2 {vm_ram=} {l2_ram=}"

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

    if do_build_nested:
        success = proc_build_nested(cwd, logger, handle, l2_ram)
        if not success:
            logger.error(f"building of L2 image inside L1 failed: {handle=}")
            sys.exit(2)

    if extract_nested:
        cwd_inside = '/root/minic'
        name = get_random_name(handle) + str(get_linenumber())
        started = command_boot_image_xsh(cwd, logger, handle, name, vm_ram, True, False)
        if not started:
            logger.error(f"failed to start {handle=} with ram {vm_ram=} and {name=} for the purpose of extracting image")
            sys.exit(3)
        # TODO: copy DIR_R to /root
        success = extract_l2_assets(cwd, logger, handle, name, cwd_inside)
        if not success:
            logger.error(f"failed to extract L2 assets")
            command_poweroff_image_xsh(cwd, logger, name)
            sys.exit(4)
        command_poweroff_image_xsh(cwd, logger, name)

    if extract_assets:
        logger.info(f"START: extract_assets - the L1 pacman repo, deduplicated")
        l2_artefacts_dir = pf"{cwd}/artefacts-nested-{handle}"
        name = get_random_name(handle) + str(get_linenumber())
        started = command_boot_image_xsh(cwd, logger, handle, name, vm_ram, True, False)
        if not started:
            logger.error(f"failed to start {handle=} with ram {vm_ram=} and {name=} for the purpose of extracting L1 repo")
            sys.exit(5)
        written = command_copy_files_xsh(cwd, logger, '{DIR_R}', f'{name}:/root', additional_env={'name': name})
        success = extract_l1_assets(cwd, logger, handle, name, l2_artefacts_dir)
        if not success:
            logger.error("failed to extract L1 pacman repo")
            command_poweroff_image_xsh(cwd, logger, name)
            sys.exit(5)
        command_poweroff_image_xsh(cwd, logger, name)

    if cache:
        logger.info(f"SUCCESS inside!!!!")
