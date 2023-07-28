#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source @(f'{d}/bootstrap.xsh')
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
    from cluster.functions import str2bool_exc as strtobool
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import random
import string
import sys

def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"extract-tmp-{handle}-{r}"

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START
    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle
    $RAISE_SUBPROC_ERROR = True

    logger.info("mounting image")
    ro_mount = command_mount_image_xsh(cwd, logger, handle, "ro-build")
    artefacts_dir = pf"{cwd}/artefacts-{handle}"
    if artefacts_dir.exists():
        rm -rf @(artefacts_dir)
    mkdir -p @(artefacts_dir)
    files_to_copy = [
        "/boot/vmlinuz-linux",
        "/boot/initramfs-linux.img",
        "/etc/fstab",
    ]
    for fpath in files_to_copy:
        src = pf"{ro_mount}/{fpath}"
        base_name = src.name
        dst = f"{artefacts_dir}/{base_name}"
        cp @(src) @(dst)
    
    sync_dir = fp"{ro_mount}/var/lib/pacman/sync".absolute()
    cache_dir = fp"{ro_mount}/var/cache/pacman/pkg/".absolute()
    dest_db_name = f"{handle}-repo"
    dest_db_dir = fp"{artefacts_dir}/{dest_db_name}".absolute()
    command_merge_pacman_repositories_xsh(logger, sync_dir, ["core", "extra"], cache_dir, dest_db_name, dest_db_dir)
    command_umount_image_xsh(cwd, logger, f"{handle}-ro-build")
    logger.info("image unmounted")
