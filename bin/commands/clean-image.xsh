#!/usr/bin/env xonsh

if __name__ == '__main__':
    d=p"$XONSH_SOURCE".resolve().parent; source f'{d}/bootstrap.xsh'
    source @(f'{d}/boot-image.xsh')
    source @(f'{d}/instance-shell.xsh')
    source @(f'{d}/poweroff-image.xsh')
    source @(f'{d}/mount-image.xsh')
    source @(f'{d}/umount-image.xsh')
    MINICLUSTER.ARGPARSE.add_argument('--handle', required=True)
    MINICLUSTER.ARGPARSE.add_argument('--repo_db', required=True)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import logging
import cluster.qmp
import psutil
import math
import random
import string

def get_files_on_disk_unaccounted(logger, cwd, handle, repo_db):
    mountpoint = command_mount_image_xsh(cwd, logger, handle, "compare")
    logger.info(f"mounted {mountpoint}")
    db_file = pf"{cwd}/{handle}-files.sqlite3"
    if db_file.exists():
	db_file.unlink()
    sqlite3 @(db_file) "create table disk_files(path, type)"
    find @(mountpoint) -printf '"%P","%y"\n' | sqlite3 @(db_file) ".import --csv /dev/stdin disk_files"
    command_umount_image_xsh(cwd, logger, f"{handle}-compare")
    sql = (
	f"attach '{repo_db}' as \"repodb\";\n"
        """
	with
        repo_files as (
                select distinct file, type from repodb.files order by file
        ),
        disk as (
                select path from disk_files where
                        path not glob 'dev/*'
                        and path not glob 'boot/*'
                        and path not glob 'run/*'
                        and path not glob 'etc/ca-certificates/extracted/cadir/*'
                        and path not glob 'etc/ca-certificates/extracted/*'
                        and path not glob 'var/lib/pacman/local/*' -- local db must stay
                        and path not glob 'var/log/*' -- TODO: remove
			and path not glob 'etc/systemd/system/*'
			and path not glob 'etc/systemd/user/*'
                        and path not glob 'etc/ssl/certs/*'
                        and path not glob 'etc/pacman.d/gnupg/*'
                        and path not glob 'etc/systemd/system/*.target.wants/*'
                        and path not glob 'usr/lib/modules/*/modules.*'
			and path not in ('etc/credstore', 'etc/credstore.encrypted', 'etc/group-', 'etc/gshadow-', 'etc/hostname', 'etc/ld.so.cache', 'etc/locale.conf')
			and path not in ('etc/localtime', 'etc/os-release', 'etc/pacman.d/gnupg', 'etc/passwd-', 'etc/shadow-')
			and path not in ('var/lib/pacman/local', 'var/lib/pacman/sync', 'etc/mkinitcpio.d/linux.preset', 'lost+found')
			and path not in ('var/cache/ldconfig', 'var/cache/private')
			and path not in ('var/lib/systemd/catalog', 'var/lib/systemd/catalog/database', 'var/lib/systemd/coredump', 'var/lib/systemd/linger', 'var/lib/systemd/pstore', 'var/lib/systemd/random-seed', 'var/lib/systemd/timers', 'var/lib/systemd/timers/stamp-archlinux-keyring-wkd-sync.timer', 'var/lib/systemd/timers/stamp-shadow.timer')
			and path not in ('etc/.updated', 'var/.updated')
			and path not in ('etc/systemd/network/80-dhcp.network')
                order by path
        )
        select disk.* from disk
        left join repo_files on repo_files.file = disk.path
        where repo_files.file is null
        order by path;"""
    )
    lines = $(sqlite3 @(db_file) @(sql)).splitlines()
    return lines

def get_random_name(handle):
    r = ''.join((''.join(random.choice(string.ascii_lowercase)) for i in range(8)) )
    return f"clean-{handle}-{r}"

def command_clean_image_xsh(cwd, logger, handle, repo_db):
    name = get_random_name(handle)
    logger.info(f"cleaning image {handle=} as {name=}")
    if True:
	pow_fac = int(math.log2(psutil.virtual_memory().available // 2**20 * 1/3))
	if pow_fac < 8:
	    pow_fac = 8
	if pow_fac > 10:
	    pow_fac = 10
	def_ram = 2**pow_fac
	started = command_boot_image_xsh(cwd, logger, handle, name, def_ram, True, False)
	if not started:
	    return False
	rm_patterns = [
	    #"rm -f /tmp/bootstrap-rootimage.sh", # TODO: this should be removed by us somewhere else
	    "bash -c 'yes | pacman -Scc'",
	    "systemd-tmpfiles --create --clean --remove --boot",
	    "find /run -type f -delete",
	    "find /etc -type f -name '*.pacnew' -delete",
	    #find / -type d -name __pycache__ rm -rf {} # problem: it seems these are included by packages
	    "rm -rf /root/.gnupg",
	    "rm -rf /root/.bash_history",
	    "rm -rf /root/.ssh",
	    "find /tmp -type d -name '.*-unix' -delete", # TODO: why is this not removing anything?
	    "find /var/lib/pacman/sync/ -type f -name '*.db' -delete", # TODO: for each of them, also disable in /etc/pacman.conf
	    "rm -rf /etc/.pwd.lock /etc/sudoers",
	    "rm -rf /var/cache/ldconfig/aux-cache",
	    "journalctl --vacuum-size=0M",
	]
	for pattern in rm_patterns:
	    logger.info(f"execute {pattern}")
	    command_instance_shell_simple_xsh(cwd, logger, name, pattern, interval=0.1, env={}, show_out=True)
	command_instance_shell_simple_xsh(cwd, logger, name, "sync", interval=0.1, env={}, show_out=True)
	command_poweroff_image_xsh(cwd, logger, name)

    files = get_files_on_disk_unaccounted(logger, cwd, handle, repo_db)
    if len(files) > 0:
	for f in files:
	    logger.warning(f)
	logger.error(f"files unaccounted for on disk: {len(files)}")
	return False

    return True

if __name__ == '__main__':
    cwd = MINICLUSTER.CWD_START

    logger = logging.getLogger(__name__)
    handle = MINICLUSTER.ARGS.handle
    repo_db = MINICLUSTER.ARGS.repo_db
    $RAISE_SUBPROC_ERROR = True
    command_clean_image_xsh(cwd, logger, handle, repo_db)
