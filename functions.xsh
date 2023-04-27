import sys
import argparse
import logging

import glob
import collections
import psutil

class CommandArgumentParser(argparse.ArgumentParser):
	parser = None

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.add_argument('--log', default='WARNING')
	
	def parse_known_args(self):
		return super().parse_known_args(sys.argv)
	
	def get_logger(self, name):
		parsed = self.parse_known_args()
		loglevel=parsed[0].log
		numeric_level = getattr(logging, loglevel.upper(), None)
		logging.basicConfig(level=numeric_level, format='[%(asctime)s] [%(levelname)-8s] [%(name)s] - %(message)s')
		return logging.getLogger(name)
	

def get_logger(name=None):
	parser=argparse.ArgumentParser()
	parser.add_argument('--log', default='WARNING')
	parsed = parser.parse_known_args(sys.argv)
	loglevel=parsed[0].log

	numeric_level = getattr(logging, loglevel.upper(), None)
	logging.basicConfig(level=numeric_level, format='[%(asctime)s] [%(levelname)-8s] [%(name)s] - %(message)s')
	return logging.getLogger(name)

def nbd_block_devices():
    logger = get_logger(__name__)
    devices_and_partitions = {}

    util_parts = psutil.disk_partitions()
    mountpoint_map = {d.mountpoint: d.device for d in util_parts}
    device_map = {d.device: d.mountpoint for d in util_parts}
    mountpoint_util_parts_map = {util_parts[i].mountpoint: i for i in range(len(util_parts))}
    device_util_parts_map = {util_parts[i].device: i for i in range(len(util_parts))}
    logger.debug(f"{util_parts=}")
    logger.debug(f"{mountpoint_map=}")
    logger.debug(f"{device_map=}")
    logger.debug(f"{mountpoint_util_parts_map=}")
    logger.debug(f"{device_util_parts_map=}")
    Disk = collections.namedtuple("Disk", ["name", "partitions", "mountpoints"])
    for blockdev_stat in glob.glob('/sys/block/*/stat'):
        blockdev_dir = blockdev_stat.rsplit('/', 1)[0]
	name = blockdev_dir.rsplit('/', 1)[-1]
	if not name.startswith('nbd'):
		continue
	partitions = []
	mountpoints = []
        for part_stat in glob.glob(blockdev_dir + '/*/stat'):
	    p_name = part_stat.rsplit('/', 2)[-2]
	    partitions.append(p_name)
	    devpath = f"/dev/{p_name}"
	    if devpath in device_util_parts_map:
		i = device_util_parts_map[devpath]
		mountpoints.append(util_parts[i].mountpoint)
	d = Disk(name, partitions, mountpoints)
	devices_and_partitions[name] = d
    return devices_and_partitions
