#!/usr/bin/env xonsh

d = pf"{__file__}".resolve().parent

if __name__ == '__main__':
    source f'{d}/bootstrap.xsh'
    MINICLUSTER.ARGPARSE.add_argument('--file', required=False, default=None)
    MINICLUSTER = MINICLUSTER.bootstrap_finished(MINICLUSTER)

import requests
import time
import operator
import itertools
import logging
import json

# TODO: better heuristic to choose the list of countries
def mirrors_tier_url_iter(logger, tier=1, protocols=['http', 'https'], max_score=1.5, countries=['DE', 'SE', 'CZ', 'RO', 'DK', 'AT', 'SI', 'EE', 'FI', 'FR', 'GB', 'LT', 'MD', 'NL', 'NO', 'PL', 'PT', 'SE']):
	tier_url = f"https://archlinux.org/mirrors/status/tier/{tier}/json/"
	r = requests.get(tier_url, headers={'Accept': 'application/json'})
	j = r.json()
	assert 3 == j['version']
	for mirrordata in j['urls']:
		if 'url' not in mirrordata:
			continue
		if mirrordata['protocol'] not in protocols:
			continue
		if countries and mirrordata['country_code'] not in countries:
			continue
		if not mirrordata['active']:
			logger.info(f"{mirrordata['url']} skipped because it's not active")
			continue
		if not mirrordata['delay'] or mirrordata['delay'] > 3600:
			logger.info(f"{mirrordata['url']} is lagging by seconds: {mirrordata['delay']=}")
			continue
		if mirrordata['completion_pct'] < 1:
			continue
		if not mirrordata['isos']:
			logger.info(f"{mirrordata['url']} does not mirror iso images")
			continue
		if mirrordata['score'] > 1.5:
			logger.info(f"{mirrordata['url']} has a bad score (lower is better): {mirrordata['score']=}")
			continue
		if mirrordata['duration_stddev'] > 1:
			logger.info(f"{mirrordata['url']} takes abnormally long to sync: {mirrordata['duration_stddev']=}")
			continue
		yield mirrordata


def mirrors_ranked(iter, logger, test_size_mb=5):
	data = []
	chunk_size = test_size_mb * 2**20
	for j in iter:
		try:
			r = requests.get(f"{j['url']}/lastsync", timeout=2, headers={"Range": f"bytes=0-100"})
			if r.status_code != 206:
				logger.info(f"{j['url']} does not support partial downloads or file is missing")
				continue
			j['local_connect_latency'] = r.elapsed.total_seconds()
			r = requests.get(f"{j['url']}/iso/latest/archlinux-x86_64.iso", stream=True, headers={"Range": f"bytes=0-{chunk_size}"}, timeout=2)
			if r.status_code != 206:
				logger.info(f"{j['url']} does not support partial downloads or file is missing")
				continue
			start = time.perf_counter()
			dummy = r.content
		except:
			logger.info(f"{j['url']} raised an exception")
			continue
		stop = time.perf_counter()
		j['local_download_speed'] = chunk_size / (stop-start)
		data.append({'url': j['url'], 'local_connect_latency': j['local_connect_latency'], 'local_download_speed': j['local_download_speed']})
	data = list(sorted(data, key=operator.itemgetter('local_download_speed')))
	return data

def command_get_top_mirror_xsh(logger, file=None):
	if not file:
		with open(file, 'r') as f:
			mirrors = json.load(f)
	else:
		mirrors = command_get_ranked_mirrors_xsh(logger)
	return mirrors[-1]

def command_get_ranked_mirrors_xsh(logger):
	iter = itertools.chain(mirrors_tier_url_iter(tier=1, logger=logger), mirrors_tier_url_iter(tier=2, logger=logger))
	mirrors = mirrors_ranked(iter, logger)
	return mirrors

if __name__ == '__main__':
	cwd = MINICLUSTER.CWD_START

	file = MINICLUSTER.ARGS.file
	logger = logging.getLogger(__name__)
	$RAISE_SUBPROC_ERROR = True
	mirrors = command_get_ranked_mirrors_xsh(logger)
	if not file:
		for m in mirrors:
			print(m['url'])
	else:
		fpath = pf"{cwd}/{file}".resolve()
		with open(fpath, "w") as f:
			json.dump(mirrors, f)
		logger.info(f"data written to {fpath}")
