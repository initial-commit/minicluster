import pygit2
import re
import requests
from bs4 import BeautifulSoup
from lxml import html
import dateutil.parser
from urllib.parse import urlparse
import time
import difflib
import tarfile
import stat
import threading
import base64
import psutil
import io
import cluster.functions
import contextlib
import json


META_REQUIRED = [
    'pkgbase',
    'pkgname',
    'pkgdesc',
    'pkgver',
    'url',
    'pkgrel',
]

META_UNIQUE = [
    'pkgbase',
    'pkgname',
    'pkgdesc',
    'pkgver',
    'url',
    'pkgrel',
    'install',
    'epoch',
    'changelog',
]
ARCHITECTURES = [
    'aarch',
    'aarch64',
    'amd64',
    'arm',
    'arm64',
    'arm6h',
    'arm7h',
    'armel',
    'armhf',
    'armv5',
    'armv5h',
    'armv5tel',
    'armv6',
    'armv6h',
    'armv6l',
    'armv7',
    'armv7h',
    'armv7l',
    'armv8',
    'armv8h',
    'i386',
    'i486',
    'i586',
    'i686',
    'loong64',
    'loongarch64',
    'mips',
    'mips64',
    'mips64el',
    'mipsel',
    'pentium4',
    'ppc64',
    'ppc64le',
    'ppcle_64',
    'riscv64',
    's390_64',
    's390x',
    'x86',
    'x86_32',
    'x86_64',
    'x86_64_v3',
]

METALIST_CHECKSUM_PREFIXES = [
    'b2sums_',
    'cksums_',
    'md5sum_',  # TODO: validate
    'md5sums_',
    'sha1sums_',
    'sha224sums_',
    'sha256sums_',
    'sha265sums_',
    'sha384sums_',
    'sha512sums_',
]

_metalist_architecture_prefixes = [
    'b2sums_',
    'cksums_',
    'conflicts_',
    'depends_',
    'makedepends_',
    'md5sum_',  # TODO: validate
    'md5sums_',
    'optdepends_',
    'provides_',
    'sha1sums_',
    'sha224sums_',
    'sha256sums_',
    'sha265sums_',
    'sha384sums_',
    'sha512sums_',
    'source_',
]


META_LIST = [
    'arch',
    'b2sums',
    'backup',
    'checkdepends',
    'cksums',
    'conflicts',
    'depend',  # TODO: validate
    'depends',
    'groups',
    'license',
    'makedepends',
    'md5sums',
    'noextract',
    'optdepends',
    'options',
    'provides',
    'replaces',
    'sha1sums',
    'sha224sums',
    'sha256sums',
    'sha265sums',
    'sha384sums',
    'sha512sums',
    'source',
    'validpgpkeys',
]

def iter_all_git_tree_files(tree, prefix=''):
    for gitobj in tree:
        if gitobj.type == pygit2.GIT_OBJ_BLOB:
            yield (prefix, gitobj)
        elif gitobj.type == pygit2.GIT_OBJ_TREE:
            yield from iter_all_git_tree_files(gitobj, f"{prefix}/{gitobj.name}".lstrip('/'))


class DictWithMetaKeys(dict):
    """
    Dictionary with tuples as keys.

    The first item in the tuple is already unique, but the rest of the tuple serves as additional meta-data.
    All this aside, this dictionary works as a regular dictionary.
    """
    realkeys = None

    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        if not isinstance(key, tuple):
            keys = self._real_keys()
            if key in keys:
                key = keys[key]
        val = dict.__getitem__(self, key)
        return val

    def __setitem__(self, key, val):
        if not isinstance(key, tuple):
            keys = self._real_keys()
            if key in keys:
                key = keys[key]
        self.realkeys = None
        dict.__setitem__(self, key, val)

    def __repr__(self):
        dictrepr = dict.__repr__(self)
        return '%s(%s)' % (type(self).__name__, dictrepr)

    def update(self, *args, **kwargs):
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def _real_keys(self):
        if not self.realkeys:
            self.realkeys = {}
            for h, t in map(lambda x: (x[0], x[1:]), self):
                self.realkeys[h] = tuple([h, *t])
        return self.realkeys

def aur_repo_iterator_simple(repo, include_only=set()):
    yielded = 0
    branches = repo.raw_listall_branches(pygit2.GIT_BRANCH_REMOTE)
    for br in branches:
        br = br.decode('utf-8')
        pkg = br.split('/', 1)[1]
        if pkg in ['HEAD', 'main', ]:
            continue
        if len(include_only) > 0 and pkg not in include_only:
            continue
        #if pkg not in ['gn-bin', '0ad-boongui', 'arm-linux-gnueabihf-ncurses', '0ad-git', 'jamomacore-git', 'pam_autologin', 'mediasort', 'linux-binder']:
        #   continue
        rev = repo.revparse_single(br)
        tree = rev.tree
        entries = DictWithMetaKeys({})
        for prefix, gitobj in iter_all_git_tree_files(tree):
            k = gitobj.name
            if prefix:
                k = f"{prefix}/{gitobj.name}"
            entries[(k, gitobj.filemode)] = gitobj.data
        yield (pkg, entries, False)
        yielded += 1
        #if yielded == 1000:
        #    break
    yield (None, None, True)

# TODO: remove, deprecated function
def arch_parse_srcinfo(pkgbase, srcinfo, logger):
    kv_r = re.compile(r'^\s*(?P<key>[^\s=]+)\s*=\s*(?P<val>.*)$')
    meta_list = META_LIST
    for arch in ARCHITECTURES:
        for pref in _metalist_architecture_prefixes:
            meta_list.append(f"{pref}{arch}")
    meta_list = list(set(meta_list))
    lines = srcinfo.decode('utf-8', 'backslashreplace').strip()
    lines = lines.splitlines()
    meta = {}
    meta_global = []
    for line in lines:
        if "pkgname" in line and not line.startswith("pkgname"):
            continue
        m = kv_r.match(line)
        if not m:
            continue
        groups = m.groupdict()
        k = groups['key']
        v = groups['val']
        logger.info(f"{pkgbase=} {k=} {v=}")


# TODO: deprecated function, remove
def aur_repo_iterator(repo, extractor, errorlogger):
    #collected_unknown_keys = []
    branches = repo.raw_listall_branches(pygit2.GIT_BRANCH_REMOTE)
    kv_r = re.compile(r'^\s*(?P<key>[^\s=]+)\s*=\s*(?P<val>.*)$')
    meta_list = META_LIST
    for arch in ARCHITECTURES:
        for pref in _metalist_architecture_prefixes:
            meta_list.append(f"{pref}{arch}")
    meta_list = list(set(meta_list))
    for br in branches:
        do_package = True
        br = br.decode('utf-8')
        pkg = br.split('/', 1)[1]
        if pkg in ['HEAD', 'main', ]:
            continue
        #if pkg not in ['gn-bin', 'arm-linux-gnueabihf-ncurses', '0ad-git', 'jamomacore-git', 'pam_autologin', 'mediasort']:
        #    continue
        rev = repo.revparse_single(br)
        tree = rev.tree
        own_srcinfo = None
        errorlogger.info(f"==================================== {pkg=}")
        if 'PKGBUILD' in tree:
            lines = tree['PKGBUILD'].data.decode('utf-8', 'backslashreplace')
            own_srcinfo = extractor(lines.encode('utf-8'))
            if own_srcinfo:
                own_srcinfo = own_srcinfo.rstrip().splitlines()
                for line in own_srcinfo:
                    errorlogger.debug(f".SRCINFO-SHOULD-BE:{line}")
            # TODO: copy file to extractor and printsrcinfo inside via input-data
        if '.SRCINFO' in tree:
            lines = tree['.SRCINFO'].data.decode('utf-8', 'backslashreplace').strip()
            lines = lines.splitlines()
            # TODO: compare .SRCINFO to makepkg --printsrcinfo and issue a warning
            meta = {}
            meta_global = []
            for line in lines:
                errorlogger.debug(f".SRCINFO:{line}")
                if "pkgname" in line and not line.startswith("pkgname"):
                    continue
                m = kv_r.match(line)
                if not m:
                    continue
                groups = m.groupdict()
                k = groups['key']
                v = groups['val']
                #print("\t", k, v)
                if k in ['pkgname', 'pkgbase'] and meta:
                    meta_global.append(meta)
                    #print(f"{meta=}")
                    #print(f"{meta_global=}")
                    meta = {}
                if k in META_UNIQUE:
                    if k not in ['url'] and pkg not in ['pam_autologin']:
                        assert k not in meta, f"{k=} already exists in {meta=}, cannot set {v=}"
                    meta[k] = v
                elif k in meta_list:
                    if k not in meta:
                        meta[k] = []
                    meta[k].append(v)
                else:
                    #collected_unknown_keys.append(k)
                    #do_package = False
                    #break
                    raise Exception(f"Encountered unknown key for package {k=} {pkg=} with value {v=}")
            if not do_package:
                continue
            if own_srcinfo:
                diffs = difflib.unified_diff([v.strip() for v in own_srcinfo if v.strip()], [v.strip() for v in lines if v.strip()], fromfile=f'{pkg}/.SRCINFO-SHOULD_BE', tofile=f'{pkg}/.SRCINFO', lineterm='', n=1)
                for diffline in diffs:
                    errorlogger.error(diffline)
            meta_global.append(meta)
            is_pkgbase = lambda meta: 'pkgname' not in meta and 'pkgbase' in meta
            pkgbase_type = list(map(is_pkgbase, meta_global))
            i = -1
            groups = []
            for t in pkgbase_type:
                if t:
                    i += 1
                groups.append(i)
            pkgbase_type = groups
            #print(f"{meta_global=}")
            #print(f"{pkgbase_type=}")
            meta_grouped = []
            for i, meta in enumerate(meta_global):
                group_index = pkgbase_type[i]
                if len(meta_grouped) == group_index:
                    meta_grouped.insert(group_index, [])
                meta_grouped[group_index].append(meta)
                #print(i, group_index, meta)
            for group in meta_grouped:
                if len(group) == 1:
                    group.append({'pkgname': group[0]['pkgbase']})
                meta_base = group[0]
                meta_tail = group[1:]
                for meta in meta_tail:
                    data = {**meta_base, **meta}
                    pkgid = f"{data['pkgname']}-{data['pkgver']}-{data['pkgrel']}"
                    if 'epoch' in data:
                        epoch = int(data['epoch'])
                        if epoch > 0:
                            pkgid = f"{data['pkgname']}-{epoch}:{data['pkgver']}-{data['pkgrel']}"
                    yield (pkgid, data, False)
    yield (None, None, True)
    #collected_unknown_keys = list(set(collected_unknown_keys))


def get_soup_stats(soup):
    multiple_spaces = re.compile(r"\s+")
    stats = soup.select_one('#pkglist-results .pkglist-stats > p').get_text().strip()
    stats = multiple_spaces.sub(' ', stats)
    return tuple(map(int, re.findall(r'\b\d+\b', stats)))


def get_tree_header(tree):
    header = tree.xpath('//thead/tr/th')
    header = [e.text_content().strip() for e in header]
    header = [re.sub(r'\W|^(?=\d)', '', h) for h in header]
    return list(map(str.lower, (filter(None, header))))


def get_tr_data(tr, header):
    tds = tr.xpath('td')
    tds = dict(zip(header, tds))
    lastupdated = tds['lastupdated']
    flagged = 'flagged' == lastupdated.get('class')

    vals = [td.text_content().strip() for td in tds.values()]
    data = dict(zip(header, vals))
    data['votes'] = int(data.get('votes', 0))
    data['popularity'] = float(data.get('popularity', 0.0))
    data['lastupdated'] = dateutil.parser.parse(data.get('lastupdated', '1970-01-01 00:00 (UTC)'), fuzzy=True)
    data['flagged'] = flagged
    pkgid = f"{data['name']}-{data['version']}"
    version = data.pop('version')
    # TODO: this regex is duplicated, extract
    verparse = re.compile(r'((?P<epoch>\d+):)?(?P<version>.+?)-(?P<rel>\d+)')
    m = verparse.match(version)
    assert m is not None, f"Version could not be parsed {version=} {data=}"
    verparse = m.groupdict()
    data['pkgrel'] = int(verparse['rel'])
    data['pkgver'] = verparse['version']
    if 'epoch' in verparse and verparse['epoch']:
        data['epoch'] = int(verparse['epoch'])
    data['pkgid'] = pkgid
    return data


def get_next_url(soup, currenturl):
    if not currenturl:
        return None
    navbar = soup.select_one('div.pkglist-stats:first-child p.pkglist-nav').encode_contents()

    #print(navbar)
    tree = html.fromstring(str(navbar))
    next_link = tree.xpath('.//a[contains(text(), "Next")]/@href')
    if not next_link or len(next_link) != 1:
        return None
    parsed_uri = urlparse(currenturl)
    result = '{uri.scheme}://{uri.netloc}{next_link}'.format(uri=parsed_uri, next_link=next_link[0])
    return result


def aurweb_pkg_iter_simple(perpage=2500, since_limit=None, precise_limit=False):
    if isinstance(since_limit, str):
        since_limit = dateutil.parser.parse(since_limit, fuzzy=True)
    keywords = ''
    nexturl = f'https://aur.archlinux.org/packages?O=0&SeB=nd&K={keywords}&outdated=&SB=l&SO=d&PP={perpage}&submit=Go'
    packages_start_count = None
    while nexturl:
        req = requests.get(nexturl)

        soup = BeautifulSoup(req.text, "lxml")
        if not packages_start_count:
            (packages_start_count, current_page, total_pages) = get_soup_stats(soup)
            packages_count = packages_start_count
        else:
            (packages_count, current_page, total_pages) = get_soup_stats(soup)

        results_tbl = soup.select_one('table.results')
        tree = html.fromstring(str(results_tbl))
        header = get_tree_header(tree)
        for tr in tree.xpath('//tbody/tr'):
            data = get_tr_data(tr, header)
            lastupdated = data['lastupdated']
            if precise_limit and lastupdated <= since_limit:
                nexturl = None
                break
            yield (packages_count, data)
            if not precise_limit and lastupdated < since_limit:
                nexturl = None
                break
        nexturl = get_next_url(soup, nexturl)
        if nexturl:
            time.sleep(2)


def aurweb_pkg_iterator(since_limit='1970-01-01 00:00 (UTC)'):
    if not since_limit:
        since_limit = '1970-01-01 00:00 (UTC)'
    if isinstance(since_limit, str):
        since_limit = dateutil.parser.parse(since_limit, fuzzy=True)
    data = {'lastupdated': since_limit}
    yielded_cnt = 0
    newest_update = None
    start_packages_count = None
    first_run = True
    precise_limit = False
    while first_run or yielded_cnt > 0:
        first_run = False
        yielded_cnt = 0
        for packages_count, data in aurweb_pkg_iter_simple(since_limit=since_limit, precise_limit=precise_limit):
            if not start_packages_count:
                start_packages_count = packages_count
            if newest_update and data['lastupdated'] > newest_update:
                newest_update = data['lastupdated']
            if not newest_update:
                newest_update = data['lastupdated']
            yield (data, False)
            yielded_cnt += 1
        if newest_update and newest_update > since_limit:
            since_limit = newest_update
        precise_limit = True
    yield (None, True)

def get_files_as_tar(fh, pkgbase, files):
    with tarfile.open(fileobj=fh, mode='w:gz') as tar:
        for (fname, fmode), fdata in files.items():
            if fname in ['.SRCINFO']:
                continue
            info = tarfile.TarInfo(f"tmp/{pkgbase}/{fname}")
            info.mtime=time.time()
            if fmode == stat.S_IFLNK:
                info.type = tarfile.SYMTYPE
                info.linkname = fdata.decode('utf-8')
            else:
                info.size = len(fdata)
                info.mode = fmode
            tar.addfile(info, io.BytesIO(fdata))
    fh.seek(0)
    return fh


class Extractor(object):
    WORKDIR = "/tmp"
    def __init__(self, conn, logger, workdir):
        self.conn = conn
        self.logger = logger.getChild(self.__class__.__name__)
        self.lock = threading.Lock()
        self.lock_ns = 0
        self.lock_count = 0
        self.WORKDIR = workdir

    def exec_start(self, pkgbase, files):
        pkgdir = f"/{self.WORKDIR}/{pkgbase}".replace('//', '/')
        arch_inside = f"/tmp/{pkgbase}.tar.gz"
        fh = io.BytesIO()
        fh = get_files_as_tar(fh, pkgbase, files)
        t1 = time.thread_time_ns()
        with self.lock:
            self.lock_ns += time.thread_time_ns() - t1
            self.lock_count += 1
            self.conn.write_to_vm(fh, arch_inside)
        t1 = time.thread_time_ns()
        with self.lock:
            self.lock_ns += time.thread_time_ns() - t1
            self.lock_count += 1
            resp = self.conn.guest_exec_wait(["bash", "-c", f"tar xfz {arch_inside} && rm -rf {arch_inside}"])
        t1 = time.thread_time_ns()
        with self.lock:
            self.lock_ns += time.thread_time_ns() - t1
            self.lock_count += 1
            st = self.conn.guest_exec(f'/{self.WORKDIR}/printsrcinfo.sh', env=[f"PKGDIR={pkgdir}"])
            #self.logger.debug(f"initial status for printsrcinfo {pkgbase=} {st=}")
            return (int(st), True)
        return (0, False)

    def get_stats(self):
        lock_cont_ms = self.lock_ns / 1000 / 1000
        if self.lock_count:
            lock_cont_avg = lock_cont_ms / self.lock_count
        else:
            lock_cont_avg = 0
        return (lock_cont_ms, lock_cont_avg)

    def exec_result(self, pid, pkgbase):
        t1 = time.thread_time_ns()
        with self.lock:
            self.lock_ns += time.thread_time_ns() - t1
            self.lock_count += 1
            st = self.conn.guest_exec_status(pid)
            if st['exited']:
                t1 = time.thread_time_ns()
                self.lock_ns += time.thread_time_ns() - t1
                self.lock_count += 1
                st2 = self.conn.guest_exec_wait(f"rm -rf /{self.WORKDIR}/{pkgbase}")
            return st


class MonitoringThread(threading.Thread):
    def __init__(self, queue_in, queue_out, storage_thread, logger):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.logger = logger.getChild(self.__class__.__name__)
        self.keep_monitoring = threading.Event()
        self.keep_monitoring.set()
        self.git_processed = None
        self.put_duration = None
        self.storage_thread = storage_thread
        super().__init__(name=self.__class__.__name__)

    def run(self):
        has_data = True
        no_data_start = time.time()
        last_logged = None
        prev_queue_in = None
        prev_queue_out = None
        keep_monitoring = True
        monitor_delta_sec = 2
        last_git_processed = None
        processing_speed_history = []
        while self.keep_monitoring.is_set():
            queue_in = self.queue_in.qsize()
            queue_out = self.queue_out.qsize()
            do_logging = True
            if queue_in == prev_queue_in and queue_out == prev_queue_out:
                do_logging = False
            if last_logged and time.time() - last_logged < monitor_delta_sec:
                do_logging = False
            if do_logging:
                hist_size = 5
                mib = psutil.Process().memory_info().rss / 1024 ** 2
                approx_git_processed = self.git_processed
                processing_speed = None
                put_dur = self.put_duration
                put_avg = None
                if put_dur:
                    put_avg = put_dur / approx_git_processed
                if last_git_processed:
                    delta_processed = approx_git_processed - last_git_processed
                    processing_speed = delta_processed / (time.time() - last_logged)
                if processing_speed:
                    processing_speed_history.append(processing_speed)
                    processing_speed_history = processing_speed_history[-hist_size:]
                if len(processing_speed_history) == hist_size:
                    avg = sum(processing_speed_history)/len(processing_speed_history)
                    sleep_dur = self.storage_thread.sleep_duration
                    fact = queue_in / avg
                    if avg - queue_in > 6:
                        self.logger.info(f"avg speed {avg=} {avg-queue_in} {sleep_dur=} {fact=}")
                        self.logger.info(f"speed too high")
                    elif avg - queue_in < -6:
                        self.logger.info(f"avg speed {avg=} {avg-queue_in} {sleep_dur=} {fact=}")
                        self.logger.info(f"speed too low, increasing sleep time in storage thread (consumer)")
                        sleep_dur *= fact
                        sleep_dur = min(sleep_dur, 1.000)
                        self.storage_thread.sleep_duration = sleep_dur
                        self.logger.info(f"new sleep duration {sleep_dur=}")
                self.logger.info(f"{queue_in=} {queue_out=} {mib=}MiB {approx_git_processed=} {processing_speed=} items/s {put_avg=}")
                last_logged = time.time()
                last_git_processed = approx_git_processed
            prev_queue_in = queue_in
            prev_queue_out = queue_out
            if self.keep_monitoring.is_set():
                time.sleep(0.001)
        self.logger.info("stop monitoring")


class ExtractorThread(threading.Thread):
    def __init__(self, name, queue_in, queue_out, extractor, logger, cond):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.extractor = extractor
        self.cond = cond
        self.logger = logger.getChild(self.__class__.__name__)
        super().__init__(name=name)

    def run(self):
        last = False
        while not last:
            error_lines = []
            (pkgbase, files, last)= self.queue_in.get()
            diffs = []
            if last:
                self.logger.info(f"THREAD {self.name} SENTINEL DETECTED, NOOP")
                self.queue_in.task_done()
                self.logger.info(f"THREAD {self.name} FINISHED")
                return
            # TODO: here process with extractor
            #local.t1 = time.thread_time()
            #self.logger.info(f"THREAD {self.name} starts extracting {pkgbase=}")
            (pid, success) = self.extractor.exec_start(pkgbase, files)
            if success:
                exited = False
                while not exited:
                    st = self.extractor.exec_result(pid, pkgbase)
                    self.logger.debug(f"exec_result during loop for {pkgbase=} {pid=} {st=}")
                    exited = st['exited']
                    if not exited:
                        time.sleep(0.010)
                #local.dur = time.thread_time() - local.t1
                #self.logger.info(f"duration: {local.dur=}")
                if 'err-data' in st:
                    st['err-data'] = base64.b64decode(st['err-data'])
                    if len(st['err-data']):
                        error_lines = st['err-data'].decode('utf-8').strip().splitlines()
                        for err_line in error_lines:
                            self.logger.debug(f"problems for pkg {pkgbase=} {err_line}")
                if st['exitcode'] == 0:
                    data = base64.b64decode(st['out-data'])
                    files['.SRCINFO-ORIGINAL'] = files['.SRCINFO']
                    files['.SRCINFO'] = data

            # TODO: further processing with a function
            self.logger.debug(f"THREAD {self.name} before putting in output: {pkgbase}")
            self.queue_out.put((pkgbase, files, error_lines))
            self.logger.debug(f"THREAD {self.name} after putting in output: {pkgbase}")
            self.queue_in.task_done()
            self.logger.debug(f"THREAD {self.name} PROCESSED ITEM IN queue: {pkgbase}")


class StorageThread(threading.Thread):
    buffer_pkginfo = {}
    buffer_dependencies = {}
    buffer_errors = {}
    buffer_files = {}
    checksum_keys = ['md5sums', 'sha224sums', 'sha256sums', 'sha265sums', 'sha384sums', 'sha512sums', 'sha1sums', 'b2sums', 'cksums', ]
    dependency_keys = ['requires', 'makedepends', 'depends', 'provides', 'conflicts', 'optdepends', 'checkdepends', 'replaces', 'depend', ]
    dry_run = False
    no_diff = False
    # adapted dynamically by the monitoring thread with a cap of 1s
    sleep_duration = 0.010

    def __init__(self, db, queue_out, logger):
        self.queue_out = queue_out
        self.logger = logger.getChild(self.__class__.__name__)
        self.items_stored = 0
        self.db = db
        self.do_store = threading.Event()
        self.do_store.set()
        for pref in METALIST_CHECKSUM_PREFIXES:
            for arch in ARCHITECTURES:
                self.checksum_keys.append(f"{pref}{arch}")
        t = []
        for arch in ARCHITECTURES:
            for reltype in self.dependency_keys:
                t.append(f"{reltype}_{arch}")
        self.dependency_keys.extend(t)
        super().__init__(name=self.__class__.__name__)

    def run_profiled(self):
        import cProfile
        profiler = cProfile.Profile()
        try:
            return profiler.runcall(StorageThread.profiled_run, self)
        finally:
            profiler.dump_stats('myprofile-%d.profile' % (self.ident,))

    def run(self):
        last_stored = None
        first_stored = None
        qsize_out = self.queue_out.qsize()
        meta_list = META_LIST
        for arch in ARCHITECTURES:
            for pref in _metalist_architecture_prefixes:
                meta_list.append(f"{pref}{arch}")
        meta_list = list(set(meta_list))
        while self.do_store.is_set() or qsize_out > 0:
            qsize_out = self.queue_out.qsize()
            if qsize_out == 0:
                time.sleep(self.sleep_duration)
                qsize_out = self.queue_out.qsize()
                continue
            (pkgbase, files, error_lines) = self.queue_out.get()
            if not first_stored:
                first_stored = time.time()
            self.logger.info(f"STORING: {pkgbase=}")
            self.acknowledge_package(pkgbase, files, error_lines, meta_list)
            self.queue_out.task_done()
            qsize_out = self.queue_out.qsize()
            self.items_stored += 1
            last_stored = time.time()
        storing_duration = None
        avg_per_sec = None
        if last_stored and first_stored:
            storing_duration = last_stored - first_stored
            avg_per_sec = self.items_stored / storing_duration
        self.flush_db()
        self.logger.info(f"Finished storing after {storing_duration=}s {self.items_stored=} {avg_per_sec=}")
        assert len(self.buffer_pkginfo) == 0
        assert len(self.buffer_dependencies) == 0
        assert len(self.buffer_errors) == 0
        assert len(self.buffer_files) == 0

    def acknowledge_package(self, pkgbase, files, error_lines, meta_list):
        if self.dry_run:
            return True
        tags_to_ignore = set(['curl', 'empty', 'compilation_terminated', 'gpg_key_not_changed', ])
        srcinfo = files['.SRCINFO'].decode('utf-8', 'backslashreplace').splitlines()
        if '.SRCINFO-ORIGINAL' in files and not self.no_diff:
            srcinfo_original = files['.SRCINFO-ORIGINAL'].decode('utf-8', 'backslashreplace').splitlines()
            lines_original = [v.strip() for v in srcinfo_original if v.strip()]
            lines_new = [v.strip() for v in srcinfo if v.strip()]
            diffs = difflib.unified_diff(lines_original, lines_new, fromfile=f'{pkgbase}/.SRCINFO-ORIGINAL', tofile=f'{pkgbase}/.SRCINFO', lineterm='', n=1)
            for line in diffs:
                self.logger.warning(f"DIFF {pkgbase}: {line}")
            for line in srcinfo_original:
                self.logger.debug(f".SRCINFO-ORIGINAL {pkgbase}: {line}")
            for line in srcinfo:
                self.logger.debug(f".SRCINFO {pkgbase}: {line}")
        for line in error_lines:
            (tags, meta) = self.aur_errorline_tags(line)
            if tags_to_ignore.intersection(tags):
                continue
            if len(tags) != 0 or len(meta) != 0:
                self.logger.info(f"PARSED ERROR: {pkgbase=} {tags=} {meta=} {line=}")
            else:
                self.logger.warning(f"{pkgbase=} unhandled line {line=}")

        srcinfo_parser = SrcinfoParser(meta_list)
        norm_pkgs_info = srcinfo_parser.parse_srcinfo(pkgbase, srcinfo)
        for pkg_info in norm_pkgs_info:
            pkgname = pkg_info['pkgname']
            try:
                dependencies = self.normalize_deps(pkg_info)
            except:
                self.logger.error(f"exception for package {pkgbase}")
                raise
            #for deptype, deps in dependencies.items():
            #    for dep in deps:
            #        self.logger.info(f"SRCINFO PARSED: {pkgbase=} {pkgname=} {deptype=} {dep=}")
            #self.logger.info(f"{pkg_info=}")
            self.buffer_pkginfo[pkgname] = pkg_info
            self.buffer_dependencies[pkgname] = dependencies
        self.buffer_errors[pkgbase] = error_lines
        self.buffer_files[pkgbase] = files
        if len(self.buffer_pkginfo) >= 1000:
            self.flush_db()

    def _flush_db_pkginfo(self):
        to_flush = []
        #self.logger.info(f"flushing to db")
        #self.logger.info(f"===========================================================")
        buffer = []
        pkg_bases = set()
        for pkgname, norm_meta in self.buffer_pkginfo.items():
            db_row = {
                'pkgbase': norm_meta.pop('pkgbase', None),
                'pkgname': norm_meta.pop('pkgname', None),
                'pkgdesc': norm_meta.pop('pkgdesc', None),
                'pkgver': norm_meta.pop('pkgver', None),
                'pkgrel': norm_meta.pop('pkgrel', None),
                'url': norm_meta.pop('url', None),
                'arch': norm_meta.pop('arch', None),
                'license': norm_meta.pop('license', None),
                'options': norm_meta.pop('options', None),
                'backup': norm_meta.pop('backup', None),
                'pgpsig': norm_meta.pop('validpgpkeys', None),
                'group': norm_meta.pop('groups', None),
                'epoch': norm_meta.pop('epoch', None),
                'noextract': norm_meta.pop('noextract', None),
                'reponame': 'aur',
            }
            pkg_bases.add(db_row['pkgbase'])
            # TODO: store this in the database
            install = norm_meta.pop('install', None)
            changelog = norm_meta.pop('changelog', None)
            if db_row['epoch']:
                pkgid = f"{db_row['pkgname']}-{db_row['epoch']}:{db_row['pkgver']}-{db_row['pkgrel']}"
            else:
                pkgid = f"{db_row['pkgname']}-{db_row['pkgver']}-{db_row['pkgrel']}"
                db_row['epoch'] = None
            db_row['pkgid'] = pkgid
            checksums = {}
            for k, v in norm_meta.items():
                if k in self.checksum_keys:
                    checksums[k] = v
            for k, v in checksums.items():
                norm_meta.pop(k)
            sources = {}
            for k, v in norm_meta.items():
                if k == 'source' or k.startswith('source_'):
                    sources[k] = v
            for k, v in sources.items():
                norm_meta.pop(k)
            db_row['sources'] = sources
            if len(norm_meta) != 0:
                self.logger.error(f"{pkgname} {norm_meta=}")
            list_columns = {k:json.dumps(v) for k,v in db_row.items() if isinstance(v, list)}
            db_row |= list_columns
            dict_columns = {k:json.dumps(v) for k,v in db_row.items() if isinstance(v, dict)}
            db_row |= dict_columns
            buffer.append(db_row)

        with self.db:
            with contextlib.closing(self.db.cursor()) as cur:
                placeholders = ('?,' * len(pkg_bases))[:-1]
                sql = f"DELETE FROM pkginfo where reponame='aur' AND pkgbase IN ({placeholders})"
                cur.execute(sql, list(pkg_bases))
                sql = ("INSERT INTO pkginfo(pkgid, reponame, pkgbase, pkgname, pkgdesc, pkgver, pkgrel, url, arch, license, options, pgpsig, \"group\", sources, epoch, noextract)"
                    "VALUES(:pkgid, :reponame, :pkgbase, :pkgname, :pkgdesc, :pkgver, :pkgrel, :url, :arch, :license, :options, :pgpsig, :group, :sources, :epoch, :noextract)")
                cur.executemany(sql, buffer)

    def flush_db(self):
        if len(self.buffer_pkginfo):
            self._flush_db_pkginfo()
            self.buffer_pkginfo = {}
            self.buffer_dependencies = {}
            self.buffer_errors = {}
            self.buffer_files = {}

    def normalize_deps(self, pkg_info):
        dependencies = {}
        for k in self.dependency_keys:
            if k in pkg_info:
                raw_dep = list(filter(None, pkg_info.pop(k)))
                if raw_dep:
                    try:
                        dependencies[k] = cluster.functions.depend_parse(raw_dep)
                    except:
                        do_raise = False
                        if len(raw_dep) == 1 and ' ' in raw_dep:
                            raw_dep = raw_dep.split()
                            try:
                                dependencies[k] = cluster.functions.depend_parse(raw_dep)
                            except:
                                do_raise = True
                        if do_raise:
                            raise
        return dependencies

    def aur_errorline_tags(self, line):
        tags = []
        meta = {}
        all_tags = [
            ('curl', r'^\s*%\s+Total\s+%\s+Received\s+%\s+Xferd\s+Average\s+Speed\s+Time\s+Time\s+Time\s+Current$'),
            ('curl', r'^\s+Dload\s+Upload\s+Total\s+Spent\s+Left\s+Speed$'),
            ('empty', r'^\s*$'),
            ('curl', r'^\s*\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s*--:--:--\s+--:--:--\s+--:--:--\s+\d+$'),
            ('curl', r'^\s*\d+\s*\d+k?\s+\d+\s+\d+\s+\d+\s+\d+\s+\dk?\s+\d+\s+\d+:\d+:\d+\s+--:--:--\s+\d+:\d+:\d+\s+\d+k?$'),
            ('curl', r'^[0-9:\sk-]+$'),
            ('no_file_or_dir', r'^(?P<buildfile>[^:]+): line (?P<line>[^:]+): (?P<file>[^:]+): No such file or directory$'),
            ('command_not_found', r'^(?P<buildfile>[^:]+): line (?P<line>[^:]+): (?P<command>[^:]+): command not found$'),
            ('cannot_change_locale', r'^(?P<buildfile>[^:]+): line (?P<line>\d+): warning: setlocale: (?P<variable>[^:]+): cannot change locale \((?P<locale>.+)\)$'),
            ('gcc_execvp_error', r"^gcc: fatal error: cannot execute '(?P<command>.+)': execvp: No such file or directory$"),
            ('compilation_terminated', r'^compilation terminated.$'),
            ('cat_no_file', r"^cat: (?P<file>[^:]+): No such file or directory$"),
            ('sed_cant_read', r"sed: can't read (?P<file>[^:]+): No such file or directory"),
            ('package_not_found', r"error: package '(?P<package>[^']+)' was not found"),
            ('pkgbuild_generic_line_error', r"^PKGBUILD: line (?P<line>[^:]+): (?P<message>.+)$"),
            ('gpg_keybox_created', r"^gpg: keybox '(?P<keybox>[^']+)' created$"),
            ('gpg_trustdb_created', r"^gpg: (?P<trustdb>[^:]+): trustdb created$"),
            ('gpg_counter', r"^gpg: (?P<message>[^:]+): (?P<count>\d+)$"),
            ('gpg_key_imported', r'^gpg: key (?P<key_id>[^:]+): public key "(?P<contact>[^"]+)" imported$'),
            ('gpg_key_not_changed', r'^gpg: key (?P<key_id>[^:]+): "(?P<contact>[^"]+)" not changed$'),
            ('gpg_missing_key', r'gpg: key (?P<key_id>[^:]+): 1 signature not checked due to a missing key'),
            ('gpg_no_trusted_key_found', '^gpg: no ultimately trusted keys found$'),
        ]
        for tag, reg in all_tags:
            m = re.match(reg, line)
            if not m:
                continue
            meta[tag] = m.groupdict()
            tags.append(tag)
        return (set(tags), meta)


class SrcinfoParser(object):
    def __init__(self, meta_list):
        self.kv_r = re.compile(r'^\s*(?P<key>[^\s=]+)\s*=\s*(?P<val>.*)$')
        self.meta_list = meta_list
        super().__init__()

    def parse_srcinfo(self, pkg, srcinfo_lines):
        meta = {}
        meta_global = []
        for line in srcinfo_lines:
            if "pkgname" in line and not line.startswith("pkgname"):
                continue
            m = self.kv_r.match(line)
            if not m:
                continue
            groups = m.groupdict()
            k = groups['key']
            v = groups['val']
            if k in ['pkgname', 'pkgbase'] and meta:
                meta_global.append(meta)
                meta = {}
            if k in META_UNIQUE:
                if k not in ['url']:
                    assert k not in meta, f"{k=} already exists in {meta=}, cannot set {v=}"
                meta[k] = v
            elif k in self.meta_list:
                if k not in meta:
                    meta[k] = []
                meta[k].append(v)
            else:
                #collected_unknown_keys.append(k)
                #do_package = False
                #break
                raise Exception(f"Encountered unknown key for package {k=} {pkg=} with value {v=}")
        meta_global.append(meta)
        is_pkgbase = lambda meta: 'pkgname' not in meta and 'pkgbase' in meta
        pkgbase_type = list(map(is_pkgbase, meta_global))
        packages = []
        i = -1
        groups = []
        for t in pkgbase_type:
            if t:
                i += 1
            groups.append(i)
        pkgbase_type = groups
        meta_grouped = []
        for i, meta in enumerate(meta_global):
            group_index = pkgbase_type[i]
            if len(meta_grouped) == group_index:
                meta_grouped.insert(group_index, [])
            meta_grouped[group_index].append(meta)
        for group in meta_grouped:
            if len(group) == 1:
                group.append({'pkgname': group[0]['pkgbase']})
            meta_base = group[0]
            meta_tail = group[1:]
            for meta in meta_tail:
                data = {**meta_base, **meta}
                packages.append(data)
        return packages

def upsert_aur_package(rawbatch, db, logger):
    tags_to_ignore = set(['curl', 'empty', 'compilation_terminated', 'gpg_key_not_changed', ])
    logger.info(f"storing pkgbase in db {len(rawbatch)}")
    for (pkgbase, files, error_lines) in rawbatch:
        srcinfo = files['.SRCINFO']
        srcinfo_original = files['.SRCINFO']
        if '.SRCINFO-ORIGINAL' in files:
            srcinfo_original = files['.SRCINFO-ORIGINAL']
        logger.debug(f"=========== {pkgbase} {len(srcinfo)=} {len(srcinfo_original)=}")
        for line in error_lines:
            (tags, meta) = aur_errorline_tags(line)
            if tags_to_ignore.intersection(tags):
                continue
            if len(tags) != 0 or len(meta) != 0:
                logger.info(f"PARSED ERROR: {pkgbase=} {tags=} {meta=} {line=}")
            else:
                logger.warning(f"{pkgbase=} unhandled line {line=}")

        norm_pkgs_info = parse_srcinfo(pkgbase, srcinfo, logger)
        for pkg_info in norm_pkgs_info:
            logger.info(f"{pkgbase=} {pkg_info=}")

    return True

    buffer = []
    buffer_dependencies = []
    for rawdata in rawbatch:
        pkginfo = rawdata.pop('pkginfo')
        dependencies = rawdata.pop('dependencies')
        logger.info(f"{dependencies=}")
        for deptype, depvals in dependencies.items():
            logger.info(f"===========================================")
            logger.info(f"{pkginfo['pkgname']}\t{deptype}\t{depvals=}")
            depvals = list(filter(None, depvals))
            try:
                vals = cluster.functions.depend_parse(depvals)
                logger.info(f"INITIAL {vals=}")
            except AssertionError:
                logger.error(f"{deptype=} {depvals=} {pkginfo=}")
                raise
            #logger.info(f"{vals=}")
            for depdict in vals:
                depdict['deptype'] = deptype
                depdict['pkgname'] = pkginfo['pkgname']
                buffer_dependencies.append(depdict)
        #logger.info(f"{dependencies=}")
        checksums = rawdata.pop('checksums')
        noextract = rawdata.pop('noextract')
        backups = rawdata.pop('backups')
        assert len(rawdata) == 0, f"Unhandled keys in rawdata: {rawdata.keys()}"
        values = {
            'pkgid': pkginfo['pkgid'],
            'reponame': 'aur',
            'pkgname': pkginfo['pkgname'],
            'pkgbase': pkginfo['pkgbase'],
            'pkgver': pkginfo['pkgver'],
            'pkgrel': float(pkginfo['pkgrel']),
            'epoch': pkginfo.get('epoch', None),
            'pkgdesc': pkginfo.get('pkgdesc', None),
            'url': pkginfo.get('url', None),
            'arch': json.dumps(pkginfo.get('arch', None)),
            'license': json.dumps(pkginfo.get('license', None)),
            'options': json.dumps(pkginfo.get('options', None)),
            'source': json.dumps(pkginfo.get('source', None)),
            'checksums': json.dumps(checksums),
            'noextract': json.dumps(pkginfo.get('noextract', None)),
        }
        logger.info(f"{values=}")
        buffer.append(values)
    with db:
        with contextlib.closing(db.cursor()) as cur:
            sql = ("INSERT INTO pkginfo(pkgid, reponame, pkgname, pkgbase, pkgver, pkgrel, epoch, pkgdesc, url, arch, license, options, source, checksums, noextract)"
            "VALUES(:pkgid, :reponame, :pkgname, :pkgbase, :pkgver, :pkgrel, :epoch, :pkgdesc, :url, :arch, :license, :options, :source, :checksums, :noextract)")
            cur.executemany(sql, buffer)
    with db:
        with contextlib.closing(db.cursor()) as cur:
            sql = ("INSERT INTO dependencies(pkgname, deptype, otherpkg, operator, version, reason)"
            "VALUES(:pkgname, :deptype, :otherpkg, :operator, :version, :reason)")
            cur.executemany(sql, buffer_dependencies)
