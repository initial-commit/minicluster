import pygit2
import re
import requests
from bs4 import BeautifulSoup
from lxml import html
import dateutil.parser
from urllib.parse import urlparse
import time


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



def aur_repo_iterator(repo):
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
        #if pkg not in ['arm-linux-gnueabihf-ncurses', '0ad-git', 'jamomacore-git', 'pam_autologin']:
        #    continue
        tree = repo.revparse_single(br).tree
        if '.SRCINFO' in tree:
            lines = tree['.SRCINFO'].data.decode('utf-8').splitlines()
            meta = {}
            meta_global = []
            for line in lines:
                #print(line)
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
                    yield (pkgid, data)
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


def aurweb_pkg_iter(since_limit='1970-01-01 00:00 (UTC)'):
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
            yield data
            yielded_cnt += 1
        if newest_update and newest_update > since_limit:
            since_limit = newest_update
        precise_limit = True