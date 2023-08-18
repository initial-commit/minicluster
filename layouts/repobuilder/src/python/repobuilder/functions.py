import pygit2
import re

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
        #if pkg not in ['arm-linux-gnueabihf-ncurses', '0ad-git']:
        #    continue
        #if pkg not in ['jamomacore-git', 'pam_autologin']:
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
                    yield (pkgid, data)
    #collected_unknown_keys = list(set(collected_unknown_keys))
