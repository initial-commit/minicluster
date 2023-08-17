#!/usr/bin/env python

import pygit2
import re

repo = pygit2.Repository('./.git')

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
META_LIST = [
    'arch',
    'b2sums',
    'b2sums_aarch64',
    'b2sums_armv6h',
    'b2sums_armv7h',
    'b2sums_armv7l',
    'b2sums_i686',
    'b2sums_pentium4',
    'b2sums_x86_64',
    'backup',
    'checkdepends',
    'cksums',
    'cksums_i686',
    'cksums_x86_64',
    'conflicts',
    'conflicts_aarch64',
    'conflicts_i486',
    'conflicts_x86_64',
    'depend',
    'depends',
    'depends_aarch64',
    'depends_armv7h',
    'depends_i686',
    'depends_pentium4',
    'depends_x86_64',
    'groups',
    'license',
    'makedepends',
    'makedepends_aarch64',
    'makedepends_armv6h',
    'makedepends_armv7h',
    'makedepends_i686',
    'makedepends_x86_64',
    'md5sum_i686',
    'md5sums',
    'md5sums_aarch64',
    'md5sums_arm',
    'md5sums_armv5',
    'md5sums_armv5h',
    'md5sums_armv6h',
    'md5sums_armv7',
    'md5sums_armv7h',
    'md5sums_armv7l',
    'md5sums_i386',
    'md5sums_i686',
    'md5sums_pentium4',
    'md5sums_ppcle_64',
    'md5sums_s390_64',
    'md5sums_x86',
    'md5sums_x86_32',
    'md5sums_x86_64',
    'noextract',
    'optdepends',
    'optdepends_aarch64',
    'optdepends_i686',
    'optdepends_x86_64',
    'options',
    'provides',
    'provides_aarch64',
    'provides_i486',
    'provides_x86_64',
    'replaces',
    'sha1sums',
    'sha1sums_aarch64',
    'sha1sums_arm',
    'sha1sums_armv5tel',
    'sha1sums_armv6h',
    'sha1sums_armv6l',
    'sha1sums_armv7h',
    'sha1sums_armv7l',
    'sha1sums_armv8h',
    'sha1sums_i386',
    'sha1sums_i686',
    'sha1sums_x86_64',
    'sha224sums',
    'sha256sums',
    'sha256sums_aarch',
    'sha256sums_aarch64',
    'sha256sums_amd64',
    'sha256sums_arm',
    'sha256sums_arm64',
    'sha256sums_armel',
    'sha256sums_armhf',
    'sha256sums_armv5',
    'sha256sums_armv5h',
    'sha256sums_armv6',
    'sha256sums_armv6h',
    'sha256sums_armv7',
    'sha256sums_armv7h',
    'sha256sums_armv7l',
    'sha256sums_armv8',
    'sha256sums_armv8h',
    'sha256sums_i386',
    'sha256sums_i486',
    'sha256sums_i586',
    'sha256sums_i686',
    'sha256sums_loong64',
    'sha256sums_mips',
    'sha256sums_mips64',
    'sha256sums_mips64el',
    'sha256sums_mipsel',
    'sha256sums_pentium4',
    'sha256sums_ppc64',
    'sha256sums_ppc64le',
    'sha256sums_riscv64',
    'sha256sums_s390x',
    'sha256sums_x86',
    'sha256sums_x86_64',
    'sha256sums_x86_64_v3',
    'sha265sums',
    'sha384sums',
    'sha384sums_x86_64',
    'sha512sums',
    'sha512sums_aarch64',
    'sha512sums_arm',
    'sha512sums_arm64',
    'sha512sums_arm6h',
    'sha512sums_arm7h',
    'sha512sums_armhf',
    'sha512sums_armv5h',
    'sha512sums_armv6h',
    'sha512sums_armv7h',
    'sha512sums_armv7l',
    'sha512sums_armv8h',
    'sha512sums_i386',
    'sha512sums_i686',
    'sha512sums_loong64',
    'sha512sums_loongarch64',
    'sha512sums_pentium4',
    'sha512sums_x86_64',
    'source',
    'source_aarch',
    'source_aarch64',
    'source_amd64',
    'source_arm',
    'source_arm64',
    'source_arm6h',
    'source_arm7h',
    'source_armel',
    'source_armhf',
    'source_armv5',
    'source_armv5h',
    'source_armv5tel',
    'source_armv6',
    'source_armv6h',
    'source_armv6l',
    'source_armv7',
    'source_armv7h',
    'source_armv7l',
    'source_armv8',
    'source_armv8h',
    'source_i386',
    'source_i486',
    'source_i586',
    'source_i686',
    'source_loong64',
    'source_loongarch64',
    'source_mips',
    'source_mips64',
    'source_mips64el',
    'source_mipsel',
    'source_pentium4',
    'source_ppc64',
    'source_ppc64le',
    'source_ppcle_64',
    'source_riscv64',
    'source_s390_64',
    'source_s390x',
    'source_x86',
    'source_x86_32',
    'source_x86_64',
    'source_x86_64_v3',
    'validpgpkeys',
]

META_LIST.sort()

for x in META_LIST:
    print(f"'{x}',")

import sys
sys.exit(1)



def aur_repo_iterator(repo):
    collected_unknown_keys = []
    branches = repo.raw_listall_branches(pygit2.GIT_BRANCH_REMOTE)
    kv_r = re.compile(r'^\s*(?P<key>[^\s=]+)\s*=\s*(?P<val>.*)$')
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
        print(pkg)
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
                elif k in META_LIST:
                    if k not in meta:
                        meta[k] = []
                    meta[k].append(v)
                else:
                    collected_unknown_keys.append(k)
                    do_package = False
                    break
                    #raise Exception(f"Encountered unknown key for package {k=} {pkg=} with value {v=}")
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
    collected_unknown_keys = list(set(collected_unknown_keys))
    print(collected_unknown_keys)

i = 0
for (pkg, meta) in aur_repo_iterator(repo):
    #print(i, pkg, meta)
    print(i, pkg)
    i += 1
