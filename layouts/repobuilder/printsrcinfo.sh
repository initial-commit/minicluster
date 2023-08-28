#!/usr/bin/env bash

source '/usr/share/makepkg/lint_pkgbuild.sh'
source '/usr/share/makepkg/util/pkgbuild.sh'
source '/usr/share/makepkg/srcinfo.sh'

source_safe() {
	local shellopts=$(shopt -p extglob)
	shopt -u extglob

	if ! source /dev/stdin; then
		exit 193
	fi

	eval "$shellopts"
}
set -e
source_safe
pkgbase=${pkgbase:-${pkgname[0]}}
#lint_pkgbuild || exit 194
write_srcinfo_content || exit 195
