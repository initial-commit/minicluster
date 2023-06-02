#!/usr/bin/env xonsh

d="d1"

make-empty-image.xsh --handle @(d) && mount-image.xsh --handle @(d) && prepare-chroot.xsh --handle @(d)
