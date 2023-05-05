#!/usr/bin/env xonsh

d="d1"

bin/commands/make-empty-image --handle @(d) && bin/commands/mount-image --handle @(d) && bin/commands/prepare-chroot --handle @(d)
