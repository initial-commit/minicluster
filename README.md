Introduction
============

Minicluster is a library for setting up infrastructure.

It can be used in various ways:

* as a platform-engineering tool for developers
* to test processes like: upgrade paths, backup restoration
* for black-box testing and system testing (also in failure scenarios)
* to develop cloud-based Linux images
* for setting up and operating homelabs
* for safe deployment to production of provably working systems
* for testing dedicated hardware by giving control over the kernel and kernel
  drivers
* as the name suggests, all of the above in a cluster, including features like
  failover, backup, restore, observability (logging, monitoring, health-checks)
  and more

By combining these aspects within the same organization, it can cover the full
needs, from development to deploying software into production.

Minicluster is optimized for

* developer experience
* gaining knowledge
* robust, reproducible environments
* promoting the infrastructure to production with peace of mind
* observability

Minicluster has commands to do basic things. However, to model complex
pipelines/processes, you'll have to write python code.

Advantages
==========

WIP

* it's a library, so you write just code in python (more languages once the API is stable)
* it fosters knowledge by tackling the problems at the most fundamental level,
  instead of building abstractions on top (it doesn't use yaml or the like)

Disadvantages
=============

WIP

Example Use-Cases
=================

The project is currently in the bootstrapping phase, examples will be added here.

Installation
============

Requirements
------------

* ArchLinux, current release
* 8-12 GB RAM; minicluster adapts its ram usage based on what is freely available; still, at least 8 GB RAM is ideal
* 20 GB free disk space

Set up xonsh
------------

```
pipx ensurepath
exit # then start up your shell again
pipx install --include-deps 'xontrib-ergopack[onepath,prompt,dev]'
xpip install 'xonsh[full]' psutil python-dateutil pyzstd requests 
# optional, as root: chsh -s /home/[username]/.local/bin/xonsh [username] # replace [username] with your username
```

Now configure xonsh with the following `~/.config/xonsh/rc.xsh`:
```
from xonsh.xontribs import xontribs_load
xontribs_load([
#        'whole_word_jumping',
        'ergopack',
#        'autoxsh',
#        'ssh_agent',
#        #'cmd_done',
])
```

Setup steps for minicluster itself
----------------------------------

Since we're in the early stages and we don't even have a release yet, the steps necessary are for tinkerers.

* clone the repository
* Execute as root the commands listed in `bin/commands/bootstrap-host.sh`; Please review and understand them first
* Start your shell, and if it's not xonsh, run `xonsh`
* `cd minicluster` # this executes the commands in `.autoxsh` if you've
  configured xonsh that way; the very first time you do this, you will be
  prompted to accept the commands
* if you don't use xonsh as a shell and autoxsh, then execute: `export
  PATH=$PATH:``pwd``/bin/commands`
* `cd ~/.cache/; mkdir minicluster; cd minicluster`
* `build-base-image.xsh --handle d1` - this can take 20-40 minutes, depending also on your internet speed.

At the end of the process, you will have three artefacts:

* L2 image with the base arch installation; `artefacts-nested-d1/nested-d1.qcow2` and the kernel and fstab files: `artefacts-nested-d1/fstab` (for debugging, not really used), `artefacts-nested-d1/initramfs-linux.img`, `artefacts-nested-d1/vmlinuz-linux` (the kernel image and initramfs are used on L0 by kvm/qemu to boot the VM, bypassing the VMs own boot loader)
* `artefacts-nested-d1/nested-d1-repo/` the repository used to put together the L2 image
* `tmp/d1-repo/` - the repository which could be used to construct a minicluster-capable VM on top of the L2 image

Not that `d1` above is whatever you used as a handle for the command `build-base-image.xsh` above.

If `build-base-image.xsh` fails, see HACKING.md for diagnosing.

FAQs
====

WIP

* why archlinux, isn't it too unstable?
* isn't it less secure to use a rolling release distro like archlinux?

License
=======

Minicluster is licensed under the GNU AGPLv3, see LICENSE.txt for details.

For exceptions and/or business oportunities, contact me at 
`Flavius Aspra <flavius.as+minicluster@gmail.com>`.

```
    Minicluster - craft your clusters easily and reliably.
    Copyright (C) 2003  Flavius-Adrian ASPRA <flavius.as+minicluster@gmail.com>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
```
