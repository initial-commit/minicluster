Rules
=====

* commands in `bin/` document all actual commands being executed

TODOs
=====

Today
-----

* contract all commands in only one: build-base-image
* turn the cluster inside-out for invertion of control, i.e. turn it into a library
  * write the python code for the `minicluster` minicluster
* make the whole image and test it

Next
----

* create the concept of a cluster "project" where files reside, instead of CWD
  * for fstab (instead of /tmp)
  * for kernel and initramfs (instead of cmd)
  * for qcow2 images
  * for downloaded packages (instead of /tmp)
  * for meta-data about the project
    * disk specifications
* create the concept of cluster layout and config
* add inbound networking via different networking modes
* cache packages and reuse them
  * make a package cache image with the commands, new minicluster "arch-cacher"
  * introduce this image in a "base-image" cluster
* build itself inside L1
* move to btrfs
* detect differences between layout spec and actual spec and issue commands


Bigger plans
------------

* an architecture based on command pattern
* the commands are submitted to a daemon, who takes care of the actual execution and error handling
* connect clusters on different hardware machines and make them act as one

Dependencies
-----------

pacman -S qemu-base arch-install-scripts libguestfs guestfs-tools

fusermount: option allow_other only allowed if 'user_allow_other' is set in /etc/fuse.conf


python pip: xonsh and xpip autoxsh
