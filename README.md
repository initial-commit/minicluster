Rules
=====

* commands in `bin/` document all actual commands being executed

TODOs
=====

Today
-----

* put policy for mount and umount in overlay
* get rid of sudo
* contract all commands in only one: build-base-image
* make unmounting robust
* find a way to cache packages so that they're not downloaded again
* turn the cluster inside-out for invertion of control, i.e. turn it into a library
  * write the python code for the `minicluster` minicluster
* make the whole image and test it

Next
----

* get rid of sudo by writing an extra  `bin/programs/minicluster-bridge.xsh`
* cache packages and reuse them
  * make a package cache image with the commands, new minicluster "arch-cacher"
  * introduce this image in a "base-image" cluster
* build itself inside L1
* move to btrfs

Dependencies
-----------

pip install python-dateutil psutil

#pacman -S qemu-base arch-install-scripts nbd udisks2
pacman -S qemu-base arch-install-scripts libguestfs guestfs-tools

fusermount: option allow_other only allowed if 'user_allow_other' is set in /etc/fuse.conf
