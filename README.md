Rules
=====

* commands in `bin/` document all actual commands being executed

TODOs
=====

Today
-----

* put policy for mount and umount in overlay
* get rid of sudo
* make unmounting robust
* find a way to cache packages so that they're not downloaded again
* turn the cluster inside-out for invertion of control, i.e. turn it into a library
  * write the python code for the `minicluster` minicluster
* make the whole image and test it

Next
----

* get rid of sudo by writing an extra  `bin/programs/minicluster-bridge.xsh`
* build itself inside L1
* move to btrfs

Dependencies
-----------

python install python-dateutil psutil

#pacman -S qemu-base arch-install-scripts nbd udisks2
pacman -S qemu-base arch-install-scripts libguestfs guestfs-tools

