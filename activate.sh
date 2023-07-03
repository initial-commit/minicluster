export PATH=$(pwd)/bin/commands:$PATH
stty rows 46 cols 189
bootstrap-host.sh
pacman -S --noconfirm vim tmux tree htop dnsmasq dmidecode wget ncdu
export LIBGUESTFS_TRACE=0
#libguestfs-test-tool
#qemu-img create -f qcow2 d2.qcow2 4G
#libguestfs-make-fixed-appliance

#/usr/bin/supermin --build --verbose --if-newer --lock /var/tmp/.guestfs-0/lock  --copy-kernel -f ext2 --host-cpu x86_64 /usr/lib/guestfs/supermin.d -o /var/tmp/.guestfs-0/appliance.d
