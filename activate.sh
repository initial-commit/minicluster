export PATH=$(pwd)/bin/commands:$PATH
stty rows 46 cols 189
# turn on line wrapping
printf %b '\033[?7h'
bootstrap-host.sh
pacman -Qi vim &>/dev/null || {
	pacman -S --noconfirm vim tmux tree htop dnsmasq dmidecode wget ncdu 
}
