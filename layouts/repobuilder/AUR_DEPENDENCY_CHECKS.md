makepkg --printsrcinfo
makepkg --printsrcinfo | grep -w makedepends | cut -d'=' -f 2 | sed 's/[[:blank:]]*//'
makepkg --printsrcinfo | grep -w depends | cut -d'=' -f2 | sed 's/[[:blank:]]*//'

makepkg --verifysource
