#! /usr/bin/env sh
# Checks that the right version of Elm is installed.

MINOR_VERSION=19
VERSION_REGEX='0\.'$MINOR_VERSION'\.[0-9]\+'

die() {
    echo $1
    echo "Elm binaries found here: https://guide.elm-lang.org/install.html"
    exit 1
}

$(elm --version > /dev/null 2>&1) || die "Elm not found"
$(elm --version | grep -q $VERSION_REGEX) || die "Wrong version of Elm. Found $(elm --version) but need 0.$MINOR_VERSION.x"

echo "Elm version $(elm --version) OK"
exit 0
