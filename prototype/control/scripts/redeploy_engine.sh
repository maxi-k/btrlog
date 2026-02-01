#!/usr/bin/env sh

mkerr() {
    echo $1
    exit
}

test -z "$1" && mkerr "$0 <path-to-binary> [sshkey=.env/aws.key] [user=ubuntu] [path-to-ipfile=public_node_ips.txt]"

binary=$1
keyfile=${2:-".env/aws.key"}
user=${3:-"ubuntu"}
ipfile=${4:-"public_node_ips.txt"}

test -f $ipfile || mkerr "ipfile does not exist"

binary_abspath=$(realpath "$binary")
fname=$(basename $binary_abspath)

# if we're uploading a directory, zip it then ship it
# if [ -d $binary_abspath ]; then
# compress first
    test -f /tmp/$fname.zip && rm /tmp/$fname.zip
    (cd "$(dirname "$binary_abspath")" && zip -ur /tmp/$fname.zip "$(basename "$binary_abspath")")
    binary=/tmp/$fname.zip
    fname=$fname.zip
# fi

while IFS= read nodeip || [ -n "$nodeip" ]; do
    echo "syncing to $user@$nodeip:~/$binary..."
    scp -i $keyfile -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $binary $user@$nodeip:~/$fname &
done < $ipfile

echo "waiting for rsync procs to complete"
wait
