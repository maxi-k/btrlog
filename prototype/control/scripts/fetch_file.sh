#!/usr/bin/env sh

usage() {
    echo "$0 <node-id> <output-file-path>"
    exit
}

test -z "$1" && usage
id=$1; shift
test -z "$1" && usage
fname=$1

cmd="${id}p"
ip=`sed -n $cmd ./public_node_ips.txt`

sshcmd='ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i .env/aws.key'
echo "executing rsync -e "$sshcmd" ubuntu@$ip:~/$fname $fname"
rsync -e "$sshcmd" ubuntu@$ip:~/$fname $fname
