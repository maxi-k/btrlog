#!/usr/bin/env sh

mkerr() {
    echo $1
    exit
}

if [ "$1" = "help" ]; then
    mkerr "usage: $0 local-file-prefix [remote_filename=perf.csv] [public-ip-file=public_node_ips.txt]"
fi

test -z "$1" && mkerr "usage: $0 local-file-prefix [remote_filename=perf.csv] [public-ip-file=public_node_ips.txt]"

prefix=$1
filename=${2:-"perf.csv"}
infile=${3:-"public_node_ips.txt"}

sshopt='ssh -i ./.env/aws.key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
i=0
while IFS= read ip || [ -n "$ip" ]; do
    rsync -e "$sshopt" ubuntu@$ip:~/$filename $prefix-$i-$filename
    i=$((i+1))
done < $infile
