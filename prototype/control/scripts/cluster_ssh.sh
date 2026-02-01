#!/usr/bin/env sh

mkerr() {
    echo $1
    exit
}

if [ "$1" = "help" ]; then
    mkerr "usage: $0 [ssh-cmd-file=cluster_ssh_commands.txt]"
fi

infile=${1:-"cluster_ssh_commands.txt"}

while IFS= read cmd || [ -n "$cmd" ]; do
    echo "executing '$cmd' in $TERMINAL"
    $TERMINAL -e $SHELL -c "$cmd -t 'tmux new'" &
done < $infile
