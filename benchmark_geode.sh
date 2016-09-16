#!/bin/bash

path=$1
preamble=$2
thread=$3
server=$4
host=$(hostname | tr -d '-')

cd /home/frojala/YCSB

if [[ ! -e /home/frojala/YCSB/bin/ycsb ]]; then
	echo "YCSB not found in: /home/frojala/YCSB/bin/ycsb"
	exit 1
fi

if [[ ! -e "$path/$preamble" ]]; then
	mkdir "$path/$preamble"
fi

d_path="$path/$preamble/$preamble"S"$server"

if [[ ! -e $d_path ]]; then
       mkdir $d_path
fi

f_preamble="$preamble"S"$server"_T"$thread"

if [[ ! -e "$d_path/$f_preamble" ]]; then
	mkdir "$d_path/$f_preamble"
fi

echo "
	/home/frojala/YCSB/bin/ycsb run basic -P /home/frojala/YCSB/workloads/LTEworkload -s -t \
		-p hdrhistogram.output.path=$d_path/$f_preamble/hdr_histo_ \
		-threads $thread > $d_path/$f_preamble/$f_preamble".log"
"
echo "Done with $f_preamble on $(hostname)"

echo "Completed benchmark of $preamble "S"$server"_T"$thread on $(hostname)"
echo "" > "$d_path/$f_preamble/complete"
