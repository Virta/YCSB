#!/bin/bash

path=$1
preamble=$2
threads=$3
server=$4
increment=$5
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

for th_counter in $(seq 1 $increment $threads);do
	if [[ $th_counter -lt 10 ]]; then
		f_preamble=$preamble"S"$server"_T0$th_counter"
	else
		f_preamble=$preamble"S"$server"_T$th_counter"
	fi

	if [[ ! -e "$d_path/$f_preamble" ]]; then
		mkdir "$d_path/$f_preamble"
	fi


	/home/frojala/YCSB/bin/ycsb run basic -P /home/frojala/YCSB/workloads/LTEworkload -s -t \
		-p hdrhistogram.output.path=$d_path/$f_preamble/hdr_histo_ \
		-threads $th_counter > $d_path/$f_preamble/"$f_preamble".log

	echo "Done with $f_preamble on $(hostname)"

done
echo "Completed benchmark of "$preamble"S"$server" on $(hostname)"
echo "" > $d_path/complete
