#!/bin/bash

start=$1
increment=$2
max_servers=$3
threads=$4
exp=$5

while read s i m t e servers; do
	for host in $servers; do
		ssh $host "/home/frojala/YCSB/remote-seq.sh $start $increment $max_servers $threads $exp" &
	done
done <<< $@
