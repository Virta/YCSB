#!/bin/bash

start=$1
increment=$2
max_servers=$3
threads=$4

sequence=$(seq $start $increment $max_servers)

while read s i m t servers; do
	for host in $servers; do
		ssh $host 'for i in '$sequence'; do /home/frojala/YCSB/benchmark_geode.sh /home/frojala/EXPERIMENTS/ E1S '$threads' '$i' '$increment'; done'

	done

done <<< $@
