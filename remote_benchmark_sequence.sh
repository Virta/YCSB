#!/bin/bash

start=$1
increment=$2
max_servers=$3
threads=$4
exp=$5

while read s i m t e servers; do
	num_servers=0
	for i in $(seq $start $increment $max_servers); do
		for host in $servers; do
			ssh $host "/home/frojala/YCSB/start-servers.sh $((i - $((increment-1)))) $i" >> /home/frojala/EXPERIMENTS/$exp/server_start_logs 2>> /home/frojala/EXPERIMENTS/$exp/server_start_error_logs
		done
		for host in $servers; do
			ssh $host "/home/frojala/YCSB/benchmark_geode.sh /home/frojala/EXPERIMENTS/ $exp $threads $i $increment" &
			num_servers=$((num_servers+1))
		done
		completed_servers=0
		while [[ ! $completed_servers -eq $num_servers ]]; do
			completed_servers=0
			for host in $servers; do
				if ssh $host test -e "/home/frojala/EXPERIMENTS/$exp/$exp'S'$i/complete" ; then
					completed_servers=$((completed_servers+1))
				fi
			done
			echo "completed servers: $completed_servers"
			sleep 30
		done
	done
done <<< $@
