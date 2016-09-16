#!/bin/bash

start=$1
increment=$2
max_servers=$3
threads=$4
exp=$5

function run_benchmark {
   while read server servers; do
	for th_counter in $(seq 1 $increment $threads); do
                for host in $servers; do
                        ssh $host "/home/frojala/YCSB/benchmark_geode.sh /home/frojala/EXPERIMENTS/ $exp $th_counter $server" &
                        num_servers=$((num_servers+1))
                done
                completed_servers=0
                while [[ ! $completed_servers -eq $num_servers ]]; do
                        completed_servers=0
                        for host in $servers; do
                                if ssh $host test -e "/home/frojala/EXPERIMENTS/$exp/$exp"S"$server/$exp"S"$server"T"$th_counter/complete" ; then
                                        completed_servers=$((completed_servers+1))
                                fi
                        done
                        echo -e "\n\n$(date) $exp"S"$server"T"$th_counter: completed servers: $completed_servers / $num_servers.\n\n"
                        sleep 30
                done
        done
   done <<< $@
}

while read s inc m t e servers; do
	for host in $servers; do
		ssh $host mkdir /home/frojala/EXPERIMENTS
		ssh $host mkdir /home/frojala/EXPERIMENTS/$exp
	done

	run_benchmark 1 $servers

	for server in $(seq $start $increment $max_servers); do
		num_servers=0
		for host in $servers; do
			date
			ssh $host "/home/frojala/YCSB/start-servers.sh $((server - $((increment-1)))) $server" 2>&1 | tee /home/frojala/EXPERIMENTS/$exp/server_start_logs
			date
			echo "Servers $((server - $((increment-1)))) to $server started on $host."
		done
		ssh nc-3 '/home/frojala/apache-geode-src-1.0.0-incubating.M2/geode-assembly/build/install/apache-geode/bin/gfsh -e "connect" -e "rebalance"'
	   date
	   run_benchmark $server $servers
	done
done <<< $@
