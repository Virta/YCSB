#!/bin/bash

start=$1
increment=$2
max_servers=$3
threads=$4
exp=$5
server_list=

function run_benchmark {
   while read server servers; do
	for th_counter in $(seq 1 $increment $threads); do
		num_servers=0
                for host in $servers; do
                        ssh $host "/home/frojala/YCSB/benchmark_geode.sh /home/frojala/EXPERIMENTS/ $exp $th_counter $server" &
                        num_servers=$((num_servers+1))
                done
                completed_servers=0
                while [[ ! $completed_servers -eq $num_servers ]]; do
			sleep 30
                        completed_servers=0
                        for host in $servers; do
                                if ssh $host test -e "/home/frojala/EXPERIMENTS/$exp/$exp"S"$server/$exp"S"$server"_T"$th_counter/complete" ; then
                                        completed_servers=$((completed_servers+1))
                                fi
                        done
                        echo -e "\n\n$(date) $exp"S"$server"T"$th_counter: completed servers: $completed_servers / $num_servers.\n\n"
                done
		ssh frojala@cs@shell.cs.helsinki.fi echo "Done: $exp"S"$server"_T"$th_counter" >> ~/public_html/experiments
        done
   done <<< $@
}

while read s inc m t e servers; do
	server_list=$servers
	for host in $server_list; do
		ssh $host mkdir /home/frojala/EXPERIMENTS
		ssh $host mkdir /home/frojala/EXPERIMENTS/$exp
	done

	run_benchmark 1 $server_list

	for server in $(seq $start $increment $max_servers); do
		num_servers=0
		for host in $server_list; do
			date
			ssh $host "/home/frojala/YCSB/start-servers.sh $((server - $((increment-1)))) $server" 2>&1 | tee /home/frojala/EXPERIMENTS/$exp/server_start_logs
			date
			echo -e "\n\nServers $((server - $((increment-1)))) to $server started on $host.\n\n"
		done
		ssh nc-3 '/home/frojala/apache-geode-src-1.0.0-incubating.M2/geode-assembly/build/install/apache-geode/bin/gfsh -e "connect" -e "rebalance"'
		date
		run_benchmark $server $server_list
	done
done <<< $@
