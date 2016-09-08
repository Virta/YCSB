#!/bin/bash

path=$1
preamble=$2
threads=$3
server=$4
increment=$5

if [[ ! -e /home/frojala/YCSB/bin/ycsb ]]; then
	echo "YCSB not found in: /home/frojala/YCSB/bin/ycsb"
	exit 1
fi

for to_start in $(seq $((server-$((increment-1)))) 1 $server); do
	if [[ $to_start -eq 0 ]]; then continue; fi
	server_port=$((40403+to_start))
echo "	/home/frojala/apache-geode-src-1.0.0-incubating.M2/geode-assembly/build/install/apache-geode/bin/gfsh\
		-e "connect" -e "start server \
		--name=serv-nc3-$to_start \
		--server-port=$server_port \
		--classpath=/home/frojala/YCSB/core/target/archive-tmp/core-0.11.0-SNAPSHOT.jar"
"

done

# /home/frojala/apache-geode-src-1.0.0-incubating.M2/geode-assembly/build/install/apache-geode/bin/gfsh -e "connect" -e "rebalance"

d_path="$path/$preamble$server"

if [[ ! -e $d_path ]]; then
       mkdir $d_path
fi

for th_counter in $(seq 1 $increment $threads);do
	if [[ $th_counter -lt 10 ]]; then
		f_preamble=$preamble$server"_T0$th_counter"
	else
		f_preamble=$preamble$server"_T$th_counter"
	fi

	if [[ ! -e "$d_path/$f_preamble" ]]; then
		mkdir "$d_path/$f_preamble"
	fi
echo "

	/home/frojala/YCSB/bin/ycsb run basic -P /home/frojala/YCSB/workloads/LTEworkload -s -t \
		-p hdrhistogram.output.path=$d_path/$f_preamble/hdr_histo_ \
		-threads $th_counter > $d_path/$f_preamble/"$f_preamble".log
"
	echo "Done with $f_preamble"

done
echo "Completed benchmark of $preamble$server"
