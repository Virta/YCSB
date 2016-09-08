#!/bin/bash

start=$1
increment=$2
max_servers=$3
threads=$4
host=$5

ssh $host '
for i in $(seq $start $increment $max_servers);
	do /home/frojala/YCSB/benchmark_geode.sh /home/frojala/EXPERIMENTS/ E1S $threads $i $increment;
done
' &
