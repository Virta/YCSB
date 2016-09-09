#!/bin/bash

start=$1
increment=$2
max_serv=$3
threads=$4

for i in $(seq $start $increment $max_serv); do
	/home/frojala/YCSB/benchmark_geode.sh /home/frojala/EXPERIMENTS/ E1S $threads $i $increment;
done

