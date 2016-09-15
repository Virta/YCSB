#!/bin/bash

start=$1
max_servers=$2
host=$(hostname | tr -d '-')

for to_start in $(seq $start $max_servers); do
        if [[ $to_start -eq 0 ]]; then continue; fi
        server_port=$((40403+to_start))
        /home/frojala/apache-geode-src-1.0.0-incubating.M2/geode-assembly/build/install/apache-geode/bin/gfsh\
                -e "connect" -e "start server \
                --name=serv-$host-$to_start \
                --server-port=$server_port \
                --classpath=/home/frojala/YCSB/core/target/archive-tmp/core-0.11.0-SNAPSHOT.jar"
done

/home/frojala/apache-geode-src-1.0.0-incubating.M2/geode-assembly/build/install/apache-geode/bin/gfsh\
                -e "connect" -e "rebalance"
