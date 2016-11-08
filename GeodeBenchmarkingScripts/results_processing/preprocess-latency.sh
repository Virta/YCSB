#!/bin/bash

for f in $(ls); do
	cd $f

	for sd in $(ls -d */);do
		cd $sd
		~/aggregate_latency.sh
		cd ..
	done

	for sd in $(ls); do
		mv $sd/$sd"_LATENCY.csv" $sd"_LATENCY_${f: -3}.csv"
	done
	cd ..
done

	mkdir aggregate-latency
	mkdir aggregate-latency/separate
	cp */*LATENCY*csv aggregate-latency
	cd aggregate-latency
