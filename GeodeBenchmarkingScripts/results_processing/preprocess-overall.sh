#!/bin/bash

for f in $(ls); do
	cd $f

	for sd in $(ls -d */);do
		cd $sd
		~/aggregate_overall.sh
		cd ..
	done

	for sd in $(ls); do
		mv $sd/$sd"_OVERALL.csv" $sd"_OVERALL_${f: -3}.csv"
	done
	cd ..
done

	mkdir aggregate
	mkdir aggregate/separate
	cp */*csv aggregate
	cd aggregate
