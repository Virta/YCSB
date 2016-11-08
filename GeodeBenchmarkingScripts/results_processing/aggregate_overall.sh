#!/bin/bash
echo "threads, null, null, Run time, null, null, ${PWD##\/*S} servers" > $(basename "$PWD")_OVERALL.csv

for dir in $(ls); do
   if [[ -d $dir ]]; then
	res=$(cat $dir/"$dir".log | grep OVERALL | tr '\n' ', ')
	echo "${dir: -2}, $res" >> $(basename "$PWD")_OVERALL.csv
   fi
done
