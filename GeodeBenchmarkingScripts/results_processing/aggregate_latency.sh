#!/bin/bash

echo "threads, S1_RELEASE, HANDOVER, ATTACH, CELL_RESELECT, SERVICE_REQUEST, TAU, SESSION_MANAGEMENT, DETACH" > $(basename "$PWD")_LATENCY.csv

for dir in $(ls); do
   if [[ -d $dir ]]; then
	res=$(cat $dir/"$dir".log | grep -e "AverageLatency" | grep -v -e "CLEANUP" | while read -r line; do while read -r type pre value; do echo " $value"; done <<<$line; done | tr '\n' ', ')
	echo "${dir: -2},$res" >> $(basename "$PWD")_LATENCY.csv
   fi
done

	first=1
	while IFS=", " read -r threads S1_RELEASE HANDOVER ATTACH CELL_RESELECT SERVICE_REQUEST TAU SESSION_MANAGEMENT DETACH; do
	   if [[ $first -eq 1 ]]; then
		avg="AVERAGE"
		first=0
	   else
		sum=$(echo "$S1_RELEASE + $HANDOVER + $ATTACH + $CELL_RESELECT + $SERVICE_REQUEST + $TAU + $SESSION_MANAGEMENT + $DETACH" | bc)
		avg=$(echo "scale=20; $sum/8" | bc)
	   fi
	   echo "$threads, $S1_RELEASE, $HANDOVER, $ATTACH, $CELL_RESELECT, $SERVICE_REQUEST, $TAU, $SESSION_MANAGEMENT, $DETACH, $avg" >> $(basename "$PWD")_LATENCY.tmp
	done < $(basename "$PWD")_LATENCY.csv

mv $(basename "$PWD")_LATENCY.tmp $(basename "$PWD")_LATENCY.csv
