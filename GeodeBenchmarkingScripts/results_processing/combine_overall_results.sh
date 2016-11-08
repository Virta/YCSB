#!/bin/bash

exp=$1
servers=$1

function first_round {
        fileA=$1
        fileB=$2

        while IFS="," read Athread Aign1 Aign2 Aign3 Aign4 Aign5 Atps; do
          echo "$Athread, $Atps"
        done <$fileA
}

function more_rounds {
	fileA=$1
        fileB=$2

        while IFS="," read line && IFS="," read Bthread Bign1 Bign2 Bign3 Bign4 Bign5 Btps <&3; do
          echo "$line, $Btps"
        done <$fileA 3<$fileB
}

while read -r e server_list; do
	for i in $(seq 1 1 20); do
	   for s in $server_list; do
		base=E"$1"S"$i"_OVERALL_
#	echo $base$s
		if [[ -e $base$s".csv" && -e E"$1"S"$i".csv ]]; then
			more_rounds E"$1"S"$i".csv $base$s".csv" > E"$1"S"$i".tmp
			mv E"$1"S"$i".tmp E"$1"S"$i".csv
		elif [[ -e $base$s".csv" && ! -e E"$1"S"$i".csv ]];then
			first_round $base$s".csv" > E"$1"S"$i".csv
		fi
	   done
	done
done <<< $@

for i in $(seq 1 1 20); do
	first=1
   if [[ -e E"$1"S"$i".csv ]]; then
	while IFS="," read thread s1 s2 s3 s4 s5 s6 s7; do
		s1=${s1:=0}
		s2=${s2:=0}
		s3=${s3:=0}
		s4=${s4:=0}
		s5=${s5:=0}
		s6=${s6:=0}
		s7=${s7:=0}
		if [[ $first -eq 1 ]]; then
			first=0
			echo "${thread/'T'/}, $s1, $s2, $s3, $s4, $s5, $s6, $s7, $s1" >> E"$1"S"$i".tmp
			continue
		fi
		echo "${thread/'T'/}, $s1, $s2, $s3, $s4, $s5, $s6, $s7, $(echo "$s1 + $s2 + $s3 + $s4 + $s5 + $s6 + $s7" | bc)" >> E"$1"S"$i".tmp
	done < E"$1"S"$i".csv
	mv E"$1"S"$i".tmp E"$1"S"$i".csv
   fi
done

