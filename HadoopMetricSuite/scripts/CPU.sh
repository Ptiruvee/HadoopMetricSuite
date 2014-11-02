#!/bin/sh

while true ; do

CPU=$(grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage}')

DISK=$(grep 'sda ' /proc/diskstats | awk '{usage=($4)/($8)} END {print usage}')

MEM=$(cat /proc/meminfo | head -2 | awk 'NR == 1 { total = $2 } NR == 2 { free = $2 } END { print 1.0 - (free/total) }')

NET=$(grep 'em1' /proc/net/dev | sed -e 's/em1://g' | awk '{print $1/$9}')

TIME=$(date +%s)

echo "$TIME $CPU" >> hms_cpu.txt

echo "$TIME $DISK" >> hms_disk.txt

echo "$TIME $MEM" >> hms_memory.txt

echo "$TIME $NET" >> hms_network.txt

sleep $1

done