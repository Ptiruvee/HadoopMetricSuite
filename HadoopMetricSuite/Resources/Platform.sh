#!/bin/sh

echo $(date +%s) > hms_cpu.txt

nohup vmstat $1 -n >> hms_cpu.txt &

while true ; do

DISK=$(grep 'xvda ' /proc/diskstats | awk '{{read=$4} {write=$8} {readtime=$7} {writetime=$11} {iotime=$13}} END {print read, write, readtime, writetime, iotime}')

MEM=$(cat /proc/meminfo | head -2 | awk 'NR == 1 { total = $2 } NR == 2 { free = $2 } END { print 1.0 - (free/total) }')

NET=$(grep 'eth0' /proc/net/dev | sed -e 's/eth0://g' | awk '{print $2, $10}')

TIME=$(date +%s)

echo "$TIME $DISK" >> hms_disk.txt

echo "$TIME $MEM" >> hms_memory.txt

echo "$TIME $NET" >> hms_network.txt

sleep $1

done