#!/bin/sh

while true ; do

CPU=$(grep 'cpu ' /proc/stat | awk '{usage=100 * ($5-$4)/$5} END {print usage}')

DISK=$(grep 'xvda ' /proc/diskstats | awk '{{read=$4} {write=$8} {readtime=$7} {writetime=$11} {iotime=$13}} END {print read, write, readtime, writetime, iotime}')

MEM=$(cat /proc/meminfo | head -2 | awk 'NR == 1 { total = $2 } NR == 2 { free = $2 } END { print 1.0 - (free/total) }')

NET=$(grep 'eth0' /proc/net/dev | sed -e 's/eth0://g' | awk '{print $2, $10}')

TIME=$(date +%s)

echo "$TIME $CPU" >> hms_cpu.txt

echo "$TIME $DISK" >> hms_disk.txt

echo "$TIME $MEM" >> hms_memory.txt

echo "$TIME $NET" >> hms_network.txt

sleep $1

done