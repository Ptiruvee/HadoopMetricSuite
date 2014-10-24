#!/bin/sh

while true ; do

CPU=$(grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage}')

TIME=$(date +%s)

echo "$TIME $CPU" >> hms_cpu.txt

sleep 1

done