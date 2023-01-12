#!/bin/bash
cpuname=$(cat /proc/cpuinfo | sed "s/model name\t: //p" -n | head -n1)
corecnt=$(cat /proc/cpuinfo | sed "s/processor\t: //p" -n | wc -l)
memttl=$(cat /proc/meminfo | sed -E 's/MemTotal:\W*([0123456789]*).*/\1/p' -n)
memfree=$(cat /proc/meminfo | sed -E 's/MemFree:\W*([0123456789]*).*/\1/p' -n)
file=$1deviceinfo.inf
echo $cpuname > $file
echo $corecnt >> $file
echo $memttl >> $file
echo $memfree >> $file

