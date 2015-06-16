#!/bin/bash

source /uboonenew/setup
setup uboonedaq v6_10_05 -q debug:e7

ctr=0

while IFS='' read -r line
do
    runinfo=$line
    runarray=($runinfo)
    ls ${runarray[0]} > file_list.txt
    outname='noise_check_Run_'${runarray[1]}'_SubRun_'${runarray[2]}
    noise_check file_list.txt $outname
    mv $outname'_detail.txt' $2/.
    mv $outname'_list.txt' $2/.
done < "$1"