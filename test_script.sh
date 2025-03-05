#!/bin/bash
for ((i=1;i<=1;i++));
do
    echo "ROUND $i";
    LOG_LEVEL=error make project3b > ./out/test3/out3B-$i.log;
done