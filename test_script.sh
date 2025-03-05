#!/bin/bash
for ((i=1;i<=10;i++));
do
    echo "ROUND $i";
    LOG_LEVEL=error make project3b > ./out/test3/out3b-$i.log;
done