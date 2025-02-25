#!/bin/bash
for ((i=1;i<=10;i++));
do
    echo "ROUND $i";
    LOG_LEVEL=error make project2c > ./out/out-$i.txt;
done