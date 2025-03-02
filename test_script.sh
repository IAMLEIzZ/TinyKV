#!/bin/bash
for ((i=1;i<=1;i++));
do
    echo "ROUND $i";
    LOG_LEVEL=error make project2c > ./out/out-$i.log;
done