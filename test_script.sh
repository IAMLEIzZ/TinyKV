#!/bin/bash
for ((i=1;i<=10;i++));
do
    echo "ROUND $i";
    LOG_LEVEL=error make project3c > ./out/test3/out3C-$i.log;
done