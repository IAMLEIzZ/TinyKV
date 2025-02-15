#!/bin/bash
for ((i=1;i<=100;i++));
do
    echo "ROUND $i";
    make project2a > ./out/out-$i.txt;
done