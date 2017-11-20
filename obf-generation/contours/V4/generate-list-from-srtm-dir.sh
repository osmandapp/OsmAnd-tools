#!/bin/bash
while read p; do
  CNT=`du -csh $p* | wc -l`
  SIZE=`du -csh $p* | tail -1`
  echo "$p $CNT $SIZE"
done <list_files.txt
