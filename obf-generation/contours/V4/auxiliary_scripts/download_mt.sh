#!/bin/bash
#Parallel download script
#list.txt should contain list of direct web links
user="user"
password="password"
threads=10
cat list.txt | parallel --bar --no-notice -j $threads wget -nc -c --user=$user --password=$password {}