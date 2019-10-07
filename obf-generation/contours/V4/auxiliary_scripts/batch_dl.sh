#/bin/bash
wget -r -l1 -np -nc --timeout=10 "ftp://ftp.eorc.jaxa.jp/pub/ALOS/ext1/AW3D30/release_v1903/" -P data -A "S*.tar.gz"
