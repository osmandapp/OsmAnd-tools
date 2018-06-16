#!/bin/bash -xe
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../" && pwd )"
LOC=$1
UNZIPPED=$2
array=(en de el es fa fi fr he hi it nl pl pt ro ru sv uk vi zh)
mkdir -p langlinks
for lang in ${array[*]}; do
	echo "Start download $lang";
    if [ ! -f  "$LOC/$lang"wiki-latest-pages-articles.xml.bz2 ]; then
      wget --quiet -P "$LOC" -N http://dumps.wikimedia.org/"$lang"wikivoyage/latest/"$lang"wikivoyage-latest-pages-articles.xml.bz2
    fi
    if [ ! -f  "$LOC/langlinks/${lang}wikivoyage-latest-langlinks.sql.gz" ]; then
      wget --quiet  -P "$LOC/langlinks" -N "http://dumps.wikimedia.org/"$lang"wikivoyage/latest/"$lang"wikivoyage-latest-langlinks.sql.gz" 
    fi
    # net.osmand.wiki.creator.WikivoyagePreparation
    OsmAndMapCreator/utilities.sh generate-wikivoyage-raw $lang "$LOC" $UNZIPPED
done


