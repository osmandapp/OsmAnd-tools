#!/bin/bash
BUILD_PATH=$1
PARAMETER=$2

function download {
      echo "Start download $1";
      if [ ! -f  "$1"wiki-latest-pages-articles.xml.bz2 ]; then
      	wget --quiet -N http://dumps.wikimedia.org/"$1"wikivoyage/latest/"$1"wikivoyage-latest-pages-articles.xml.bz2
      fi
      #if [ ! -f  "$1"wiki-latest-langlinks.sql.gz ]; then
      	#wget --quiet -N http://dumps.wikimedia.org/"$1"wikivoyage/latest/"$1"wikivoyage-latest-langlinks.sql.gz      
      #fi
      #if [ ! -f  "$1"wiki-latest-externallinks.sql.gz ]; then
      #      wget --quiet -N http://dumps.wikimedia.org/"$1"wikivoyage/latest/"$1"wikivoyage-latest-externallinks.sql.gz      
      #fi
      if [ ! -f  "$1"wiki.sqlite ]; then
      	java -Xms256M -Xmx3200M -cp "$BUILD_PATH/WikiConverter.jar:$BUILD_PATH/build/lib/*.jar" net.osmand.osm.util.WikiVoyagePreparation $1 ./ $PARAMETER
      fi
}



download en English;




