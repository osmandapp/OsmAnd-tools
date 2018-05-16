#!/bin/bash
BUILD_PATH=$1
PARAMETER=$2
INITIAL_PATH=$PWD

function download {
      cd $INITIAL_PATH
      echo "Start download $1";
      if [ ! -f  "$1"wiki-latest-pages-articles.xml.bz2 ]; then
      	wget --quiet -N http://dumps.wikimedia.org/"$1"wikivoyage/latest/"$1"wikivoyage-latest-pages-articles.xml.bz2
      fi
      #if [ ! -f  "$1"wiki-latest-externallinks.sql.gz ]; then
      #      wget --quiet -N http://dumps.wikimedia.org/"$1"wikivoyage/latest/"$1"wikivoyage-latest-externallinks.sql.gz      
      #fi
      java -Xms256M -Xmx3200M -cp "$BUILD_PATH/WikiConverter.jar:$BUILD_PATH/build/lib/*.jar" net.osmand.osm.util.WikiVoyagePreparation $1 ./ $PARAMETER
}

function downloadLangLinks {
      if [ ! -d langlinks ]; then
           mkdir -p langlinks
      fi
      
      cd langlinks
      array=( de el en es fa fi fr he hi it nl pl pt ro ru sv uk vi zh)
      echo "Start downloading langlinks";
      for item in ${array[*]}
      do
            if [ ! -f  "$item"wiki-latest-langlinks.sql.gz ]; then
                  wget --quiet -N http://dumps.wikimedia.org/"$item"wikivoyage/latest/"$item"wikivoyage-latest-langlinks.sql.gz      
            fi
      done
}

downloadLangLinks;
#download en English;
#download de German;
#download nl Dutch;
#download fr French;
download ru Russian;
#download es Spanish;
#download pl Polish;
#download it Italian;
#download el Greek;
#download fa Farsi;
#download fi Finnish;
#download he Hebrew;
#download pt Portuguese;
#download uk Ukranian;
#download vi Vietnamese;
#download ro Romanian;
#download zh Chinese;
#download hi Hindi;
#download sv Swedish;
