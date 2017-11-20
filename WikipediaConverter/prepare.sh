#!/bin/bash
BUILD_PATH=$1
function download {
      echo "Start download $1";
      if [ ! -f  "$1"wiki-latest-pages-articles.xml.bz2 ]; then
      	wget --quiet -N http://dumps.wikimedia.org/"$1"wiki/latest/"$1"wiki-latest-pages-articles.xml.bz2
      fi
      if [ ! -f  "$1"wiki-latest-langlinks.sql.gz ]; then
      	wget --quiet -N http://dumps.wikimedia.org/"$1"wiki/latest/"$1"wiki-latest-langlinks.sql.gz      
      fi
      if [ ! -f  "$1"wiki-latest-externallinks.sql.gz ]; then
            wget --quiet -N http://dumps.wikimedia.org/"$1"wiki/latest/"$1"wiki-latest-externallinks.sql.gz      
      fi
      if [ ! -f  "$1"wiki.sqlite ]; then
      	java -Xms256M -Xmx3200M -cp "$BUILD_PATH/WikiConverter.jar:$BUILD_PATH/build/lib/*.jar" net.osmand.osm.util.WikiDatabasePreparation $1
      fi
}



download en English;
download de German;
download nl Dutch;
download fr French;
download ru Russian;
download es Spanish;
download pl Polish;
download it Italian;
download ca Catalonian;
download pt Portuguese;
download uk Ukranian;
download ja Japanese;
download vo Volapuk;
download vi Vietnamese;
download eu Basque;
download no Norwegian;
download da Danish;
download sv Swedish;
download sr Serbian;
download eo Esperanto;
download ro Romanian;
download lt Lithuanian;
download fa Farsi;
download cs Czech;
download ms Malay;
download zh Chinese;
download id Indonesian;
download fi Finnish;
download bg Bulgarian;
download et Estonian;
download hr Croatian;
download nn NorwegianNynorsk;
download ko Korean;
download sl Slovene;
download el Greek;
download he Hebrew;
download ar Arabic ;
download tr Turkish;
download th Thai;
download be Belarusian;
download ka Georgian;
download mk Macedonian;
download lv Latvian;
download lb Luxembourgish;
download os Ossetian;
download gl Galician ;

download fy Frysk;
download af Africaans;
download hy Armenian;
download ml Malayalam;
download als Alsatian;
download sw Swahili;
download ta Tamil;
download nds LowSaxon;
download ku Kurdish;
download la Latin;
download ga Irish;
download nv Navajo;
download hi Hindi;
download hu Hungarian;
download te Telugu;
download ht Haitian;
download sc Sardinian;
download new Newar;
download ceb Cebuano;
download bs Bosnian;
download bpy Bishnupriya;
download is Icelandic;
download sq Albanian;
download br Breton;
download mr Marathi;
download az Azeri;
download sh Serbo-Croatian;
download tl Filipino;
download cy Welsh;
download bn Bengali;
download pms Piedmontese;
download sk Slovak;

download war Waray;
download min Minangkabau;
download kk Kazakh;
download uz Uzbek;
download ce Chechen;
download ur Urdu;
download oc Occitan;
download zhminnan SouthernMin;
download mg Malagasy;
download tt Tatar;
download jv Javanese;
download ky Kyrgyz;
download zhyue Cantonese;
download ast AsturLeonese;
download tg Tajik;
download ba Bashkir;
download sco Scots;
download pnb Punjabi;
download cv Chuvash;
download lmo Lombard;
download my Burmese;
download yo Yoruba;
download an Aragonese;
download ne Nepali;
download gu Gujarati;
download scn Sicilian;
download bar Bavarian;
download mn Mongolian;
download nap Neapolitan;
download hsb UpperSorbian;




