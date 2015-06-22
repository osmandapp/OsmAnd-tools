% Pass full paths to route_tests.xml files. All routing.xml configuration should be configured by OsmAndMapCreator
java.exe -Djava.util.logging.config.file=logging.properties -Xms64M -Xmx512M -cp "./OsmAndMapCreator.jar;lib/OsmAnd-core.jar;./lib/*.jar" net.osmand.MainUtilities %1 %2 %3 %4 %5 %6 %7 %8 %9
