% Pass full paths to route_tests.xml files. All routing.xml configuration should be configured by OsmAndMapCreator
java.exe -Djava.util.logging.config.file=logging.properties -Xms64M -Xmx512M -cp "OsmAndMapCreator.jar;lib/*.jar" net.osmand.MainUtilities  %*
