REM Use the JVM with standard settings.
start javaw.exe -Djava.util.logging.config.file=logging.properties -jar OsmAndMapCreator.jar

REM Use the JVM with the below settings to increase the heap size (Available memory for the application)
REM start javaw.exe -Xmx4096m -Djava.util.logging.config.file=logging.properties -jar OsmAndMapCreator.jar
