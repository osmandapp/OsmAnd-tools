@echo off
rem Pass full paths to route_tests.xml files. All routing.xml configuration should be configured by OsmAndMapCreator
setlocal

set "DIR=%~dp0"
if "%JAVA_OPTS%"=="" set "JAVA_OPTS=-Xms64M -Xmx512M"

java.exe -Djava.util.logging.config.file="%DIR%logging.properties" %JAVA_OPTS% -cp "%DIR%OsmAndMapCreator.jar;%DIR%lib/*" net.osmand.MainUtilities %*
