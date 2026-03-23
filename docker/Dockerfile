# FROM openjdk:11
ARG MAPCREATOR
FROM ubuntu:22.04
RUN apt update && apt install -y openjdk-17-jdk
WORKDIR /home/work
COPY $MAPCREATOR/ /home/OsmAndMapCreator/
ENTRYPOINT ["/bin/bash","/home/OsmAndMapCreator/utilities.sh"]
