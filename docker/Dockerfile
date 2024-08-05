# FROM openjdk:11
FROM ubuntu:22.04
RUN apt update && apt install -y openjdk-17-jdk
WORKDIR /home/work
COPY OsmAndMapCreator/ /home/OsmAndMapCreator/
ENTRYPOINT ["/bin/bash","/home/OsmAndMapCreator/utilities.sh"]
