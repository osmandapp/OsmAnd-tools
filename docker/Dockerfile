FROM openjdk:11
WORKDIR /home/work
RUN apt-get update && apt-get install libstdc++6
COPY OsmAndMapCreator/ /home/OsmAndMapCreator/
ENTRYPOINT ["/bin/bash","/home/OsmAndMapCreator/utilities.sh"]
