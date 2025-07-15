#!/bin/bash

INPUT_DIR=$WEB_PATH/docs/web/main/docs/user
OUTPUT_DIR=$WEB_PATH/rag/documents/docs
VECTOR_DB_DIR=$WEB_PATH/rag/vector-db

# Define the base directory
BASE_DIR=$SCRIPT_DIR/python/support

# Define the Java source file
JAVA_SOURCE_FILE=$BASE_DIR/net/osmand/support/MarkdownSlicer.java

# Define the output directory for compiled classes
CLASS_DIR=$BASE_DIR/bin

# Create the output directory if it doesn't exist
mkdir -p $CLASS_DIR

# Compile the Java source file
javac -d $CLASS_DIR $JAVA_SOURCE_FILE

# Check if compilation was successful
if [ $? -eq 0 ]; then
    echo "Compilation successful!"
    if [ -d $OUTPUT_DIR ]; then
      rm -r $OUTPUT_DIR
    fi
    # Run Java class
    java -cp $CLASS_DIR net.osmand.support.MarkdownSlicer $INPUT_DIR $OUTPUT_DIR
    rm -r $CLASS_DIR
    if [[ "$DELETE_VECTOR_DB" == "true" ]]; then
      if [ -d "$VECTOR_DB_DIR" ]; then
        rm -r "$VECTOR_DB_DIR"
      fi
      echo "Vector DB is deleted!"
    fi
else
    echo "Compilation failed!"
fi
