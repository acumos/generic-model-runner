#! /bin/bash
curl --silent https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/maven-metadata.xml | grep -Po '(?<=<version>)([0-9\\.]+(-SNAPSHOT)?)' | sort --version-sort -r | head -n 1
