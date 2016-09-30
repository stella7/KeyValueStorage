#!/bin/sh

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 configFile"
    exit -1
fi

JAVA=/usr/lib/jvm/java-1.8.0/bin/java
$JAVA -cp .:gen-java/:"lib/*" Client $1
