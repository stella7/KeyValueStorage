#!/bin/sh

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 configFile port"
    exit -1
fi

JAVA=/usr/lib/jvm/java-1.8.0/bin/java
$JAVA -cp .:gen-java/:"lib/*" StorageNode $1 $2
