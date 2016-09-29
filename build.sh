#!/bin/sh

#
# Wojciech Golab, 2016
#

JAVA_CC=/usr/lib/jvm/java-1.8.0/bin/javac
THRIFT_CC=/opt/bin/thrift

echo --- Cleaning
rm -f *.class
rm -fr gen-java

echo --- Compiling Thrift IDL
$THRIFT_CC --version
$THRIFT_CC --gen java a1.thrift

echo --- Compiling Java
javac -version
javac gen-java/*.java -cp .:"lib/*"
javac *.java -cp .:gen-java/:"lib/*"

echo --- Done, now check out the run_storage_node.sh script!
