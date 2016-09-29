#!/bin/sh

#
# Wojciech Golab, 2016
#

CODEPATH=`pwd`
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 node_num"
    exit -1
fi

LINENUM=`expr $1 + 1`
SHOST=`head -n $LINENUM a1.config | tail -1 | cut -d' ' -f1`
SPORT=`head -n $LINENUM a1.config | tail -1 | cut -d' ' -f2`

echo Attempting to run storage node $1 on $SHOST:$SPORT

# timeout helps ensure that stuck processes are terminated after one hour
ssh $SHOST "cd $CODEPATH; timeout 3600 java -cp .:gen-java:\"lib/*\" StorageNode a1.config $1" &> output-$SHOST:$SPORT.txt &
sleep 1
echo Output redirected to output-$SHOST:$SPORT.txt

