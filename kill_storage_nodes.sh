#!/bin/sh

#
# Wojciech Golab, 2016
#

NUM=`cat a1.config | wc -l`

for i in `seq 1 ${NUM}`; do
    LINENUM=$i
    SHOST=`head -n $LINENUM a1.config | tail -1 | cut -d' ' -f1`
    SPORT=`head -n $LINENUM a1.config | tail -1 | cut -d' ' -f2`
    echo Killing all java processes belonging to $USER on $SHOST
    ssh $SHOST "killall -u $USER java" &> /dev/null
done
