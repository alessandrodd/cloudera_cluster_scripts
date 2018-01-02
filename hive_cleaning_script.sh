#!/bin/bash

####################################################
# Warning: this script is meant to be used in  
# CDH < 5.8.4 or Hive < 1.3.0 .
# For CDH 5.8.4 and above (or Hive 1.3.0 and above)
# the cleardanglingscratchdir service should be
# used (see HIVE-15068).
####################################################

TEMPFILE=/tmp/$$.tmp

log() {
    printf "`date`: $1\n"
}

show_help(){
    echo  "Hive Scratch Directories Cleaner Utility"
    echo  " *****************************************************************************"
    echo  " *****************************************************************************"
    echo  " ** "  
    echo  " ** Warning: this script is meant to be used in CDH < 5.8.4 or Hive < 1.3.0 ."
    echo  " ** For CDH 5.8.4 and above (or Hive 1.3.0 and above) the "
    echo  " ** cleardanglingscratchdir service should be used (see HIVE-15068)."
    echo  " ** "  
    echo  " *****************************************************************************"
    echo  " *****************************************************************************"
    echo  "Usage: $0 [days]" 
    echo  "          Search and deletes all hive temp files older than [days] days."
    echo  "          If there is a running Hive job that is older than [days], than"
    echo  "          that job will fail."  

}


if [ ! "$1" ]
then
    show_help
    exit 1
fi
now=$(date +%s)
echo 0 > $TEMPFILE
HADOOP_USER_NAME=hdfs hdfs dfs -ls -R /tmp/hive/ | grep "^-" | while read f; do
    dir_date=`echo $f | awk '{print $6}'`
    difference=$(( ( $now - $(date -d "$dir_date" +%s) ) / (24 * 60 * 60 ) ))
    if [ $difference -gt $1 ]; then
        HADOOP_USER_NAME=hdfs hdfs dfs -rm -skipTrash `echo $f | awk '{ print $8 }'`;
        COUNTER=$(($(cat $TEMPFILE) + 1))
        echo $COUNTER > $TEMPFILE
    fi
done
log "Deleted $(cat $TEMPFILE) files."
unlink $TEMPFILE
