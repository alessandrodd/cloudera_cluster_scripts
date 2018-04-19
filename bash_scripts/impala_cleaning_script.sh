#!/bin/bash

####################################################
# Warning: this script is meant to be used in  
# CDH < 5.9.2 or Impala < 2.8.0 .
# In CDH 5.9.2 and above (or Impala 2.8.0 and above)
# IMPALA-3983 and IMPALA-3983 where solved 
####################################################

JAVA_IO_TMPDIR_DEFAULT=/tmp
TEMPFILE=/tmp/$$.tmp

log() {
    printf "`date`: $1\n"
}

show_help(){
    echo  "Impala Catalog Server UDF jars Cleaner Utility"
    echo  " *****************************************************************************"
    echo  " *****************************************************************************"
    echo  " ** "  
    echo  " ** Warning: this script is meant to be used in CDH < 5.9.2 or Impala < 2.8.0"
    echo  " ** In CDH 5.9.2 and above (or Impala 2.8.0 and above) "
    echo  " ** this should not be necessary (see IMPALA-3983 and IMPALA-3983)."
    echo  " ** "  
    echo  " *****************************************************************************"
    echo  " *****************************************************************************"
    echo  "Usage: $0 [minutes]" 
    echo  "          Search and deletes all UDF jars older than [minutes] minutes."
    echo  "          If a Catalog Server is using it, than it could fail or crash. However,"
    echo  "          these jar files should be used by the process only for a few ."  
    echo  "          instruction, so for example a day-old file should be unnecessary."  
    echo  "                                                                           "  
    echo  "          Temp directory is defined by _JAVA_OPTIONS environment variable,"  
    echo  "          e.g. export _JAVA_OPTIONS=\"\$_JAVA_OPTIONS -Djava.io.tmpdir=/tmp\""
    echo  "          (default /tmp)                                                     "  

}


if [ ! "$1" ]
then
    show_help
    exit 1
fi
now=$(date +%s)
echo 0 > $TEMPFILE
# check if we are using a global custom java.io.tmpdir dir
JAVA_IO_TMPDIR="$(echo $_JAVA_OPTIONS | grep -Po 'java.io.tmpdir=\K[^ ]+' | tail -1)"
if [[ -z "${JAVA_IO_TMPDIR// }" ]]; then
    JAVA_IO_TMPDIR=$JAVA_IO_TMPDIR_DEFAULT
fi
if cd ${JAVA_IO_TMPDIR}; then
    for file in $(find . -regextype posix-extended -regex './(\.){0,1}[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.jar(\.crc){0,1}' -user impala -mmin +$1); do 
        echo $file
        rm $file
        COUNTER=$(($(cat $TEMPFILE) + 1))
        echo $COUNTER > $TEMPFILE
    done
fi
log "Deleted $(cat $TEMPFILE) files."
unlink $TEMPFILE
