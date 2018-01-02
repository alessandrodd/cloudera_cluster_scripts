#!/bin/bash

# Initialize all the option variables.
# This ensures we are not contaminated by variables from the environment.
user=""
verbose=0
should_clean_hdfs=false
should_clean_hive=false

log() {
    printf "`date`: $1\n"
}

error() {
    log "[ERROR] $1" 1>&2
}

check_if_command_exists() {
    if hash "$1" 2>/dev/null; then
        echo true
    else
        echo false
    fi
}

run_cleardanglingscratchdir(){
    log "cleaning Hive dangling scratch directory for user $1"
    HADOOP_USER_NAME=$1 hive --service cleardanglingscratchdir
    if [ "$verbose" -gt "0" ]; then
        log "Return Code: " "$?"
    fi
}

clean_hive(){
    if "$( check_if_command_exists hive )"
    then
        if [ -z "$user" ]; then
            # we cannot use the following simpler version because the -C argument was added in CDH 5.8 (see HADOOP-10971)
            # for filename in `hdfs dfs -ls -C /user | awk '{print $NF}' | tr '\n' ' '`
            for filename in `hdfs dfs -ls /user | sed 1d | perl -wlne'print +(split " ",$_,8)[7]'| awk '{print $NF}' | tr '\n' ' '`
            do
                username=$(basename $filename)
                run_cleardanglingscratchdir $username
            done
        else
            for username in $(echo $user | sed "s/,/ /g")
            do
                run_cleardanglingscratchdir $username
            done
        fi  
    else
            error "hive command not found. Skipping..."
    fi
    log "Hive cleaning done"
}

run_expunge(){
    log "expunging trash for user $1"
    HADOOP_HOME_WARN_SUPPRESS=1 HADOOP_ROOT_LOGGER="ERROR" HADOOP_USER_NAME=$1 hdfs dfs -expunge
    if [ "$verbose" -gt "0" ]; then
        log "Return Code: " "$?"
    fi
}

clean_hdfs(){
    log "Emptying HDFS trash..."
    if "$( check_if_command_exists hdfs )"
    then
        if [ -z "$user" ]; then
            # we cannot use the following simpler version because the -C argument was added in CDH 5.8 (see HADOOP-10971)
            # for filename in `hdfs dfs -ls -C /user | awk '{print $NF}' | tr '\n' ' '`
            for filename in `hdfs dfs -ls /user | sed 1d | perl -wlne'print +(split " ",$_,8)[7]'| awk '{print $NF}' | tr '\n' ' '`
            do
                username=$(basename $filename)
                run_expunge $username
            done
        else
            for username in $(echo $user | sed "s/,/ /g")
            do
                run_expunge $username
            done
        fi           
    else
            error "hdfs command not found. Skipping trash expunge..." 
    fi
    log "HDFS cleaning done"
}

die() {
    printf '%s\n' "$1" >&2
    exit 1
}

# Usage info
show_help() {
    cat << EOF
    Cloudera cluster cleaning utility
    Usage: ${0##*/} [--hive] [--hdfs] [-hv] [--user=USER1,USER2,...]

        --hdfs      cleans the HDFS trash, removing all checkpoints older than
                    fs.trash.interval parameter
        --hive      clean Hive scratch directory; removes dangling temp files
                    warning: hive.scratchdir.lock should be set to true to 
                    avoid corrupting running jobs
        -h          display this help and exit
        -v          verbose mode; can be used multiple times for increased
                    verbosity
        --user      execute the operation for the specified user[s]. If not
                    specified, then it will be executed for all users
    EOF
}


if [ $# -eq 0 ]
  then
    show_help # Show help if no arguments passed
fi

while :; do
    case $1 in
        --hdfs)
            should_clean_hdfs=true  # Do all hdfs cleaning operations
            ;;
        --hive)
            should_clean_hive=true  # Do all hive cleaning operations
            ;;
        -h|-\?|--help)
            show_help    # Display a usage synopsis.
            exit
            ;;
        -v|--verbose)
            verbose=$((verbose + 1))  # Each -v adds 1 to verbosity.
            ;;
        --user=?*)
            user=${1#*=} # Delete everything up to "=" and assign the remainder.
            ;;
        --user=)         # Handle the case of an empty --user=
            die 'ERROR: "--user" requires a non-empty option argument.'
            ;;
        --)              # End of all options.
            shift
            break
            ;;
        -?*)
            printf 'WARN: Unknown option (ignored): %s\n' "$1" >&2
            ;;
        *)               # Default case: No more options, so break out of the loop.
            break
    esac

    shift
done
if $should_clean_hdfs; then
    clean_hdfs
fi
if $should_clean_hive; then
    clean_hive
fi