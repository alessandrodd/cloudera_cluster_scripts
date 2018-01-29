# cloudera_cluster_scripts
Various scripts that can be useful in a Cloudera cluster environment

## cloudera_cleaner_script.sh

Cloudera cluster cleaning utility

### Usage:  
```
cloudera_cleaner_script.sh [--hive] [--hdfs] [--sqoop] [-h] [-v] [--user=USER1,USER2,...]
```

### Parameters:

    --hdfs
    
Cleans the HDFS trash by executing an _hdfs expunge_ operation that removes all checkpoints older than fs.trash.interval parameter.

    --hive

Cleans the Hive scratch directory by calling the cleardanglingscratchdir service that removes dangling temp files (files that are usually under /tmp/hive/). 
WARNING: Note that hive.scratchdir.lock should be set to true in order to avoid corrupting running jobs. In fact, activating the lock settings, hive places a lock file whenever it's using a certain temp table (i.e. a temp file); doing so, the cleardanglingscratchdir service can detect if it's safe to delete a certain file.

    --sqoop

Cleans the Sqoop GATEWAY tmp directory (files that are usually under /tmp/sqoop-USERNAME), removing all temp files older than 3 days. It doesn't check if a temp file
is actually in use, so you should be confident that there aren't job older than 3 days running.

    -h, --help, -?

Displays an help message

    -v

Verbose mode, can be used multiple times for increased verbosity (e.g. cloudera_cleaner_script.sh --hive -v -v)

    --user=USR1,USR2

Executes the selected operations only for the specified user[s]. If not specified, then it will be executed for all users.


## hive_cleaning_script.sh

Hive Scratch Directories Cleaner Utility

### Usage:  
```
hive_cleaning_script.sh [days]
```

Search and deletes all hive temp files older than [days] days. If there is a running Hive job that is older than [days], than that job will fail.

### Example:

#### Delete hive temp files older than 3 days

```
hive_cleaning_script.sh 3
```

Warning: this script is meant to be used in CDH < 5.8.4 or Hive < 1.3.0 . For CDH 5.8.4 and above (or Hive 1.3.0 and above) the cleardanglingscratchdir service or the hive_cleaning_script.sh script should be used (see HIVE-15068).