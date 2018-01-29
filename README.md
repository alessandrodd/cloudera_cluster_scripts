# cloudera_cluster_scripts
Various scripts that can be useful in a Cloudera cluster environment

# role_aware_cloudera_cleaner
A python script that, using Cloudera Manager API, detects the configured role types for the executing host and executes the needed scripts that are placed in [bash_scripts](bash_scripts) directory in order to clean the cluster. It should be executed on all hosts of the cluster to be effective, since some cleaning operations are cluster-wide and are only executed from an elected host to avoid duplication.