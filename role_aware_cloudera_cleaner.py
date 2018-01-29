#!/usr/bin/env python

import os
import sys
import argparse
import logging
import socket
import ConfigParser
from subprocess import Popen, PIPE, STDOUT
from cm_api.api_client import ApiResource

API_VERSION = 10

script_path = os.path.dirname(os.path.realpath(__file__))
bash_scripts_path = os.path.join(script_path, "bash_scripts")

debug_mode=False

def execute_script(script_name, args):
    full_path = os.path.join(bash_scripts_path, script_name)
    cmd = [full_path]
    cmd = cmd + args
    if debug_mode:
        print(" ".join(map(str, cmd)))
        return
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    output = p.stdout.read()
    print(output)
    err_output = p.stderr.read()
    print(err_output, file=sys.stderr)

def execute_cleaning(cluster_name, cluster_version, service_type, role_type, is_leader):
    if role_type == "GATEWAY" and service_type == "HDFS":
        logging.info("Host has role {0} for service {1} in cluster '{2}'".format(role_type, service_type, cluster_name))
        if is_leader:
            execute_script("cloudera_cleaner_script.sh", ["--hdfs"])
        else:
            logging.info("Not running HDFS cleaning because this host is not the leader.")
    elif role_type == "GATEWAY" and service_type == "HIVE":
        logging.info("Host has role {0} for service {1} in cluster '{2}'".format(role_type, service_type, cluster_name))
        if is_leader:
            if cluster_version < "5.8.4":
                logging.info("Running 'naive' cleaning script because CDH version is < 5.8.4")
                execute_script("hive_cleaning_script.sh", ["7"])
            else:
                execute_script("cloudera_cleaner_script.sh", ["--hive"])
        else:
            logging.info("Not running HDFS cleaning because this host is not the leader.")
    elif role_type == "GATEWAY" and service_type == "SQOOP_CLIENT":
        logging.info("Host has role {0} for service {1} in cluster '{2}'".format(role_type, service_type, cluster_name))
        execute_script("cloudera_cleaner_script.sh", ["--sqoop"])


def is_role_leader(service, role_type, role_name):
    """Checks if a certain role instance is the leader for that role type.
    Given the complete list of roles for a particular service, the leader
    for a role type is simply the role instance whose role name is the 
    first alfabetically among all roles with the same role type."""
    logging.debug("Checking if Leader for service {0} roletype {1} is {2}".format(
        service.name, role_type, role_name))
    for role in service.get_all_roles():
        if role.type and role.type == role_type and role.name < role_name:
            logging.debug("Role Leader for service {0} roletype {1} is {2}".format(
                service.name, role.type, role.name))
            return False
    return True

def clean_host(cm_api):
    my_hostname = socket.gethostname()
    logging.debug("My Hostname: {0}".format(my_hostname))
    hosts = cm_api.get_all_hosts(view="full")
    for host in hosts:
        if host.hostname == my_hostname:
            role_refs = host.roleRefs
            for ref in role_refs:
                cluster_name = ref.clusterName
                cluster = cm_api.get_cluster(cluster_name)
                cluster_version = cluster.fullVersion
                service = cluster.get_service(ref.serviceName)
                service_type = service.type
                role = service.get_role(ref.roleName)
                role_type = role.type
                is_leader = is_role_leader(service, role_type, ref.roleName)
                execute_cleaning(cluster_name, cluster_version, service_type,
                               role_type, is_leader)
            break


def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser(
        description='Script that executes the necessary cleaning operations depending on the host roles. Queries the Cloudera Manager host to retrieve role information.\nCommand line arguments overrides values defined in config.ini')
    # Add arguments
    parser.add_argument(
        '--cm-host', type=str, help='Cloudera Manager host (e.g. "127.0.0.1")', required=False)
    parser.add_argument(
        '--cm-port', type=int, help='Cloudera Manager port', required=False)
    parser.add_argument(
        '--cm-user', type=str, help='Cloudera Manager username (e.g. "admin", although you should not use the admin user but a Read-Only user)', required=False)
    parser.add_argument(
        '--cm-pass', type=str, help='Cloudera Manager user\'s password', required=False)
    parser.add_argument('--debug-mode', help='Prints only the shell scripts without actually running them', action='store_true')   
    # Array for all arguments passed to script
    args = parser.parse_args()

    # parse the configuration file
    config = ConfigParser.ConfigParser()
    config.read("config.ini")
    cm_host = config.get("Main", "cm_host")
    cm_port = config.get("Main", "cm_port")
    cm_user = config.get("Main", "cm_user")
    cm_pass = config.get("Main", "cm_pass")

    # Override config values from commandline arguments
    if args.cm_host is not None:
        cm_host = args.cm_host
    if args.cm_port is not None:
        cm_port = args.cm_port
    if args.cm_user is not None:
        cm_user = args.cm_user
    if args.cm_pass is not None:
        cm_pass = args.cm_pass

    global debug_mode
    debug_mode=args.debug_mode

    api = ApiResource(cm_host, cm_port, cm_user, cm_pass, version=API_VERSION)

    clean_host(api)


if __name__ == "__main__":
    main()
