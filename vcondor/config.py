#!/usr/bin/env python
# ******************************************************
# Author       : Zhen-Jing CHENG
# Copyright (C)  2016 IHEP
# Last modified: 2016-09-05 16:13
# Email        : chengzj@ihep.ac.cn
# Filename     : config.py
# Description  : Setup VCondor using config file
# ******************************************************
import os
import sys
from urlparse import urlparse
import ConfigParser


# VMQuota Options Module
# Set default valuse
condor_retrieval_method = {}
condor_q_command = {}
condor_status_command = {}
condor_status_shortcmd = {}
Timeout = 600
cleanup_interval = 600
VmSchedule_interval = 600
DEBUG_MODE = True
VMQuotaIp = ''
VMQuotaPort = 0

client_loop_interval = {}
log_level = ""
log_file = ""
admin_log_file = ""

EnableToRun = 'false'
ClusterType = ""
OS_USERNAME = ""
OS_PASSWORD = ""
OS_AUTH_URL = ""
OS_TENANT_NAME = ""
availability_zone = ""

EndPoint = ""
Username = ""
Password = ""
vm_lifetime = 10080
vm_poller_interval = 5
job_poller_interval = 5
machine_poller_interval = 5
scheduler_interval = 5
vm_start_running_timeout = -1 # Unlimited time
vm_idle_threshold = 5 * 60 # 5 minute default
max_starting_vm = -1
max_destroy_threads = 10
max_keepalive = 60 * 60  # 1 hour default
log_location = None
log_format = "%(asctime)s - %(levelname)s - %(threadName)s - %(message)s"
log_max_size = 2097152


GROUP_SET = []
GROUP_DICT = {}
GROUP_NONEGROUP_DICT = {}
IMAGE_DICT = {}
FLAVOR_DICT = {}
NETWORK_DICT = {}
NETWORK_QUOTA_DICT = {}
GROUP_HOST_DICT = {}


FormatKeysListSend = []
FormatTypeDictSend = {}
FormatKeysListRecv = []
FormatTypeDictRecv = {}



"fix case problem"
class MyConfigParser(ConfigParser.ConfigParser):  
    def __init__(self,defaults=None):  
        ConfigParser.ConfigParser.__init__(self,defaults=None)  
    def optionxform(self, optionstr):  
        return optionstr 


def condor_cmd_setup(config_file=None,group=None):
    """setup condor command using config file and resource group name
    """
    global condor_retrieval_method
    global condor_q_command
    global condor_status_command
    global condor_status_shortcmd
    global client_loop_interval

    global vm_lifetime
    global cleanup_interval
    global vm_poller_interval
    global job_poller_interval
    global machine_poller_interval
    global scheduler_interval

 
    if config_file.has_option(group,"condor_retrieval_method"):
        condor_retrieval_method[group] = config_file.get(group,
                                                "condor_retrieval_method")
    else:
        print "Configuration file problem: %s: condor_retrieval_method SECTION must be " \
                  "configured." % group
        sys.exit(1)


    if config_file.has_option(group, "condor_q_command"):
        condor_q_command[group] = config_file.get(group,
                                                "condor_q_command")
    else:
        print "Configuration file problem: %s: condor_q_command SECTION must be " \
                  "configured." % group
        sys.exit(1)


    if config_file.has_option(group, "condor_status_command"):
        condor_status_command[group] = config_file.get(group,
                                                "condor_status_command")
    else:
        print "Configuration file problem: %s: condor_status_command SECTION must be " \
                  "configured." % group
        sys.exit(1)


    if config_file.has_option(group, "condor_status_shortcmd"):
        condor_status_shortcmd[group] = config_file.get(group,
                                                "condor_status_shortcmd")
        print condor_status_shortcmd
    else:
        print "Configuration file problem: %s: condor_status_shortcmd SECTION must be " \
                  "configured." % group
        sys.exit(1)


def setup(path=None):
    """setup VCondor using config file
    setup will look for a configuration file specified on the command line
    or in ~/.VCondor.conf or /etc/VCondor.conf
    """

    global Timeout
    global cleanup_interval
    global VmSchedule_interval
    global None_Group
    global DEBUG_MODE

    global VMQuotaIp
    global VMQuotaPort

    global EnableToRun
    global ClusterClass
    global EndPoint
    global OS_USERNAME
    global OS_PASSWORD   
    global OS_AUTH_URL   
    global OS_TENANT_NAME 
    global availability_zone

    global Username
    global Password
    global log_level
    global log_location
    global log_file
    global admin_log_file
    global log_location_cloud_admin
    global admin_log_comments
    global log_stdout
    global log_syslog
    global log_max_size
    global log_format

    global GROUP_SET
    global GROUP_DICT
    global GROUP_NONEGROUP_DICT
    global IMAGE_DICT
    global FLAVOR_DICT
    global NETWORK_DICT
    global NETWORK_QUOTA_DICT
    global GROUP_HOST_DICT

    global FormatKeysListSend
    global FormatTypeDictSend
    global FormatKeysListRecv
    global FormatTypeDictRecv




    homedir = os.path.expanduser('~')

    #Find config file
    if not path:
        if os.path.exists("/etc/vcondor/VCondor.conf"):
            path = "/etc/vcondor/VCondor.conf"
            print path
        else:
            print >> sys.stderr, "Configuration file problem: There doesn't " \
                  "seem to be a configuration file. " \
                  "You can put one in /etc/vcondor/VCondor.conf"
            sys.exit(1)

    #Read config file
    config_file = MyConfigParser()
    try:
        config_file.read(path)
    except IOError:
        print >> sys.stderr, "Configuration file problem: There was a " \
              "problem reading %s. Check that it is readable," \
              "and that it exists. " % path
        raise
    except ConfigParser.ParsingError:
        print >> sys.stderr, "Configuration file problem: Couldn't " \
              "parse your file. Check for spaces before or after variables."
        raise
    except:
        print "Configuration file problem: There is something wrong with " \
              "your config file."
        raise


    _group_list = []
    if config_file.has_section("Group-Level-Map-Relations"):
        _group_list = config_file.options("Group-Level-Map-Relations")

    try:
        for _group in _group_list:
            condor_cmd_setup(config_file,_group)
    except Exception, e:
        print e
        sys.exit(1)

    '''
    if config_file.has_option("global", "scheduler_interval"):
        try:
            scheduler_interval = config_file.getint("global", "scheduler_interval")
        except ValueError:
            print "Configuration file problem: scheduler_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "vm_poller_interval"):
        try:
            vm_poller_interval = config_file.getint("global", "vm_poller_interval")
        except ValueError:
            print "Configuration file problem: vm_poller_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "job_poller_interval"):
        try:
            job_poller_interval = config_file.getint("global", "job_poller_interval")
        except ValueError:
            print "Configuration file problem: job_poller_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "machine_poller_interval"):
        try:
            machine_poller_interval = config_file.getint("global", "machine_poller_interval")
        except ValueError:
            print "Configuration file problem: machine_poller_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "cleanup_interval"):
        try:
            cleanup_interval = config_file.getint("global", "cleanup_interval")
        except ValueError:
            print "Configuration file problem: cleanup_interval must be an " \
                  "integer value."
            sys.exit(1)
    '''


    if config_file.has_option("global", "Timeout"):
        try:
            Timeout = config_file.getint("global", "Timeout")
        except:
            print "Configuration file problem: global Timeout must be an " \
                  "integer value."
            sys.exit(1)
 
    if config_file.has_option("global", "cleanup_interval"):
        try:
            cleanup_interval = config_file.getint("global", "cleanup_interval")
        except ValueError:
            print "Configuration file problem: cleanup_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "VmSchedule_interval"):
        try:
            VmSchedule_interval = config_file.getint("global", "VmSchedule_interval")
        except ValueError:
            print "Configuration file problem: VmSchedule_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "DEBUG_MODE"):
        try:
            DEBUG_MODE = config_file.get("global", "DEBUG_MODE")
        except:
            print "Configuration file problem: DEBUG_MODE " 
            sys.exit(1)
 
    if config_file.has_option("global", "EnableToRun"):
        try:
            EnableToRun = config_file.get("global", "EnableToRun")
        except:
            print "Configuration file problem: global.EnableToRun " 
            sys.exit(1)

    if config_file.has_option("VMQuota", "Ip"):
        try:
            VMQuotaIp = config_file.get("VMQuota", "Ip")
        except:
            print "Configuration file problem: VMQuota.Ip" 
            sys.exit(1)

    if config_file.has_option("VMQuota", "Port"):
        try:
            VMQuotaPort = config_file.getint("VMQuota", "Port")
        except:
            print "Configuration file problem: VMQuota.Port" 
            sys.exit(1)

    if config_file.has_section("opennebula"):
        ClusterType = "opennebula"
        if config_file.has_option("opennebula", "EndPoint"):
            try:
                EndPoint = config_file.get("opennebula", "EndPoint")
            except:
                print "Configuration file problem: opennebula.EndPoint " 

        if config_file.has_option("opennebula", "Username"):
            try:
                Username = config_file.get("opennebula", "Username")
            except:
                print "Configuration file problem: opennebula.Username " 

        if config_file.has_option("opennebula", "Password"):
            try:
                Password = config_file.get("opennebula", "Password")
            except:
                print "Configuration file problem: opennebula.Password " 
                sys.exit(1)

    if config_file.has_section("openstack"):
        ClusterType = "openstack"
        if config_file.has_option("openstack", "OS_USERNAME"):
            try:
                OS_USERNAME = config_file.get("openstack", "OS_USERNAME")
            except:
                print "Configuration file problem: openstack.OS_USERNAME " 

        if config_file.has_option("openstack", "OS_PASSWORD"):
            try:
                OS_PASSWORD = config_file.get("openstack", "OS_PASSWORD")
            except:
                print "Configuration file problem: openstack.OS_PASSWORD " 

        if config_file.has_option("openstack", "OS_AUTH_URL"):
            try:
                OS_AUTH_URL = config_file.get("openstack", "OS_AUTH_URL")
            except:
                print "Configuration file problem: openstack.OS_AUTH_URL " 
                sys.exit(1)

        if config_file.has_option("openstack", "OS_TENANT_NAME"):
            try:
                OS_TENANT_NAME = config_file.get("openstack", "OS_TENANT_NAME")
            except:
                print "Configuration file problem: openstack.OS_TENANT_NAME " 
                sys.exit(1)

        if config_file.has_option("openstack", "availability_zone"):
            try:
                availability_zone = config_file.get("openstack", "availability_zone")
            except:
                print "Configuration file problem: openstack.availability_zone " 
                sys.exit(1)

    # Default Logging options
    if config_file.has_option("logging", "log_level"):
        log_level = config_file.get("logging", "log_level")

    if config_file.has_option("logging", "log_file"):
        log_file = os.path.expanduser(config_file.get("logging", "log_file"))

    if config_file.has_option("logging", "log_format"):
        log_format = config_file.get("logging", "log_format", raw=True)

    if config_file.has_option("logging", "log_max_size"):
        try:
            log_max_size = config_file.getint("logging", "log_max_size")
        except:
            print "Configuration file problem: logging.log_max_size must be an interger."
            sys.exit(1)

    # Group options
    if config_file.has_section("Group-Level-Map-Relations"):
        _group_list = config_file.options("Group-Level-Map-Relations")
        print "list is %s" % _group_list
    else:
        _group_list = []

    if (_group_list!=[]):

        GROUP_SET = []
        GROUP_DICT = {}
        for _ResourceGroup in _group_list:
            _Group = config_file.get("Group-Level-Map-Relations", _ResourceGroup)
            _Group = _Group.replace(' ','').replace(',','|')
            temp = []
            temp.append(_Group)
            GROUP_DICT[_ResourceGroup] = temp
            GROUP_SET.append(temp)
        print 'GROUP_SET is %s' % GROUP_SET
        print GROUP_DICT

    else:
        print "Configuration file problem: Group-Level-Map-Relations must be " \
                  "configured."
        sys.exit(1)

    # Image options
    if config_file.has_section("Image"):
        _group_list = config_file.options("Image")
    else:
        _group_list = []

    if (_group_list!=[]):

        IMAGE_DICT = {}
        for _ResourceGroup in _group_list:
            _Image = config_file.get("Image", _ResourceGroup)
            IMAGE_DICT[_ResourceGroup] = _Image
        print IMAGE_DICT

    else:
        print "Configuration file problem: IMAGE SECTION must be " \
                  "configured."
        sys.exit(1)


    # Flavor options
    if config_file.has_section("Flavor"):
        _group_list = config_file.options("Flavor")
    else:
        _group_list = []

    if (_group_list!=[]):

        FLAVOR_DICT = {}
        for _ResourceGroup in _group_list:
            _Flavor = config_file.get("Flavor", _ResourceGroup)
            FLAVOR_DICT[_ResourceGroup] = _Flavor
        print FLAVOR_DICT

    else:
        print "Configuration file problem: FLAVOR SECTION must be " \
                  "configured."
        sys.exit(1)


    # Network options
    if config_file.has_section("Network"):
        _group_list = config_file.options("Network")
    else:
        _group_list = []

    if (_group_list!=[]):

        NETWORK_DICT = {}
        for _ResourceGroup in _group_list:
            _Network = config_file.get("Network", _ResourceGroup)
            NETWORK_DICT[_ResourceGroup] = _Network
        print NETWORK_DICT



    # Network quota
    if config_file.has_section("Network-quota"):
        _netlist = config_file.options("Network-quota")
    else:
        _netlist = []

    if (_netlist!=[]):

        NETWORK_QUOTA_DICT = {}
        for _net in _netlist:
            _quota = 0
            try:
                _quota = config_file.getint("Network-quota",_net)
            except ValueError, e:
                print "Configuration file problem: Network quota must be an " \
                  "integer value."
                sys.exit(1)
            NETWORK_QUOTA_DICT[_net] = _quota
        print NETWORK_QUOTA_DICT


    # Group host options
    if config_file.has_section("Host"):
        _group_list = config_file.options("Host")
    else:
        _group_list = []

    if (_group_list!=[]):

        GROUP_HOST_DICT = {}
        for _ResourceGroup in _group_list:
            try:
                _Host_list = config_file.get("Host", _ResourceGroup).split('|')
                GROUP_HOST_DICT[_ResourceGroup] = _Host_list
            except Exception as e:
                print e
                sys.exit(1)

        print GROUP_HOST_DICT


    # JSONFormatCheck
    if config_file.has_section("JSONFormatCheck-SentToQuotaControl"):
        if "FormatKeysList" not in config_file.options("JSONFormatCheck-SentToQuotaControl"):
            print "Configuration file problem: FormatKeysList OPTION must be " \
                  "configured IN SECTION JSONFormatCheck-SentToQuotaControl."
            sys.exit(1)

        try:
            FormatKeysListSend = config_file.get("JSONFormatCheck-SentToQuotaControl", "FormatKeysList").split(',')
            for key in FormatKeysListSend:
                _type = config_file.get("JSONFormatCheck-SentToQuotaControl", key)
                FormatTypeDictSend[key] = _type
            print FormatKeysListSend
            print FormatTypeDictSend
        except Exception, e:
            print e
            print "Error: Cannot get information from JSONFormatCheck-SentToQuotaControl in config file"
            sys.exit(1)

    else:
        print "Configuration file problem: JSONFormatCheck-SentToQuotaControl SECTION must be " \
                  "configured."
        sys.exit(1)


    if config_file.has_section("JSONFormatCheck-RecvFromQuotaControl"):
        if "FormatKeysList" not in config_file.options("JSONFormatCheck-RecvFromQuotaControl"):
            print "Configuration file problem: FormatKeysList OPTION must be " \
                  "configured IN SECTION JSONFormatCheck-RecvFromQuotaControl."
            sys.exit(1)

        try:
            FormatKeysListRecv = config_file.get("JSONFormatCheck-RecvFromQuotaControl", "FormatKeysList").split(',')
            for key in FormatKeysListRecv:
                _type = config_file.get("JSONFormatCheck-RecvFromQuotaControl", key)
                FormatTypeDictRecv[key] = _type
            print FormatKeysListRecv
            print FormatTypeDictRecv
        except Exception, e:
            print e
            print "Error: Cannot get information from JSONFormatCheck-RecvFromQuotaControl in config file"
            sys.exit(1)

    else:
        print "Configuration file problem: JSONFormatCheck-RecvFromQuotaControl SECTION must be " \
                  "configured."
        sys.exit(1)


if __name__ == '__main__':
    setup()
