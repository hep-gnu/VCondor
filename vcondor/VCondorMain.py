#!/usr/bin/env python
# ******************************************************
# Author       : Zhen-Jing CHENG
# Copyright (C)  2016 IHEP
# Last modified: 2016-08-29 16:01
# Email        : chengzj@ihep.ac.cn
# Filename     : VCondorMain.py
# Description  : The main body for the VCondor, that encapsulates and organizes 
#                all openstack and opennebula HTCondor VMQuota functionally.
# ******************************************************

import sys
import os
import time
import logging
import logging.handlers
import threading
from decimal import *
import json

if sys.version_info[:2] < (2,5):
    print "You need at least Python 2.5 to run VCondor"
    sys.exit(1)

if not './VCondor' in sys.path:
    sys.path.append('./VCondor')

import config as config
import utilities as utilities
from JClient import JClient as JClient
import cloud_management as cloud_management
import job_management as job_management

try:
    import openstackcluster
    from openstackcluster import OpenStackCluster
except:
    print "VCondor needs module openstackcluster to support openstack."

try:
    import opennebulacluster
    from opennebulacluster import OpennebulaCluster
except:
    print "VCondor needs module opennebulacluster to support opennebula."

ACTIVE_SET = [0,'Busy']
INACTIVE_SET = [1,'Idle']
ALL_STATUS_SET = [0,1,'Busy','Idle']
vr_request = {}
data_response = {}
vr_url=''
MIN_VM = 20
MAX_VM = 40
jc = None
flag = 'true'


##
## Functions
##

def has_value(DICT,VALUE):
    """Return whether a dictionary has a certain value."""
    for val in DICT.values():
        if (cmp(repr(val),repr(VALUE))==0):
            logger.info('Find %s in DICT %s' % (repr(VALUE),repr(DICT)))
            return True
    logger.error('Error:%s is not in DICT:%s' % (repr(VALUE),repr(DICT)))
    return False

def get_key_by_value(DICT,VALUE):
    """Return key by a certain value in a dictionary."""
    for key, val in DICT.items():
        if (cmp(repr(val),repr(VALUE))==0):
            logger.info('Find %s in DICT %s, return key %s' % (repr(VALUE),repr(DICT),key))
            return key
    logger.error('Error:%s is not in DICT:%s' % (repr(VALUE),repr(DICT)))
    return None

def get_request_by_group(group_name_list):
    """Get request string by input :resource_group_name"""
    if has_value(GROUP_DICT,group_name_list):
        ResourceGroup = get_key_by_value(GROUP_DICT, group_name_list)
        vr_request = {"ResID":ResourceGroup}
        logger.debug('Send Json Data: %s' % vr_request)
        return vr_request
    else:
        logger.error('Send a null Json Data to VR! System failed!')
        return 'null'

def get_vr_response(json_req,json_resp):
    """Send a json string to vr written by Li HaiBo and catch the responsei."""
    pass

def decode_vr_response(json_resp,name):
    """Decode a json reponse string by name like ResID or Min."""
    pass

#@profile
def VmSchedule(group_name_list):
    """Main function for each group, including vm creating and destroying."""

    global flag
    logger.info('Starting a thread:VmSchedule for %s' % group_name_list)
    num_vm_to_launch = 0
    num_vm_to_destroy = 0
    jv = None
    EnableToRun = False
    if config.EnableToRun=='true':
        EnableToRun = True
    else:
        EnableToRun = False

    if config.ClusterType=='openstack':
        Cluster = openstackcluster.OpenStackCluster(name='',username='',password='',tenant_id='',auth_url='')
    if config.ClusterType=='opennebula':
        Cluster = opennebulacluster.OpennebulaCluster(name='',username='',password='',tenant_id='',auth_url='')

    ResourcePool = cloud_management.ResourcePool()
    JobPool = job_management.JobPool(name='condor', condor_query_type='local')

    while(EnableToRun):
        config.setup()
        try:
            jc = JClient(host=config.VMQuotaIp, port=config.VMQuotaPort, bufsize=1024, allow_reuse_addr=True)
        except Exception,e:
            logger.error("Unable to create JClient object or connect to VMQuota %s:%s\n%s" % (config.VMQuotaIp,
                        config.VMQuotaPort,e))
            time.sleep(60)
            continue

    	"""Try to connect to VMQuota designed by Li HaiBo""" 
        string_request = get_request_by_group(group_name_list)
        if (string_request!='null'):
            try:
                logger.debug('String_request sent to VMQuota: %s' % string_request)
                result = jc.JSONFormatCheck(string_request, config.FormatKeysListSend, config.FormatTypeDictSend)
                if(result==0):
                    logger.error("Data_request:%s format is wrong! You shall check it carefully!" % string_request)
                    continue
                data_response = jc.oneRequest(string_request)
                logger.error('RECV data:%s' % data_response)
                logger.debug('RECV data:%s' % data_response)
                result = jc.JSONFormatCheck(data_response, config.FormatKeysListRecv, config.FormatTypeDictRecv)
                if(result==0):
                    logger.error("Data_response:%s format is wrong! You can check Remote VMQuota!" % data_response)
                    time.sleep(600)
                    continue
                MIN_VM = int(data_response['MIN'])
                MAX_VM_TO_LAUNCH = int(data_response['AVAILABLE'])
                logger.debug("Data_response: MIN_VM is %d,MAX_VM_TO_LAUNCH is %d." % (MIN_VM,MAX_VM_TO_LAUNCH))
            except Exception, e:
                logger.error("Unable to decode data_response by VR: %s" % e)
                time.sleep(60)
                continue
            except StandardError,e:
                logger.error("Unable to decode data_response by VR: %s" % e)
                time.sleep(60)
                continue
        else:
            logger.error("Unable to create request string which will be sent to VMQuota.")
            time.sleep(600)
            continue
        
        """Try to execute command 'condor_status -l' and transform the output into machine objects."""
        try:
            condor_status_machinelist = ResourcePool.resource_query_local(get_key_by_value(GROUP_DICT, group_name_list))
        except Exception as e:
            logger.error("Some error occured when trying to excute function ResourcePool.resource_query_local().")
            continue

        """Try to find vms running on openstack and transform the output into VM objects."""
        vms = ()
        vms_new = ()
        try:
            vms = Cluster.get_vms_local()
        except Exception as e:
            logger.error("Some error occured when trying to excute function get_vms_local(): %s" % e)
            continue

        """Try to modify VM object's job attribute with results from command 'condor_status -l'."""
        try:
            vms_new = ResourcePool.update_vmslist_from_machinelist(vms,condor_status_machinelist)
            Cluster.vms = vms_new
        except Exception as e:
            logger.error("Some error occured when trying to excute function ResourcePool.update_vmslist_from machinelist: %s" % e)

        """Try to figure out how many jobs for each group and those jobs' activity(idle or running). """
        job_ads = JobPool.job_query_local(get_key_by_value(GROUP_DICT, group_name_list))

        vm_name = group_name_list[0] + "_VM"
        image_name = ''
        flavor_uuid = ''
        network_list = ''

        try:
            if  has_value(GROUP_DICT, group_name_list) and IMAGE_DICT.has_key(get_key_by_value(GROUP_DICT, group_name_list)):
                image_name = IMAGE_DICT[get_key_by_value(GROUP_DICT, group_name_list)]
            else:
                logger.error("Unable to find image for group:%s." % '|'.join(group_name_list))
        except Exception as e:
            logger.error(e)
            sys.exit(1)

        try:
            if has_value(GROUP_DICT, group_name_list) and FLAVOR_DICT.has_key(get_key_by_value(GROUP_DICT, group_name_list)):
                flavor_uuid = FLAVOR_DICT[get_key_by_value(GROUP_DICT, group_name_list)]
            else:
                logger.error("Unable to find flavor uuid for group:%s." % '|'.join(group_name_list))
                raise Exception("Unable to find flavor uuid for group:%s." % '|'.join(group_name_list))
        except Exception as e:
    	    logger.error(e)
            sys.exit(1)


        try:
            if has_value(GROUP_DICT, group_name_list) and NETWORK_DICT.has_key(get_key_by_value(GROUP_DICT, group_name_list)):
                network_list = NETWORK_DICT[get_key_by_value(GROUP_DICT, group_name_list)]
            else:
                logger.error("Unable to find network name for group:%s." % '|'.join(group_name_list))
                raise Exception("Unable to find network name for group:%s." % '|'.join(group_name_list))
        except Exception as e:
    	    logger.error(e)
            continue

        num_vm_busy = Cluster.num_vms_by_group_activity(vms=vms_new,group=group_name_list,activity=ACTIVE_SET)
        logger.info("Group: %s num_vm_busy: %s " % (group_name_list,num_vm_busy))
        num_vm_idle = Cluster.num_vms_by_group_activity(vms=vms_new,group=group_name_list,activity=INACTIVE_SET)
        logger.info("Group: %s num_vm_idle: %s " % (group_name_list,num_vm_idle))
        num_vm_all = num_vm_busy+num_vm_idle
        logger.info("Group: %s num_vm_all: %s " % (group_name_list,num_vm_all))

        num_job_idle = JobPool.get_jobcount_by_group_activity(jobs=job_ads,Group=group_name_list,JobStatus='idle')
        logger.info("Group: %s num_job_idle: %s " % (group_name_list,num_job_idle))
        num_job_running = JobPool.get_jobcount_by_group_activity(jobs=job_ads,Group=group_name_list,JobStatus='running')
        logger.info("Group: %s num_job_running: %s " % (group_name_list,num_job_running))

        '''
        if(num_vm_idle>0 and num_job_idle>0) or (num_job_running!=num_vm_busy):
            logger.error('Cannot handle this: num_vm_idle=%s num_job_idle=%s' % (num_vm_idle,num_job_idle))
            logger.error('Cannot handle this: num_job_running=%s num_vm_busy=%s' % (num_job_running,num_vm_busy))
            time.sleep(10)
            continue
        '''


        if (num_job_idle>0)and(num_vm_idle>0)and(num_job_idle>=num_vm_idle):
            num_vm_busy = num_vm_busy+num_vm_idle
            num_job_idle = num_job_idle-num_vm_idle
            num_vm_idle = 0

        if (num_job_idle>0)and(num_vm_idle>0)and(num_job_idle<num_vm_idle):
            num_vm_busy = num_vm_busy+num_job_idle
            num_vm_idle = num_vm_idle-num_job_idle
            num_job_idle = 0

        num_vm_all = num_vm_busy+num_vm_idle

        MAX_VM = MAX_VM_TO_LAUNCH+num_vm_all
 
        if(num_vm_all<MIN_VM):
            if(num_vm_all+num_job_idle<MAX_VM):
                num_vm_to_launch = (num_job_idle if (num_job_idle+num_vm_all)>=MIN_VM else (MIN_VM-num_vm_all))
            else:
                num_vm_to_launch = MAX_VM-num_vm_all
            num_vm_to_destroy = 0

        if(num_vm_all > MAX_VM):
            num_vm_to_launch = 0
            if(num_vm_busy > MAX_VM):
                num_vm_to_destroy = num_vm_all-MAX_VM
            if(num_vm_busy > MIN_VM and num_vm_busy<=MAX_VM):
                num_vm_to_destroy = num_vm_idle
            else:
                num_vm_to_destroy = num_vm_all-MIN_VM

        if(num_vm_all>=MIN_VM) and (num_vm_all<=MAX_VM):
            if(num_job_idle > 0):
                num_vm_to_launch = (num_job_idle if num_job_idle<=(MAX_VM-num_vm_all) else (MAX_VM-num_vm_all))
            else:
                num_vm_to_launch = 0
	    num_vm_to_destroy = (num_vm_idle if num_vm_busy>=MIN_VM else (num_vm_all-MIN_VM))

        if(num_vm_to_launch > 0) and (num_vm_to_destroy>0):
            if(num_vm_to_destroy>=num_vm_to_launch):
                num_vm_to_destroy = num_vm_to_destroy-num_vm_to_launch 
                num_vm_to_launch = 0
            else:
                num_vm_to_launch = num_vm_to_launch-num_vm_to_destroy
                num_vm_to_destroy=0

        if config.ClusterType=='openstack':
            nova = Cluster.get_creds_nova()
            Host_free_vcpus_dict = {}
            for Host in config.GROUP_HOST_DICT[get_key_by_value(GROUP_DICT, group_name_list)]:
                try:
                    all_vcpu = nova.hosts.get(Host)[0].to_dict()['resource']['cpu']
                    used_vcpu = nova.hosts.get(Host)[1].to_dict()['resource']['cpu']
                    free_vcpu = all_vcpu-used_vcpu
                    Host_free_vcpus_dict[Host]=free_vcpu
                except Exception as e:
                    logger.error("Unable to find vcpus for group %s: %s" % (group_name_list,e))
                except:
                    logger.info("No physical machine for group %s ?" % group_name_list)
        
        logger.info("launch %d instance for group:%s!" % (num_vm_to_launch,group_name_list))
        logger.info("destroy %d instance for group:%s!"% (num_vm_to_destroy,group_name_list))

        try:
            network_name_list = network_list.split('|')
        except Exception,e:
            logger.error(e)
            sys.exit(1)

        num_vm_launched = 0
        CreateVmThread = []
        if config.ClusterType=='opennebula':
            for num in range(0,num_vm_to_launch):
                t = threading.Thread(target=Cluster.vm_create,args=(vm_name,'',get_key_by_value(GROUP_DICT, group_name_list),image_name,flavor_uuid,config.ClusterType,'',[]))
                CreateVmThread.append(t)


        if config.ClusterType=='openstack':
            for vm_network in network_name_list:
                try:
                    ip_quota = NETWORK_QUOTA_DICT[vm_network]-Cluster.num_vms_by_network(vms=vms_new,network=vm_network)
                    num_vm_to_launch_by_net = (num_vm_to_launch if (num_vm_to_launch<=ip_quota) else ip_quota)
                    for num in range(0,num_vm_to_launch_by_net):
                        for host in Host_free_vcpus_dict.keys():
                            if Host_free_vcpus_dict[host]>0:
                                avail_zone = config.availability_zone + host
                                Host_free_vcpus_dict[host] -= 1
                                break
                            else:
                                avail_zone = config.availability_zone
                        t = threading.Thread(target=Cluster.vm_create,args=(vm_name,'',get_key_by_value(GROUP_DICT, group_name_list),image_name,flavor_uuid,avail_zone,vm_network,[]))
                        CreateVmThread.append(t)
                    num_vm_to_launch = num_vm_to_launch-num_vm_to_launch_by_net
                    num_vm_launched = num_vm_launched+num_vm_to_launch_by_net
                except Exception as e:
                    logger.error("Unable to lauch instances by method vm_create for group %s.\n%s" % ('|'.join(group_name_list),e))

        for tr in CreateVmThread:
            tr.start()
        for tr in CreateVmThread:
            tr.join()

        
        if (num_vm_to_destroy>0 and num_vm_to_destroy<=num_vm_idle):
            try:
                Cluster.vm_destroy_by_Group_JobActivity(count=num_vm_to_destroy,group=group_name_list,activity=INACTIVE_SET,vms=vms_new)
            except Exception as e:
                logger.error("Unable to destroy %d instances  for group %s \n %s" 
        			    % (num_vm_to_destroy,'|'.join(group_name_list),e))
        elif (num_vm_to_destroy>0 and num_vm_to_destroy>num_vm_idle):
            try:
                Cluster.vm_destroy_by_Group_JobActivity(count=num_vm_idle,group=group_name_list,activity=INACTIVE_SET,vms=vms_new)
            except Exception as e:
                logger.error("Unable to destroy %d instances for group %s." 
                        % (num_vm_to_destroy,'|'.join(group_name_list)))

        time.sleep(config.VmSchedule_interval)
        
    return 0



def main():
    """Main Function of VCondor."""

    """Setup VCondor using config file"""
    config.setup()

    global GROUP_SET
    global GROUP_DICT
    global IMAGE_DICT
    global FLAVOR_DICT
    global NETWORK_DICT
    global NETWORK_QUOTA_DICT
    global log_file
    global logger

    GROUP_SET = config.GROUP_SET
    GROUP_DICT = config.GROUP_DICT
    IMAGE_DICT = config.IMAGE_DICT
    FLAVOR_DICT = config.FLAVOR_DICT
    NETWORK_DICT = config.NETWORK_DICT
    NETWORK_QUOTA_DICT = config.NETWORK_QUOTA_DICT

    logger = logging.getLogger() 
    logger.setLevel(config.log_level) 
    fh = None
    if config.log_max_size:
        fh = logging.handlers.RotatingFileHandler(
                                   config.log_file,
                                   maxBytes=config.log_max_size)
    else:
        try:
            fh = logging.handlers.WatchedFileHandler(config.log_file,)
        except AttributeError:
            # Python 2.5 doesn't support WatchedFileHandler
            fh = logging.handlers.RotatingFileHandler(config.log_file,)
    fh.setFormatter(logging.Formatter(config.log_format))  
    logger.addHandler(fh)  

    threads = []
    for group_list in GROUP_SET:
        t = threading.Thread(target=VmSchedule,args=(group_list,))
        threads.append(t)

    for tr in threads:
        tr.start()
    for tr in threads:
        tr.join()

    return 0


##
## Main Functionality
##

main()

    

