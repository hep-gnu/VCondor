#!/usr/bin/env python
# ******************************************************
# Author       : Zhen-Jing CHENG
# Copyright (C)  2016 IHEP
# Last modified: 2016-09-05 16:21
# Email        : chengzj@ihep.ac.cn
# Filename     : cloud_management.py
# Description  : ResourcePool class
# ******************************************************
# set ts=8 sw=8

import sys
import string
import logging
import logging.handlers
import threading
import subprocess
import sys
from decimal import *
from collections import defaultdict

import cluster_tools
import config as config
from utilities import determine_path
from utilities import get_or_none
from utilities import ErrTrackQueue
from utilities import splitnstrip
import utilities as utilities

log = None


##
##  CLASSES
##
class ResourcePool():

    """Stores and organizes a list of Cluster resources."""
    ## Instance variables
    resources = []
    machine_list = []
    prev_machine_list = []
    vm_machine_list = []
    prev_vm_machine_list = []
    master_list = []
    retired_resources = []
    config_file = ""

    def __init__(self,):

        self.logger = logging.getLogger('MainLog')
        self.fh = None
        if config.log_max_size:
            self.fh = logging.handlers.RotatingFileHandler(
                                       config.log_file,
                                       maxBytes=config.log_max_size)
        else:
            try:
                self.fh = logging.handlers.WatchedFileHandler(config.log_file,)
            except AttributeError:
                # Python 2.5 doesn't support WatchedFileHandler
                self.fh = logging.handlers.RotatingFileHandler(config.log_file,)
        self.fh.setLevel(config.log_level)
        self.fh.setFormatter(logging.Formatter(config.log_format))
        self.logger.addHandler(self.fh)


    ## Instance methods
    def resource_query_local(self,group):
        """
        resource_query_local -- does a Query to the condor collector
        Returns a list of dictionaries with information about the machines
        registered with condor.
        """
        self.logger.info("Querying Condor Collector with %s" % config.condor_status_command)
        condor_status=condor_out=condor_err=""
        try:
            condor_status = config.condor_status_command[group]
            sp = subprocess.Popen(condor_status, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (condor_out, condor_err) = sp.communicate(input=None)
            self.logger.debug("Query Condor Worker Node out: %s" % condor_out)
            self.logger.debug("Query Condor Worker Node err: %s" % condor_out)
	    
        except OSError:
            self.logger.error("OSError occured while doing condor_status - will try again next cycle.")
            return []
        except:
            self.logger.error("Problem running %s, unexpected error: %s" % (condor_status, condor_err))
            return []
	
        return self._condor_status_to_machine_list(condor_out)


    
    def _condor_status_to_machine_list(self,condor_status_output):
        """
        _condor_status_to_machine_list - Converts the output of
               condor_status -l to a list of dictionaries with the attributes
               from the Condor machine ad.
               returns [] is there are no machines
        """

        machines = []

        try:
            # Each classad is seperated by '\n\n'
            raw_machine_classads = condor_status_output.split("\n\n")
            # Empty condor pools give us an empty string or stray \n in our list
            raw_machine_classads = filter(lambda x: x != "" and x != "\n", raw_machine_classads)

            for raw_classad in raw_machine_classads:
                classad = {}
                classad_lines = raw_classad.splitlines()
                for classad_line in classad_lines:
                    classad_line = classad_line.strip()
                    (classad_key, classad_value) = classad_line.split(" = ", 1)
                    classad_value = classad_value.strip('"')
                    #self.logger.debug('Attributes of a condor worker node:')
                    if classad_key in ['Machine','HardwareAddress','MyAddress','Activity','Name','Start','AccountingGroup','RemoteOwner','RemoteUser','JobId','GlobalJobId']:
                        classad[classad_key] = classad_value

                self.logger.debug('%s ' % classad)
                machines.append(classad)
        except Exception as e:
            self.logger.error('Exception occured when trying to convert the output of condor_status -l: %s' % e)

        return machines


    def update_vmslist_by_image(self, vms, ImageName, Group):
        try:
            for vm in vms:
                if vm.image_name == ImageName:
                    vm.resource_group_str = repr(Group)
                    vm.group = repr(Group)
                    vm.activity = 'Null'
        except Exception as e:
            print e
            self.logger.error('Exception when trying to update vms Group by image_name:%s Exception:%s' % (ImageName,e))
        return vms


    def update_vmslist_from_machinelist(self, vms, machinelist):
	machine_num = len(machinelist)
        for machine in machinelist:
            try:
                hostname = resource_group_str = activity = owner = jobid = MacAddress = IpAddress =\
                     address_master = state = activity = vmtype = start_req = \
                     remote_owner = slot_type = total_slots = ""
                if machine.has_key('Machine'):
                    hostname = machine['Machine']
                if machine.has_key('Activity'):
                    activity = machine['Activity']
                if machine.has_key('RemoteOwner'):
                    owner = machine['RemoteOwner'].split('@')[0]
                if machine.has_key('GlobalJobId'):
                    jobid = machine['GlobalJobId']
                if machine.has_key('HardwareAddress'):
                    MacAddress = machine['HardwareAddress']
                if machine.has_key('MyAddress'):
                    IpAddress = machine['MyAddress'].split('<')[1].split(':')[0]

                try:
                    self.update_job_status_for_vm(vms,MacAddress,owner,activity,jobid,hostname)
                    self.update_job_status_for_vm(vms,IpAddress,owner,activity,jobid,hostname)
                except Exception, e:
                    #self.logger.error("Failed to update a VM Obj by condor_status command!")
                    print e
            except Exception as e:
                #self.logger.error("Failed to update a VM Obj by condor_status command!")
                print e
                sys.exit(1)
        return vms
    
    
    def get_vm_by_ip_or_mac(self,vms,address=''):
        """Find the vm by ipaddress or macaddress and return the vm object.
        """
        for vm in vms:
            if (vm.ipaddress==address) or (vm.macaddress==address):
        	return vm
        return None

    def update_job_status_for_vm(self,vms,address,owner='',activity='',jobid='',hostname=''):
        """For a certain vm object, update status of the job running on it."""
	for vm in vms:
	    if (vm.ipaddress==address) or (vm.macaddress==address):
        	try:
            	    vm.owner = repr(owner)
            	    vm.activity = repr(activity)
            	    vm.jobid = repr(jobid)
            	    vm.hostname = repr(hostname)
       		except Exception, e:
            	    print e
                    self.logger.error("Unable to update job status for a certain VM: %s %s Exception:%s" % (vm.id,vm.name,e))
		    return 0
		return 1
        return None


'''
class VMDestroyCmd(threading.Thread):
    """
    VMCmd - passing shutdown and destroy requests to a separate thread
    """

    def __init__(self,OpenstackCluster,count=0,group=[],activity=[],vms=[]):
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self.OpenstackCluster = OpenstackCluster
        self.count = count
        self.group = group
        self.activity = activity
        self.vms = vms
        self.result = None
        self.init_time = time.time()
    def run(self):
        self.result = self.OpenstackCluster.vm_destroy_by_Group_JobActivity(count=self.count,
                group=self.group,activity=self.activity,vms=self.vms)
    def get_result(self):
        return self.result
    def get_vm(self):
        return self.vm
'''
