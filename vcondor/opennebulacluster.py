#!/usr/bin/env python
# ******************************************************
# Author       : Zhen-Jing CHENG
# Copyright (C)  2016 IHEP
# Last modified: 2016-09-05 16:34
# Email        : chengzj@ihep.ac.cn
# Filename     : opennebulacluster.py
# Description  : opennebulacluster class
# ******************************************************

import sys
import time
import thread
import threading
import logging
import logging.handlers
import subprocess
import random

import cluster_tools
import config as config
import utilities as utilities

if not '../VCondor' in sys.path:
    sys.path.append('../VCondor')

# Use this global variable for logging.
log = None

# 
# This is an sub-class inherited from ICluster class and VM class.
# It defines and realizes some attributes and actions like creating a VM or 
# destroying a VM .
# 
#
class OpennebulaCluster(cluster_tools.ICluster,cluster_tools.VM):
    """
    OpennebulaCluster Class - Represents a opennebula cluster


    """
    global username
    global password
    global auth_url
    vms = []
    vms_by_group = []
    vms_by_group_activity = []
    # A list of possible statuses for VM.
    VM_STATES = {
            "BUILD" : "Starting",
            "ACTIVE" : "Running",
            "SHUTOFF" : "Shutdown",
            "SUSPENDED" : "Suspended",
            "PAUSED" : "Paused",
            "ERROR" : "Error",
    }
    security_groups = ['default']
    

    def __init__(self,username,password,tenant_id,auth_url,name='opennebula cluster',):
    
        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name,username=username,password=password,tenant_id=tenant_id,auth_url=auth_url,)

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

        security_group = ["default"]
        self.security_group = security_group
        self.username = username if username else "admin"
        self.password = password if password else ""
        self.tenant_id = tenant_id if tenant_id else ""
        self.auth_url = auth_url if auth_url else ""

    def __getstate__(self):
        pass

    def __setstate__(self, state):
        pass

    def get_vms_local(self):
        """ Create vms objects list from vm on Opennebula at this moment."""
        vms = []
        try:
            cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a describe -r compute -o json_pretty" %(config.EndPoint,config.Username,config.Password)
            self.logger.info(cmd)
            sp = subprocess.Popen(cmd, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (response, err) = sp.communicate(input=None)
            self.logger.debug('occi cmd out:%s err:%s' % (response, err))
            returncode = sp.returncode
            response = utilities.JSONDecode(response)
            self.logger.debug('Response to json output:%s' % response)
        except Exception as e:
            self.logger.error("Unable to get instance list by rOCCI: %s" % e)
            return 0
        
        try:
            instance_num = len(response)
        except Exception as e:
            self.logger.error("Unable to get instance num by rOCCI. Exception:%s" % e)
            return 0

        for instance in response:
            self.logger.debug('Next is a instance descrption:')
            self.logger.debug('%s' % instance)
            try:
                _uuid = instance['attributes']['occi']['core']['id']
                _name = instance['attributes']['occi']['core']['title']
            except Exception as e:
                self.logger.error('Unable to get VM name and VM uuid. Exception:%s' % e)
                _uuid = ''
                _name = ''

            try:
                _network = instance['links'][1]['attributes']['occi']['core']['title']
                _ipaddress = instance['links'][1]['attributes']['occi']['networkinterface']['address']
                _macaddress = instance['links'][1]['attributes']['occi']['networkinterface']['mac']
            except Exception as e:
                self.logger.error('Unable to get VM:%s network information. Exception:%s' % _(uuid,e))
                _network = ''
                _ipaddress = ''
                _macaddress = ''

            _os_tpl = None
            try:
                for mixin in instance['mixins']:
                    _temp = mixin.split('os_tpl#')
                    if len(_temp)>1:
                        _os_tpl = _temp[1]
            except Exception as e:
                self.logger.error('Unable to get VM:%s os_tpl information. Exception:%s' % _(uuid,e))
                _os_tpl = ''

            _flavor = ''
            try:
                new_vm = cluster_tools.VM(name = _name, uuid = _uuid,resource_group_str='',group='',owner='',activity='',
			 jobid='',hostname='',ipaddress = _ipaddress,macaddress = _macaddress, 
                         network = _network, clusteraddr='',clusterport='',
            		cloudtype='opennebula', image_name = _os_tpl, flavor = _flavor,cpucores=0,memory=0,
            		storage=0,keep_alive=0)
                vms.append(new_vm)
            except Exception as e:
                print e
                self.logger.error('Unable to create a VM object for VM:%s. Exception:%s' % _(uuid,e))
                return 0

        return vms
        
          
    def set_vms(self,vms):
        pass


    def clear_vms(self,vms):
        """ Try to clear the vm objects list.
        """
        del vms[:]

    def num_vms(self,vms):
        """Returns the number of VMs running on the cluster (in accordance
        to the vms[] list)
        """
        return len(vms)

    def num_vms_by_group(self,vms=[],group=[]):
        """Returns the number of VMS that run jobs of specific group.
        """
        vm_count = 0
        for vm in vms:
            if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
                vm_count += 1
        return vm_count

    def num_vms_by_group_activity(self,vms=[],group=[],activity=[]):
        """Returns the number of VMs that run jobs of specific group and
        specific activity.
        """
        vm_count = 0
        for vm in vms:
	    if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
	    	if (str(activity).find(vm.activity)>0 or str(vm.activity).find(str(activity))>0):
                    vm_count += 1
	    
        return vm_count

    def num_vms_by_network(self,vms=[],network=""):
        """Returns the number of VMS that belongs to a certain Network.
        """
        vm_count = 0
        for vm in vms:
            if (vm.network ==  network):
                vm_count += 1
        return vm_count

    def get_vm_by_id(self,uuid,vms):
        """Find the vm by id and return the vm object. 
        """
        for vm in vms:
            if str(vm.uuid) == uuid:
                return vm
        return None
    def get_vms_by_group(self,group=[],vms=[]):
        """Return the number of VMs which belongs to a certain VM.
        """
        vms_by_group = []
        for vm in vms:
            if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
                vms_by_group.append(vm)
        return vms_by_group


    def get_vms_by_group_activity(self,group,activity,vms):
        """Return the number of VMs which belongs to a certain VM and a certain activity.
        """
        vms_by_group_activity = []
        for vm in vms:
            self.logger.debug('group = %s,activity = %s,vm.group = %s,vm.activity=%s' 
                    % (group,activity,vm.group,vm.activity))
            if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
                if (str(activity).find(vm.activity)>0 or str(vm.activity).find(str(activity))>0):
                    vms_by_group_activity.append(vm)
        self.logger.debug('Find those vms:%s' % vms_by_group_activity)
        return vms_by_group_activity


    def vm_create(self,vm_name="", resource_group_str="", group="", imageId="",instance_flavorId="",
                availbility_zone="",vm_networkassoc="",securitygroup=[]):
        """ Create a VM on Opennebula. VM network and securitygroup is defined in Opennebula template."""
        threads = []
        t = threading.Thread(target=self._vm_create,args=(vm_name,group,imageId,instance_flavorId,availbility_zone))
        threads.append(t)
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        return 0

        


    def _vm_create(self,vm_name='', group='', imageId='', instance_flavorId='', availbility_zone=''):
        """ Opennebula VM create thread."""

        VmId = ''
        try:
            cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a create -r compute --mixin os_tpl#%s --mixin resource_tpl#%s -t occi.core.title=%s" %(config.EndPoint,config.Username,config.Password,imageId,instance_flavorId,vm_name)
            self.logger.info(cmd)
            sp = subprocess.Popen(cmd, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = sp.communicate(input=None)
            returncode = sp.returncode
            self.logger.debug('returncode:%s out:%s err:%s' % (returncode,out,err))
            if returncode != 0:
                self.logger.error("Unable to launch a VM by rOCCI.You shall check this message.\n%s\n%s" % (out, err))
                return 0
            VmId = out
        except Exception as e:
            self.logger.error("Exception occured while trying to create a VM on Opennebula by OCCI.\n%s" % e)
            return 0

        Sec = 0
        _ipaddress = ''
        while True:
            time.sleep(1)
            Sec = Sec+1
            if Sec>20:
                """Delete this VM."""
                self.logger.error("Unable to get ipaddress from VM %s.I had to shut it down." % VmId)

                try:
                    cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a delete -r %s" % (config.EndPoint,config.Username,config.Password,VmId)
                    self.logger.info(cmd)
                    sp = subprocess.Popen(cmd, shell=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (out, err) = sp.communicate(input=None)
                    self.logger.debug('out:%s err:%s' % (out,err))
                except:
                     self.logger.error("Failed to delete VM %s Exception %s" % (VmId,e))
                     return 0
                break

            try:
                cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a describe -o json_pretty -r %s" %(config.EndPoint,config.Username,config.Password,VmId)
                self.logger.debug(cmd)
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
                self.logger.debug('out:%s err:%s' % (out,err))
                out = utilities.JSONDecode(out)
                self.logger.debug('convert out to json: %s' % out)
                _ipaddress = out[0]['links'][1]['attributes']['occi']['networkinterface']['address']
                self.logger.debug('New vm ip %s' % _ipaddress)
                break
            except Exception as e:
                self.logger.error('Exception when trying to get ip from a new VM: %s' % e)
                continue
        self.logger.info("Create an instance! %s %s " % (VmId,_ipaddress))
        result = self._checkVmInCondor(vm_ip=_ipaddress,group=group,tm=config.Timeout,HopeValue=0)
        if(result!=0):
            self.logger.error("Attention,  administrator! Condor on VM IP:%s UUID:%s of group %s doesn't start to work in 10 minutes. \
                              I had to shut it down" % (_ipaddress,VmId,group)) 

            try:
                cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a delete -r %s" % (config.EndPoint,config.Username,config.Password,VmId)
                self.logger.info(cmd)
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
                self.logger.debug('out:%s err:%s' % (out,err))
            except Exception as e:
                self.logger.error("Failed to delete VM %s Exception:%s" % (VmId,e))
                return 0
        thread.exit_thread()
       
    def _checkVmInCondor(self, vm_ip='', group='', tm=600, HopeValue=0):
        """ Check a VM in condor pool or not."""
        cmd=out=err=""
        returncode = 1-HopeValue
        while (returncode!=HopeValue and tm>0):
            time.sleep(2)
            tm = tm-10
            try:
                cmd = config.condor_status_shortcmd[group] + " -format \"%%s\\n\" StartdIpAddr|grep '%s:'" % vm_ip
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
                returncode = sp.returncode
            except OSError:
                self.logger.error("OSError occured while doing condor_status - will try again next cycle.")
                return None
            except Exception as e:
                return None

        return returncode
            

    def _checkVmInRightGroup(self, vm_ip='', group='', tm=600):
        """ Check a VM's state of condor is in Right Group or not."""
        cmd=out=err=""
        returncode = -1
        while (returncode!=0 and tm>0):
            time.sleep(2)
            tm = tm-10
            try:
                cmd = config.condor_status_shortcmd[group] + " -format \"%%s@\" MyAddress -format \"%%s\\n\" Start |grep '%s:'|awk -F '@' '{print $2}'" % vm_ip
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
            except OSError:
                self.logger.error("OSError occured while doing condor_status - will try again next cycle.")
                return None
            except Exception, e:
                return None

            temp_group = out.split('"',-1)
            lenth = len(temp_group)
            num = lenth
            resource_group_list = []
            resource_group_str = ""
            group_list = []
            while (num>1):
                num -= 2
                resource_group_list.append(temp_group[num])
                resource_group_str = "|".join(
                                          str(i) for i in resource_group_list.reverse())
            group_list.append(resource_group_str)
            LocalGroup = utilities.get_key_by_value(config.GROUP_DICT,group_list)
            returncode = cmp(LocalGroup,group)
            self.logger.error('%s Local condor Start is %s,group is %s' % (vm_ip,group_list,config.GROUP_DICT[group]))
            if group_list==config.GROUP_DICT[group]:
                returncode = 0

        return returncode

    def vm_destroy_by_Group_JobActivity(self,count=0,group=[],activity=[],vms=[]):
        """ Destroy  VMs on Openenbula which runs specific jobs in specifc activity of the group."""
        VmsTemp = self.get_vms_by_group_activity(group,activity,vms,)
        try:
            NumList = random.sample(range(len(VmsTemp)),count)
        except Exception as e:
            self.logger.error("Encounting an error when trying to create some random numbers from 0 to %d. Exception:%s" % (len(NumList)-1,e))

        threads = []
        for number in NumList:
            try:
                t = threading.Thread(target=self.vm_destroy,args=(VmsTemp[number],))
                threads.append(t)
            except Exception as e:
                self.logger.error("Encounting an error when trying to destroy some vms on Opennebula which runs \
                        specific jobs in %s activity of the group %s. Exception:%s" % (activity,group,e))
                return 0
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        return 1

    def vm_destroy(self,vm):
        """ Destroy a VM on Opennebula."""
        VmId = vm.uuid
        try:
            cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a delete -r /compute/%s" % (config.EndPoint,config.Username,config.Password,VmId)
            self.logger.info(cmd)
            sp = subprocess.Popen(cmd, shell=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = sp.communicate(input=None)
            self.logger.debug('out:%s err:%s' % (out,err))
        except Exception as e:
            self.logger.error("Failed to delete VM %s. Exception:%s" % (VmId,e))
            return 0
            thread.exit_thread()
 
        thread.exit_thread()




 
        
