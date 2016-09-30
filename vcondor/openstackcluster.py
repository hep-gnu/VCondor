#!/usr/bin/env python
# ******************************************************
# Author       : Zhen-Jing CHENG
# Copyright (C)  2016 IHEP
# Last modified: 2016-09-29 11:08
# Email        : chengzj@ihep.ac.cn
# Filename     : openstackcluster.py
# Description  : 
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
class OpenStackCluster(cluster_tools.ICluster,cluster_tools.VM):
    """
    OpenStackCluster Class - Represents a openstack cluster
    """

    global username
    global password
    global tenant_id
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
    

    def __init__(self,username,password,tenant_id,auth_url,name='openstack cluster',):
    
        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name,username=username,password=password,tenant_id=tenant_id,auth_url=auth_url,)

        self.logger = logging.getLogger('main')
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

        try:
            import novaclient.client as nvclient
            import novaclient.exceptions
            #import keystoneclient.v2_0.client as ksclient
        except:
            print "Unable to import novaclient - cannot use native openstack cloudtypes"
            sys.exit(1)
	
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
        """ Create vms objects list from vm on Openstack at this moment."""
        nova = self.get_creds_nova()
        try:
            instances = nova.servers.list()
        except Exception as e:
            self.logger.error("Unable to get instance list by method nova.servers.list().")
            return 0
        
        vms = []
        try:
            instance_num = len(instances)
            for instance in instances:
                _uuid = getattr(instance,'id')
                _name = getattr(instance, 'name')
                try:
                    _temp_dict = getattr(instance, 'addresses')
                    _temp_list = _temp_dict.keys()
                    _network = _temp_list[0]
                    _ipaddress = _temp_dict[_temp_list[0]][0]['addr']
                    _macaddress = _temp_dict[_temp_list[0]][0]['OS-EXT-IPS-MAC:mac_addr']
                except Exception as e:
                    _network = ""
                    _ipaddress = ""
                     _macaddress = ""
                    self.logger.error("Unable to get ipaddress or macaddress by novaclient for vm: %s %s." % (_name,_uuid))
                try:
                    _temp_dict = getattr(instance,'image')
                    _image_id = _temp_dict['id']
                    _flavor = getattr(instance, 'flavor')['id']
                except Exception as e:
                    _image_id = ""
                    _flavor = ""
                    self.logger.error("Unable to get image_id or flavor by novaclient for vm: %s %s." % (_name,_uuid))

                try:
                    new_vm = cluster_tools.VM(name = _name, uuid = _uuid,resource_group_type='',group='',owner='',activity='',
                            jobid='',hostname='',ipaddress = _ipaddress,macaddress = _macaddress, 
                             network = _network, clusteraddr='',clusterport='',
                            cloudtype='openstack', image_name = _image_id, flavor = _flavor,cpucores=0,memory=0,
                            storage=0,keep_alive=0)
                    vms.append(new_vm)
        		except Exception as e:
        		    print e
        except Exception as e:
            self.logger.error("Unable to transfer nova.servers.list() into cluster_tools vm objects.")
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
            #if vm.group in group:
            if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
                vms_by_group.append(vm)
        return vms_by_group

    @staticmethod
    def get_vms_by_group_activity(group,activity,vms):
        """Return the number of VMs which belongs to a certain VM and a certain activity.
        """
        vms_by_group_activity = []
        for vm in vms:
            if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
                if (str(activity).find(vm.activity)>0 or str(vm.activity).find(str(activity))>0):
                    vms_by_group_activity.append(vm)
        return vms_by_group_activity

    def vm_create(self,vm_name="", resource_group_type="", group="", imageId="",instance_flavorId="",
                availbility_zone="",vm_networkassoc="",securitygroup=[],min_count=1,max_count=1):
        """ Create a VM on Openstack."""

        nova = self.get_creds_nova()

        if len(securitygroup) != 0:
            sec_group = []
            for group in securitygroup:
                if group in self.security_groups:
                    sec_group.append(group)
            if len(sec_group) == 0:
                self.logger.debug("No defined security groups for job - trying default value from cloud_resources.conf")
                sec_group = self.security_groups
        else:
            sec_group = self.security_groups

        try:
            imageobj = nova.images.find(name=imageId)
        except novaclient.exceptions.EndpointNotFound:
            self.logger.error("Endpoint not found, are your region settings correct for %s" % self.name)
            return -4
        except Exception as e:
            self.logger.warning("Exception occurred while trying to fetch image via name: %s %s" % (imageId, e))
            try:
                imageobj = nova.images.get(imageId)
                self.logger.debug("Got image via uuid: %s" % imageId)
            except novaclient.exceptions.EndpointNotFound:
                self.logger.error("Endpoint not found, are your region settings correct for %s" % self.name)
                return -4
            except Exception as e:
                self.logger.error("Unable to fetch image via uuid: %s %s" % (imageId, e))
                return

        try:   
            flavorobj = nova.flavors.find(name=instance_flavorId)
        except Exception as e:
            self.logger.debug("Exception occurred while trying to get flavor by name: %s - will attempt to use name value as a uuid." % instance_flavorId)
            try:
                flavorobj = nova.flavors.get(instance_flavorId)
                self.logger.debug("Got flavor via uuid: %s" % instance_flavorId)
            except Exception as ex:
                self.logger.error("Exception occurred trying to get flavor by uuid: %s. Exception: %s" % (instance_flavorId,ex))
                return

        # find the network id to use if more than one network
        netid = []
        if vm_networkassoc:
            try:
                network = self._find_network(vm_networkassoc)
                if network:
                    netid.append({ 'net-id': network.id})
                else:
                    self.logger.error("Unable to find network named: %s on %s" % (vm_networkassoc, self.name))
                    return -5
            except Exception,e:
                netid = []
        else:
            netid = []
            return -5

        threads = []
        t = threading.Thread(target=self._vm_create,args=(nova,group,group+time.strftime('-%Y-%m-%dT%H:%M:%S'),imageobj,flavorobj,netid,availbility_zone))
        threads.append(t)
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        return 0
 
    def _vm_create(self, nova=None, group='', vm_name='', imageobj=None, flavorobj=None, netid=None, availbility_zone=''):
        """ Openstack VM create thread."""
        instance = None
        try:
            HostNum = len(config.GROUP_HOST_DICT[group])
        except Exception as e:
            HostNum = 0 
            print e

        self.logger.debug("Create a VM on host:%s for group:%s" % (availbility_zone,group))
        try:
            instance = nova.servers.create(name=vm_name,image=imageobj,flavor=flavorobj,nics=netid,availability_zone=availbility_zone,min_count=1,max_count=1)
        except novaclient.exceptions.OverLimit as e:
            self.logger.error("Unable to create VM without exceeded quota on %s: %s" % (self.name, e.message))
        except Exception as e:
            self.logger.error("Unhandled exception while creating vm on %s: %s" %(self.name, e))
            return -1
        
        if instance:
            Sec = 0
            while True:
                time.sleep(1)
                Sec = Sec + 1	
                instance_id = instance.id
                instance = nova.servers.get(instance_id)
                if Sec>20:
                    self.logger.error("Unable to get ipaddress or macaddress from instance %s.I had to shut it down." % instance_id)
                    instance.delete()
                    return -1
                try:
                    temp_dict = getattr(instance, 'addresses')
                    temp_list = temp_dict.keys()
                    instance_ip = temp_dict[temp_list[0]][0]['addr']
                    instance_mac = temp_dict[temp_list[0]][0]['OS-EXT-IPS-MAC:mac_addr']
                except Exception as e:
                    instance_ip=""
                    instance_mac=""
                    continue
                if instance_ip != "":
                    break
            self.logger.info("Create an instance! %s %s " % (instance_id,instance_ip))
            result = self._checkVmInCondor(vm_ip=instance_ip,group=group,tm=config.Timeout,HopeValue=0) 
            if(result!=0):
                self.logger.error("Attention,  administrator! Condor on VM IP:%s UUID:%s of group %s doesn't start to work in 10 minutes. \
                              I had to shut it down" % (instance_ip,instance_id,group))
                instance.delete()
                return 0
            thread.exit_thread()

        else:
            self.logger.error("Failed to create instance on %s" % self.name)
            return -1
        thread.exit_thread()


    def _checkVmInCondor(self, vm_ip='', group='', tm=600, HopeValue=0):
        """ Check a VM in condor pool or not."""
        cmd=out=err=""
        returncode = 1-HopeValue
        while (returncode!=HopeValue and tm>0):
            time.sleep(2)
            tm = tm-2
            try:
                cmd = config.condor_status_shortcmd[group] + " -format \"%%s\\n\" StartdIpAddr|grep '%s:'" % vm_ip
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
                returncode = sp.returncode
            except OSError:
                self.logger.error("OSError occured while doing condor_status.")
                return None
            except Exception, e:
                self.logger.error("Problem running %s, unexpected error %s" % (cmd,e))
                return None
        return returncode
            
    def vm_destroy_by_Group_JobActivity(self,count=0,group=[],activity=[],vms=[]):
        """ Destroy  VMs on Openstack which runs specific jobs in specifc activity of the group."""
        vms_temp = OpenStackCluster.get_vms_by_group_activity(group,activity,vms)
        try:
            num_list = random.sample(range(len(vms_temp)),count)
        except Exception as e:
            self.logger.error("Encounting an error when trying to create some random numbers from 0 to %d. Exception:%s" % (len(vms_temp)-1,e))
        threads = []
        for number in num_list:
            try:
                t = threading.Thread(target=self.vm_destroy,args=(vms_temp[number],group,))
                threads.append(t)
            except Exception as e:
                self.logger.error("Encounting an error when trying to destroy some vms on Openstack which runs \
                            specific jobs in %s activity of the group %s." % (activity,group))
                return 0
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        return 1


    def vm_destroy(self,vm,group):
        """ Destroy a VM on Openstack."""
        nova = self.get_creds_nova()
        import novaclient.exceptions

        if isinstance(group, list):
            group = group[0]

        try:
            instance = nova.servers.get(vm.uuid)
            instance.stop()
            instance.delete()
        except novaclient.exceptions.NotFound as e:
            self.logger.error("VM id: %s name: %s not found: removing from vrmanager" % (vm.uuid,vm.name))
        except:
            self.logger.error("Failed to log exception properly?")
            return 1
        
        thread.exit_thread()


    def vm_poll(self,vm):
        """Query Openstack for status information of VMs."""
        import novaclient.exceptions
        nova = self.get_creds_nova()
        instance = None
        try:
            instance = nova.servers.get(vm.uuid)
        except novaclient.exceptions.NotFound as e:
            self.logger.exception("VM %s not found : %s" % (vm.uuid,e))
            vm.status = self.VM_STATES['ERROR']
        except Exception as e:
            try:
                self.logger.error("Unexpected exception occurred polling vm %s: %s" % (vm.uuid, e))
            except:
                self.logger.error("Failed to log exception properly: %s" % vm.uuid)
        if instance and vm.status != self.VM_STATES.get(instance.status, "Starting"):
            vm.last_state_change = int (time.time())
            self.logger.debug("VM: %s.Changed from %s to %s." % (vm.name,vm.status, self.VM_STATES.get(instance.status, "Starting")))
        if instance and instance.status in self.VM_STATES.keys():
            vm.status = self.VM_STATES[instance.status]
        elif instance:
            vm.status = self.VM_STATES['ERROR']
        return vm.status

    def vm_status_poll(self,vm):
        """Query Openstack for status information if a specific VM."""
        pass

        
    def get_creds_nova(self):
        """Get an auth token to Nova."""
        try:
            import novaclient.client as nvclient
            import novaclient.exceptions
        except Exception as e:
            print e
            self.logger.error(e)
            return 0
            #sys.exit(1)
        creds = self._get_nova_creds()
        nova = nvclient.Client('2',**creds)
	return nova
        """
        try:
            client = nvclient.Client('2',self.username,self.password,self.tenant_id,self.auth_url)
        except Exception as e:
            self.logger.error("Unable to create connection to %s: Reason: %s" % (self.name, e))
        return client
        """

    def _get_nova_creds(self):
        """Return a cred for function get_creds_nova. """
        d = {}
        """
        d['username'] = os.environ['OS_USERNAME']
        d['api_key'] = os.environ['OS_PASSWORD']
        d['auth_url'] = os.environ['OS_AUTH_URL']
        d['project_id'] = os.environ['OS_TENANT_NAME']
        """

        d['username'] = config.OS_USERNAME
        d['api_key'] = config.OS_PASSWORD
        d['auth_url'] = config.OS_AUTH_URL
        d['project_id'] = config.OS_TENANT_NAME
        return d

    def _find_network(self, name):
        nova = self.get_creds_nova()
        network = None
        try:
            networks = nova.networks.list()
            for net in networks:
                if net.label == name:
                    network = net
        except Exception as e:
            self.logger.error("Unable to list networks on %s Exception: %s" % (self.name, e))
        return network





 
        
