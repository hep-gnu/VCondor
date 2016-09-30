#!/usr/bin/env python

from SocketServer import (ThreadingTCPServer as TTCP,
                          StreamRequestHandler as SRH)
from novaclient import client
import json
import os, sys
import logging
import time
import subprocess

from db import *
from parse_conf import *

#opennebulla account info, please modify it to your environment.
Username=''
Password=''
EndPoint=''


HOST = ''
PORT = ''
ADDR = (HOST, PORT)
INVALID_STATUS_LIMIT = ''
RESOURCE_QUOTA = ''

vmqueue=[]
INVALID_STATUS_LIMIT = ''

con=get_db_con()

def get_logger():
    logger = logging.getLogger('vmquota')
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler("/var/log/vmquota.log")
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

LOG = get_logger()

def display(displaying):
    if len(displaying) >= 256:
        return '<data not shown>'
    else:
        return displaying

# For reusing the same allowed host:
class ReusableTTCP(TTCP):
    allow_reuse_address = True

class JServerRequestHandler(SRH):
    received = True # dummy
    sending = ''
    data = ''
    
    # Usually you should call the 'main' procedure through this method.
    # What you've received and JSON-decoded(loaded), and would be encoded
    # (dumped into a string) and sent, is in the variable self.data .
    def furtherCallback(self):
        # When not implemented:
        pass
    
    def JSONDecode(self, *args):
        # String (when received) to JSON
        if args:
            data = args[0]
        else:
            data = self.received
        try:
            ret = json.loads(data)
        except Exception, e:
            LOG.exception(e)
            ret = ()
        finally:
            return ret
    
    def JSONEncode(self, *args):
        # JSON to string (then send the string)
        if args:
            data = args[0]
        else:
            data = self.data
        try:
            ret = json.dumps(data)
        except Exception, e:
            LOG.exception(e)
            ret = ''
        finally:
            return ret
    
    def handle(self):
        clnt = self.clnt = str(self.client_address)
        LOG.info('%s connected' % clnt)
        while self.received:
            self.received = self.rfile.readline().strip() # real data
            self.displaying = self.received
            if len(self.displaying) >= 256:
                self.displaying = '<data not shown>'
            LOG.debug('%s RCVD: %s' % (self.clnt, display(self.received)))
            if self.received == '':
                LOG.info('%s Blank string received, stop handling' % self.clnt)
                return
            
            # If furtherCallback() wasn't overridden, then it will just repack
            # and send unchanged JSON objects.
            self.data = self.JSONDecode(self.received)
            self.furtherCallback()
            #self.sending = self.JSONEncode(self.data)
            #LOG.debug('%s SEND: %s' % (self.clnt, display(self.sending)))
            #self.wfile.write('%s%s' % (self.sending, os.linesep))
            #if not self.sending:
                #LOG.info('%s Nothing to send, stop handling' % self.clnt)
                #return


class MyRequestHandler(JServerRequestHandler):
    def furtherCallback(self):
        LOG.debug('%s JSON: %s' % (self.clnt, self.data))
        ResID=(self.data['ResID']).upper()
        #print "=========Resource name is ", self.data['ResID']
        if ResID not in vmqueue:
            LOG.debug("do not have the resource:%s"%ResID)
            return	

        #loop the queue in the pool,get the available quota for the ResID
        used_num = vm_quota_allocate(ResID)
        res_data = get_res_avail(ResID,used_num)

        self.data = ""
        self.data = {'ResID':ResID,"MIN":res_data[0],"AVAILABLE":res_data[1]}

        self.sending = self.JSONEncode(self.data)
        #LOG.info('%s SEND: %s' % (self.clnt, self.sending))
        #self.wfile.write(self.sending)
        LOG.debug('%s SEND: %s' % (self.clnt, display(self.sending)))
        self.wfile.write('%s%s' % (self.sending, os.linesep))
        if not self.sending:
            LOG.info('%s Nothing to send, stop handling' % self.clnt)
            return


class cloud_management():
    def __init__(self,):
        CLOUDCONF="/etc/cloud.conf"
        cloud_conf=parse_cloud_conf(CLOUDCONF)
        self.cloud_type=cloud_conf['cloud_type']
        if self.cloud_type == 'openstack':
            self.Username = cloud_conf['Username']
            self.Password = cloud_conf['Password']
            self.EndPoint = cloud_conf['EndPoint']
            self.Version = int(cloud_conf['VERSION'])
            self.Tenant = cloud_conf['TENANT']
        elif self.cloud_type == 'opennebula':
            self.Username = cloud_conf['Username']
            self.Password = cloud_conf['Password']
            self.EndPoint = cloud_conf['EndPoint']
        else:
            print "unidentified cloud type!"

    def get_image_list(self):
        if self.cloud_type == 'openstack':
            print "openstack"
            nt =client.Client(self.Version,self.Username, self.Password,self.Tenant,self.EndPoint)
            images = nt.images.list()
            images = []
            for i in images:
                imagename = str(i)[8:-1]
                images.append(imagename)
            return images
        elif self.cloud_type == 'opennebula':
            print "opennebula"
            images = []
            cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a describe -r os_tpl -o json_pretty" %(self.EndPoint,self.Username,self.Password)
            sp = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (response, err)=sp.communicate(input=None)
            returncode = sp.returncode
            response = json.loads(response)
            instance_num=len(response)
            for instance in response:
                name = instance['title']
                images.append(name)
            return images

    def get_res_run(self,resid):
        running = 0
        if self.cloud_type == 'openstack':
            print "openstack"
            #nt = client.Client(2, "admin", "cuit;opstk|000", "admin", "http://192.168.81.26:5000/v2.0")
            nt =client.Client(self.Version,self.Username, self.Password,self.Tenant,self.EndPoint)
            images = nt.images.list() 
            for i in images:
                imagename = str(i)[8:-1]
                if resid in imagename:
                    imageid = nt.images.find(name=imagename).id
                    running += len(nt.servers.list(search_opts={'image':imageid}))
        elif sefl.cloud_type == 'opennebula':
            print "opennebula"    
            cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a describe -r compute -o json_pretty" %(self.EndPoint,self.Username,self.Password)
            sp = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (response, err)=sp.communicate(input=None)
            returncode = sp.returncode
            response = json.loads(response)
            for instance in response:
                name = instance['attributes']['occi']['core']['title']
                if res_name.lower() in vm:
                    running += 1 
        
        return running

def vm_quota_allocate(resid):
    used_num = 0
    LOG.debug("Tag1:begin loop the queue!")
    for i in vmqueue:
        used_num += get_res_occuped(i,resid) 
        LOG.debug("quota used: %s %s"%(i,used_num))
    LOG.debug("Tag2: end loop,all vm quota used is %s\n"%used_num)

    return used_num  

def get_res_avail(resid,used_num):
    global con
    cloudCluster = cloud_management()
    resid_info = []
    all_available = int(RESOURCE_QUOTA - used_num)
    LOG.debug("all available resource is %s"%all_available)
    res_info = get_res_info(con,resid)
    LOG.debug("res info is %s"%res_info)
    running =  cloudCluster.get_res_run(resid)
    #calculate the available vm num for the specfic queue
    res_max_num = int(res_info[2]-running)
    LOG.debug("max - running num is %s"%res_max_num)
    if res_max_num <=0:
        print "running now is the max!"
        avail = 0
        res_data=[res_info[1],avail]
        allocate_time =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        update_res_avail(con,resid,avail,allocate_time)
        LOG.debug("res data is %s\n"%res_data)
        return res_data

    res = int(all_available - res_max_num)
    if res >= 0:
        avail = res_max_num
    elif res <0:
        avail = all_available
    if avail <= 0:
        print "There is no availalbe resource now!"
        avail = 0
    allocate_time =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    update_res_avail(con,resid,avail,allocate_time)   
    LOG.debug("avial is %s"%avail)
    res_data=[res_info[1],avail]
    LOG.debug("res data is %s\n"%res_data)
    return res_data

def vmqueueInit():
    queue = {} 
    f = file("/root/vpmanagerGNU/vmquota-one/vm-image.conf")
    s = json.load(f)
    f.close()
    for i in range(len(s)):
        vmque = s[i]['queue']
        vmimagename = s[i]['imagename']
        #vmimage = nt.images.find(name=vmimagename).id
        queue[vmque]=""
    LOG.debug("queue info is %s"%queue)
    return queue

def get_image_list():
    images = []
    cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a describe -r os_tpl -o json_pretty" %(EndPoint,Username,Password)
    sp = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (response, err)=sp.communicate(input=None)
    returncode = sp.returncode
    response = json.loads(response)
    instance_num=len(response)
    for instance in response:
        _name = instance['title']
        images.append(_name)
    return images

def getimageByVMqueue2(res_name):
    imagelist = get_image_list()
    image = []
    print "queue name is ",res_name.lower()
    for i in imagelist:
        if res_name.lower() in i:
            image.append(i)
    return image  

def get_res_occuped(res_name,res_id):
    global con,INVALID_STATUS_LIMIT
    cloudCluster = cloud_management()
    res_item=[]
    res_item = get_res_info(con,res_name)
    res_avail = res_item[4]
    res_alloc_time = res_item[5]
    LOG.debug("%s,available:%s,allocate_time:%s"%(res_name,res_item[4],res_item[5]))
    if not res_alloc_time:
        LOG.debug("The resource has not ever allocate,get it's running num!")
        occuped_num =  cloudCluster.get_res_run(res_name)
        return occuped_num
        
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    now_str = time.mktime(time.strptime(now,"%Y-%m-%d %H:%M:%S"))
    pre_str = time.mktime(time.strptime(res_item[5],"%Y-%m-%d %H:%M:%S"))
   
    delta = int(now_str - pre_str)
    print "Time delta is %s"%delta
    res = int(delta - INVALID_STATUS_LIMIT)
    if res < 0:
	#donot exceed time
        print "%s resource don't timeout\n"%res_name
        running =  cloudCluster.get_res_run(res_name)
        if res_id == res_name:
            occuped_num = running
            LOG.debug("---donot timeout:right res,update running---")
            update_res_running(con,res_name,running)
        else:
            occuped_num = res_item[4]+running
    else:
        LOG.debug("%s resource has timeout"%res_name)
        occuped_num =   cloudCluster.get_res_run(res_name)
        if res_id == res_name:
            LOG.debug("---timeout:right res,update running---")
            update_res_running(con,res_name,occuped_num)
    LOG.debug("%s occpued_num is %s"%(res_name,occuped_num))
    return  occuped_num

def get_local_vms():
    vms = []
    cmd = "occi -s -e %s -n basic -u '%s' -p '%s' -a describe -r compute -o json_pretty" %(EndPoint,Username,Password)
    sp = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (response, err)=sp.communicate(input=None)
    returncode = sp.returncode
    response = json.loads(response)
    for instance in response:
        _name = instance['attributes']['occi']['core']['title']
        vms.append(_name)
    return vms

def get_res_running(res_name):
    global con
    running = 0
    #imagename = getimageByVMqueue2(res_name)
    vms = get_local_vms()
    for vm in vms:
        if res_name.lower() in vm:
            running += 1
    print "running is ",running        
    #update_res_running(con,res_name,running)
    LOG.debug("%s running num is %s"%(res_name,running))
    return running


def main():
    global HOST
    global PORT
    global ADDR
    global NVALID_STATUS_LIMIT
    global RESOURCE_QUOTA
    global vmqueue

    CONF= "/etc/vmquota.conf"
    service=parse_server_conf(CONF)
    HOST = service['HOST']
    PORT = int(service['PORT'])
    ADDR = (HOST, PORT)
    INVALID_STATUS_LIMIT = int(service['INVALID_STATUS_LIMIT'])
    RESOURCE_QUOTA =int(service['RESOURCE_QUOTA'])

    ADDR = (HOST, PORT)
    print ADDR

    vmqueue = parse_vmquota_conf(CONF)

    tcpServ = ReusableTTCP(ADDR, MyRequestHandler)
    tcpServ.daemon_threads = True
    try:
        tcpServ.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        # When shutting down the whole thing:
        LOG.info('exiting')
        tcpServ.shutdown()

if __name__ == '__main__':
    main()
