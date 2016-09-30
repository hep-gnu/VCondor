#!/usr/bin/env python
def parse_vmquota_conf(conf):
    fd=open(conf,"r")
    ques=[]
    que_attr={}
    for line in fd:
        line=line.strip()
        if line and not line.startswith("#"):
            #print line
            (key,value)=line.split("=")
            key=key.strip()
            value=value.strip()
            #print key,"=>", value
            if key=="Que":
                ques=value.split(",")
                #print "ques is ", ques
    fd.close()
    return ques

def parse_server_conf(conf):
    fd=open(conf, "r")
    service={}
    for line in fd:
        line=line.strip()
        if line and not line.startswith("#"):
            (key,value)=line.split("=")
            key=key.strip()
            value=value.strip()
            service[key]=value
    fd.close()
    return service


def parse_cloud_conf(conf):
    fd=open(conf, "r")
    cloudinfo={}
    for line in fd:
        line=line.strip()
        if line and not line.startswith("#"):
            (key,value)=line.split("=")
            key=key.strip()
            value=value.strip()
            cloudinfo[key]=value
    fd.close()
    return cloudinfo
