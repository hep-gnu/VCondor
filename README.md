# VCondor 0.1.0 README

## Introduction

VCondor is a virtual computing resource pool manager based on HTCondor. VCondor is composed of two components: vcondor and vmquota. Vcondor checks the status of different queue and get the available VM number and create new VMs or destroy existing VMs.Vmquota checks the information of VM pool and requirements of different applications to allocate or reserve resources.

## Prerequisites

* A working Condor 7.5.x or later install(details below)
* Python 2.6+
* rOCCI client for using Opennebula OCCI

## Basic Steps to get Jobs Running via VPManager

1.  Install Prerequiste libraries
2.  Condfigure HTCondor,VCondor and vmquota
3.  Setup a VM Image with Condor installed
4.  Setup a VM Template with Image in the above
5.  Check the mysqld service on, establish a resource item corresponding to the resource_pool in the resource table.
6.  Start VCondor and VMQuota. Then you can submit job(s)

## Preparing VM Images

The VM images you would like to run jobs with need to be prepared to join your Condor pool. You need to install Condor, and configure it as a worker that
will join your Condor pool.


## Relation between user-group, resource_group, VM-Image, and Virtual machine

You might need to know about what resource_group means in VCondor before you get it running.

In VCondor, Resource_group is a set of cluster user groups who use the same image to boot their VMs. For example, Resource_group 'JUNO' includes user group 'juno' of IHEPCloud. Correspondingly, the image should be named like 'WN-JUNO-40G'. The group 'juno' uses the image 'WN-JUNO-40G' to boot their VMs and get jobs running. The configuration file of condor_startd daemon - 'STARTD.conf' in VMs must be configured like that:
    Start = ( TARGET.AcctGroup =?= "juno")          
So the VM is able to run jobs of user group juno.
On the other hand, the name of VM is assigned by Vcondor, and it should be named like "juno-vm-1" which contains the resource name.
In Vmquota, it defines the maximum and minimum value for one queue. Please note that the maximum should not exceed the quota of the cloud platform.

## Configuration

### ./VCondor/VCondor.conf

The VCondor configuration file allows you to configure most of its functionality, and you'll need to open it up to get a usable installation. All of its options are described inline in the example configuration file VCondor.conf, which is included with VCondor.

### ./vmquota/vmquota.conf

The vmqouta configuration file allows you to configure most of its functionality, and you'll need to open it up to get a usable installation. All of its options are described inline in the example configuration file vmquota.conf. 

## How to Use

1)First, start the vmquota process, it will listen to the request info from vcondor. Please use the commands:
$cd vmquota
$python VmquotaMain.py
2)Then, start the vcondor process, it will check the status of condor queue and start the virtual machines once getted the result of resource from vmquota. Please use the commands:
$cd Vcondor 
$python VCondorMain.py
