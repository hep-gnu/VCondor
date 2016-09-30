#!/usr/bin/env python
# ******************************************************
# Author       : Zhen-Jing CHENG
# Copyright (C)  2016 IHEP
# Last modified: 2016-09-05 16:16
# Email        : chengzj@ihep.ac.cn
# Filename     : utilities.py
# Description  : utility functions not specific to VCondor
# ******************************************************

import os
import sys
import socket
import logging
import subprocess
import time
import gzip
import json
import errno
from urlparse import urlparse
from datetime import datetime
import config
try:
    from Openssl import crypto
except ImportError:
    pass
from cStringIO import StringIO

def determine_path ():
    """Determine the absolute path using a filename"""
    try:
        root = __file__
        if os.path.islink (root):
            root = os.path.realpath (root)
        return os.path.dirname (os.path.abspath (root))
    except:
        print "I'm sorry, but something is wrong."
        print "There is no __file__ variable. Please contact the author."
        sys.exit ()

LEVELS = {'DEBUG': logging.DEBUG,
          'VERBOSE': logging.DEBUG-1,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL,}

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

def get_key_by_value(DICT,VALUE):
    """Return key by a certain value in a dictionary."""
    for key, val in DICT.items():
        if (cmp(repr(val),repr(VALUE))==0):
           return key
    return None

def get_vrmanager_logger():
    """Gets a reference to the 'vrmanager' log handle."""
    logging.VERBOSE = LEVELS["VERBOSE"]
    logging.addLevelName(logging.VERBOSE, "VERBOSE")
    log = logging.getLogger("vrmanager")
    setattr(log, "verbose", lambda *args: log.log(logging.VERBOSE, *args))
    log.addHandler(NullHandler())

    return log

def get_hostname_from_url(url):
    """Return the hostname parsed from a full url."""
    return urlparse(url)[1].split(":")[0]

def get_or_none(config, section, value):
    """Return the value of a config option if it exists, none otherwise."""
    if config.has_option(section, value):
        return config.get(section, value)
    else:
        return None

def JSONDecode(*args):
    # String (when received ) to JSON.
    if args:
        data = args[0]
    else:
        return 0
    try:
        ret = json.loads(data)
    except Exception, e:
        ret = ()
    finally:
        return ret

def splitnstrip(sep, val):
    """Return a list of items trimed of excess whitespace from a string(typically comma separated)."""
    return [x.strip() for x in val.split(sep)];


def get_globus_path(executable="grid-proxy-init"):
    """
    Finds the path for Globus executables on the machine. 
    If GLOBUS_LOCATION is set, and executable exists, use that,
    otherwise, check the path to see if its in there,
    otherwise, raise an exception.
    """

    try:
        os.environ["GLOBUS_LOCATION"]
        retcode = subprocess.call("$GLOBUS_LOCATION/bin/%s -help" % executable, shell=True, 
                                  stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)

        if retcode != 0:
            raise EnvironmentError(retcode, "GLOBUS_LOCATION is in your environment, but unable to call '%s'" % executable)
        else:
            return os.environ["GLOBUS_LOCATION"] + "/bin/"

    except:
        retcode = subprocess.call("%s -help" % executable, shell=True, 
                                  stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
        if retcode == 127:
            raise EnvironmentError(retcode, "'%s' is not in your PATH" % executable)
        elif retcode != 0:
            raise EnvironmentError(retcode, "'%s' is in your PATH, but it returned '%s'" % (executable, retcode))
        else:
            return ""

def match_host_with_condor_host(hostname, condor_hostname):
    """
    match_host_with_condor_host -- determine if hostname matches condor's hostname
    These can look like:
    [slotx@](xxx.xxx.xxx.xxx|host.name)
    returns True if matching, and false if not.
    """
    if hostname == None:
        return False
    # Strip off slotx@
    try:
        condor_hostname_parts = condor_hostname.split("@")
        condor_hostname_noslot = condor_hostname_parts[1]
    except:
        condor_hostname = condor_hostname
        condor_hostname_noslot = condor_hostname

    if hostname == condor_hostname_noslot:
        return True

    # Check if it's an IP address
    try:
        # If it's an IP address, and it doesn't match to this point,
        # it'll never match.
        socket.inet_aton(condor_hostname)
        return False
    except:
        pass
    # If it's a hostname, let's try to match the first bit of the
    # name, otherwise, it'll never match
    condor_hostname = condor_hostname_noslot.split(".")[0]
    hostname = hostname.split(".")[0]

    if hostname == condor_hostname:
        return True

    return False

class CircleQueue():
    """Represents a Circular queue of specified length."""
    def __init__(self, length=10):
        """Initializes the new circular queue.
        
        Keywords:
        length, default 10
        
        """
        self.data = [None for _ in range(0, length)]

    def append(self, x):
        """Append item to the end of the queue while removing the head."""
        self.data.pop(0)
        self.data.append(x)

    def get(self):
        """Returns the queue as a list."""
        return self.data

    def clear(self):
        """Resets the queue to empty state filled with None values."""
        self.data = [None for _ in range(0, len(self.data))]

    def min_use(self):
        """Checks the head of the queue for a None value to determine if is queue full."""
        min_use = True
        if self.data[0] == None:
            min_use = False
        return min_use

    def length(self):
        """Returns the number of non-None values to get the actual length of queue."""
        length = 0
        for x in self.data:
            if x != None:
                length += 1
        return length

class ErrTrackQueue(CircleQueue):
    """Error Tracking Queue - Keeps a True/False record of each VM Boot."""
    def __init__(self, name):
        """Initializes new queue with the configured length."""
        CircleQueue.__init__(self, config.ban_min_track)
        self.name = name

    def dist_true(self):
        """Calculate the distribution of True(succuessful) starts."""
        tc = 0
        for x in self.data:
            if x:
                tc += 1
        return (float(tc) / float(len(self.data))) if len(self.data) > 0 else 0

    def dist_false(self):
        """Calculate the distribution of False(failed) starts."""
        return 1.0 - self.dist_true()




