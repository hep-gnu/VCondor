
#!/user/bin/env python
# vim:set expandtab ts=4 sw=4:

# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 22/3/2016

import socket, SocketServer
import json
import os
import sys
import time
import logging
import utilities
import config
#import Exception



class PseudoRequestHandler(SocketServer.StreamRequestHandler):
    def __init__(self, request, **kwargs):
        self.request = request
        for i in kwargs.keys():
            setattr(self, i, kwargs[i])
        self.setup()
        # Now we have the StreamRH.rfile and wfile.
        # That's all what we need here, so no more handle() or finish() in the
        # __init__ method

    close = SocketServer.StreamRequestHandler.finish

class JClient:
    received = True
    sending = ''
    data = ''

    def __init__(self, host='127.0.0.1', port=27020, allow_reuse_address=False, **kargs):
        logging.basicConfig(filename=config.log_file,format='%(asctime)s  [%(levelname)s] : %(module)s - %(message)s',level=logging.INFO)
        self.log = utilities.get_vrmanager_logger()
        self.host = host
        print 'host is %s' % host
        self.port = port
        self.addr = (self.host, self.port)
        self.allow_reuse_address = allow_reuse_address
        print "init socket"
        try:
            self.init_sock()
            self.init_handler()
        except Exception,e:
            print e
            self.log.error(e)

    def init_sock(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print "init socket a"
        if self.allow_reuse_address:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSE_ADDR, 1)
        try:
            self.sock.connect(self.addr)
        except Exception,e:
            print e
            self.log.error(e)
            print "catch exception 1"
            raise e

    def init_handler(self):
        self.handler = PseudoRequestHandler(self.sock)

    def close(self):
        #self.sock.send(os.linesep)
        self.handler.finish()
        self.sock.close()

    def JSONDecode(self, *args):
        # String (when received ) to JSON.
        if args:
            data = args[0]
        else:
            data = self.received
        try:
            ret = json.loads(data)
        except Exception, e:
            self.log.error(e)
            ret = ()
        finally:
            return ret

    def JSONEncode(self, *args):
        # JSON to sting (then send the string).
        if args:
            data = args[0]
        else:
            data = self.data
        try:
            ret = json.dumps(data)
        except Exception, e:
            self.log.error(e)
            ret = ''
        finally:
            return ret

    def JSONFormatCheck(self, *args):
        # JSON format check before send or after received.
        if args:
            try:
                JSONData = args[0]
                FormatKeysList = args[1]
                FormatTypeDict = args[2]
                ret = 1
                for key in FormatKeysList: # Check if JSONData has all necessary keys.
                    if key not in JSONData.keys():
                        ret = 0
                        print "No key %s in JSON %s !" % (key, JSONData)
                        return 0

                for key in FormatTypeDict.keys():
                    if (str(type(JSONData[key])).split('\'')[1] != FormatTypeDict[key]):
                        ret = 0
                        print "Wrong type for %s in JSON %s ; Type %s is the right type!"  \
                                % (str(type(JSONData[key])), JSONData, FormatTypeDict[key])
                        return 0
                return ret
            except Exception, e:
                print "JSONData is %s ." % args[0]
                print e
                return 0
        else:
            return 0
        


    def send(self, *args):
        # Send a string (when provided by formal parameter)to server.
        if args:
            data = args[0]
        else:
            data = self.data
        self.sending = self.JSONEncode(data)
        if self.sending == '':
            # Don't send such a thing, otherwise the client's corresonding handler will exit.
            self.sending = 'null'
            self.log.warning("Refusing to send blank string, sending 'null' instead")
            #raise Exception.SendNullException('I have sent a null string to vr.')
        try:
            self.handler.wfile.write('%s%s' % (self.sending, os.linesep))
        except Exception, e:
            self.log.error(e)
            #raise e

    def recv(self):
        # Receive a json by socket response and decode it to string.
        try:
            self.received = self.handler.rfile.readline()
            self.received = self.received.strip()
            #print "1122 %s" (repr(self.received))
        except Exception, e:
            self.log.error(e)
            #raise e
        if self.received != '':
            self.data = self.JSONDecode(self.received)
        else:
            self.data = ''
            self.log.info('Blank string received, exiting client')

    def oneRequest(self, *args):
        # Complete trace of encode a string into json and send it by socket.
        # Then receive a json and decode it into string
        if args:
            data = args[0]
        else:
            data = self.data
        while True:
            try:
                self.send(data)
                self.recv()
                break
            except Exception, e:
                self.log.error(e)
		print e
        return self.data

        
def main():
    jc = JClient(host='127.0.0.1', port=27020, bufsize=1024, allow_reuse_addr=True)

if __name__ == '__main__':
    main()    





