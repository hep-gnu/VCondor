#!/usr/bin/env python

import socket, SocketServer
import json
import os, sys
import time
import logging

logging.basicConfig(
#    filename = os.path.join(os.getcwd(), 'logJClient.log'),
    format = '%(asctime)s %(levelname)s:%(module)s:%(message)s',
    level = logging.DEBUG)
LOG = logging.getLogger(__name__)
#LOG.addHandler(logging.StreamHandler(sys.stderr))

def display(displaying):
    if len(displaying) >= 256:
        return '<data not shown>'
    else:
        return displaying

class PseudoRequestHandler(SocketServer.StreamRequestHandler):
    def __init__(self, request, **kwargs):
        self.request = request
        for i in kwargs.keys():
            setattr(self, i, kwargs[i])
        self.setup()
        # Now we have the StreamRH.rfile and wfile.
        # That's all what we need here, so no more handle() or finish() in the
        # __init__ method.

    close = SocketServer.StreamRequestHandler.finish

class JClient:
    received = True # dummy
    sending = ''
    data = ''
    
    def __init__(self, host='127.0.0.1', port=27015,
                 allow_reuse_address=False, **kwargs):
        self.host = host
        self.port = port
        self.addr = (self.host, self.port)
        self.allow_reuse_address = allow_reuse_address
        self.init_sock()
        self.init_handler()
        
    def init_sock(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.allow_reuse_address:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.connect(self.addr)
    
    def init_handler(self):
        self.handler = PseudoRequestHandler(self.sock)
    
    def close(self):
        #self.sock.send(os.linesep)
        self.handler.finish()
        self.sock.close()
    
    def JSONDecode(self, *args):
        # String (when received) to JSON
        # When not implemented:
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
        # When not implemented:
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
    
    def send(self, *args):
        if args:
            data = args[0]
        else:
            data = self.data
        self.sending = self.JSONEncode(data)
        if self.sending == '':
            # Don't send such a thing, otherwise the client's corresponding
            # handler will exit.
            self.sending = 'null'
            LOG.warn("Refusing to send blank string, sending 'null' instead")
        try:
            self.handler.wfile.write('%s%s' % (self.sending, os.linesep))
        except Exception, e:
            LOG.exception(e)
            raise e
    
    def recv(self):
        try:
            self.received = self.handler.rfile.readline()
            #self.sock.recv_into(self.received)
            self.received = self.received.strip()
        except Exception, e:
            LOG.exception(e)
            raise e
        if self.received != '':
            self.data = self.JSONDecode(self.received)
        else:
            self.data = ''
            LOG.info('Blank string received, exiting client')
    
    def oneRequest(self, *args):
        if args:
            data = args[0]
        else:
            data = self.data
        while True:
            try:
                self.send(data)
                self.recv()
            except Exception, e:
                LOG.exception(e)
                #raise e
                #self.handler.finish()
                #self.sock.close()
                t = 1
                n = 1
                while True:
                    try:
                        LOG.info("Retrying in %d seconds" % t)
                        time.sleep(t)
                        self.init_sock()
                        self.init_handler()
                    except Exception, e1:
                        LOG.exception(e1)
                    else:
                        break
                    t = 60 if t >= 30 else (t * 2)
                    if n == 15: # Please don't use >=
                        raise e
                    n += 1
            else:
                break
        return self.data
    
    # For testing only
    '''def nonstopRequests(self):
        from random import randint
        from time import sleep
        
        cont = True
        while cont:
            try:
                data = str(randint(0,99999999))
                #print "DATA: %s" % data
                try:
                    self.send(data)
                    self.recv()
                except Exception, e:
                    LOG.exception(e)
                    raise
                (foo, bar) = (randint(0,15), randint(0,13))
                #sleep_time = foo * (2 ** bar)
                sleep_time = 2
                #print "RAND: %d, %d Sleeping %d seconds" % (foo, bar, sleep_time)
                sleep(sleep_time)
            except KeyboardInterrupt, e:
                cont = False
'''
def main():
    jc = JClient(host='192.168.80.44', port=27015, bufsize=1024,
                 allow_reuse_address=True)
    t = 10
    try:
        while True:
            data_send = [1,2,(3,4)]
            data_recv = jc.oneRequest(data_send)
            LOG.info(str(data_recv))
            #print type(data_recv)
            LOG.info("Sleeping for %d seconds" % t)
            time.sleep(t)
    except (KeyboardInterrupt, SystemExit), e:
        # Normal exit
        jc.close()
    except Exception, e:
        LOG.exception(e)
        LOG.warn("Error captured in main")
    #finally:
    #    jc.close()

if __name__ == '__main__':
    main()
