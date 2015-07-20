#!/usr/bin/env python

import json



class Logging:
  def err(self, message, fatal=False):
    pass
  def crit(self, message, fatal=True):
    pass
  def notice(self, message):
    pass





class Client:

  ## Imports
  import zmq
  import cPickle as pickle

  ## Globals
  socket = None
  linger = None
  url    = None

  def __init__(self,linger=-1,url='tcp://127.0.0.1:8562'):
    #self.getSocket()
    self.linger = linger
    self.url = url


  def clientSocket(self):
     context = self.zmq.Context.instance()
     self.socket = context.socket(self.zmq.REQ)
     self.socket.setsockopt(self.zmq.LINGER, self.linger)
     self.socket.connect(self.url)

  def serverSocket(self):
     context = self.zmq.Context.instance()
     self.socket = context.socket(self.zmq.REP)
     self.socket.setsockopt(self.zmq.LINGER, self.linger)
     self.socket.bind(self.url)

  def getSocket(self):
    self.clientSocket()

  def send(self, payload):
    payload_serialized = self.pickle.dumps(payload)
    try:
      self.socket.send(payload_serialized)
      return(True)
    except:
      try:
        self.getSocket()
        self.socket.send(payload_serialized)
        return(True)
      except:
        return(False)

  def recv(self):  ## Fix this: Check for bound sockets
    try:
      payload_serialized = self.socket.recv()
      payload = self.pickle.loads(payload_serialized)
      return(payload)
    except:
      #try:
      if 1:
        self.getSocket()
        payload_serialized = self.socket.recv()
        payload = self.pickle.loads(payload_serialized)
        return(payload)
      #except:
      #  return(False)








class Database:
  
  ## Imports
  import os
  import zmq
  import time
  import yaml
  import signal
  import threading


  ## Instances
  log = Logging()
  client = None


  ## Globals
  namespace    = None
  persist_file = 'dbmq.persist'
  persist_poll = 1
  serializer   = None
  url          = None
  threads      = []
  ismaster     = False
  running      = True


  ## Databases
  db = {}


  ## Constructor
  def __init__(self, daemonize=False, namespace='default', url='tcp://127.0.0.1:8562',linger=-1):
    self.persist_file = self.os.path.abspath(self.persist_file)
    self.serializer = self.yaml
    self.client = Client(linger=linger,url=url)
    self.namespace = namespace
    if daemonize:
      r = self.threading.Thread(target=self.daemon)
      r.start()
      self.threads.append({"name":"dameon","thread":r})


  ## Persist file handling
  def loadPersist(self):
    if self.os.path.isfile(self.persist_file):
      with open(self.persist_file, 'r') as f:
        try:   self.db = self.load(f.read())
        except: self.log.crit('Cannot load persist file, may be corrupt', fatal=True)
    else:
      self.log.err('Persist file does not exist')
  def dumpPersist(self):
    if not self.os.path.isfile(self.persist_file):
      self.log.notice('Persist file does not exist')
      if not self.os.path.isdir(self.os.path.dirname(self.persist_file)):
        self.log.notice("Persist file's directory does not exist, creating")
        self.os.makedirs(self.os.path.dirname(self.persist_file))
    with open(self.persist_file, 'w') as f:
      try:   f.write(self.dump(self.db))
      except: self.log.crit('Cannot save persist file')
  def handler_persist(self):
    while self.running:
      self.dumpPersist()
      self.time.sleep(self.persist_poll)


  ## Serializers
  def load(self, text):
    try:      return(self.serializer.loads(text))
    except:
      try:    return(self.serializer.load(text))
      except: return(False)
  def dump(self, object):
    try:      return(self.serializer.dumps(object))
    except:
      try:    return(self.serializer.dump(object))
      except: return(False)




  ##-> Data types <-##


  ## K/V's
  def get(self, key):
    message = {'type':'keyvalue', 'action':'get', 'key':key} 
    self.client.send(message)
    reply = self.client.recv()
    if reply: return(reply['value'])
    else: return(False)
  def raw_get(self, key):
    try: return(self.db[self.namespace]['keyvalue'][key]['value'])
    except: return(False)
  def getall(self):
    message = {'type':'keyvalue', 'action':'getall'} 
    self.client.send(message)
    return(self.client.recv())
  def raw_getall(self):
    keys = {}
    try:
      for i in self.db[self.namespace]['keyvalue']:
        key = i
        value = self.db[self.namespace]['keyvalue'][i]['value']
        keys[key] = value
    except:
      pass
    return({'data':keys})
  def set(self, key, value, expire=False):
    message = {'type':'keyvalue', 'action':'set', 'key':key, 'value':value, 'expire':expire} 
    self.client.send(message)
    return(self.client.recv())
  def raw_set(self, key, value, expire=False):
    try:    self.db[self.namespace]  ## Ugly...
    except: self.db[self.namespace] = {}  ## Ugly...
    try:    self.db[self.namespace]['keyvalue']  ## Ugly...
    except: self.db[self.namespace]['keyvalue'] = {}  ## Ugly...
    try:    self.db[self.namespace]['keyvalue'][key]  ## Ugly...
    except: self.db[self.namespace]['keyvalue'][key] = {}  ## Ugly...
    try:
      self.db[self.namespace]['keyvalue'][key]['value'] = value
      self.db[self.namespace]['keyvalue'][key]['updated'] = self.time.time()
      self.db[self.namespace]['keyvalue'][key]['expire'] = expire
    except:
      return(False)
  def unset(self, key):
    message = {'type':'keyvalue', 'action':'unset', 'key':key} 
    self.client.send(message)
    return(self.client.recv())
  def raw_unset(self, key):
    try:
      del self.db[self.namespace]['keyvalue'][key]
      return(True)
    except:
      return(False)


  ## Queues
  def push(self, queue, payload):
    message = {'type':'queue', 'action':'push', 'queue':queue, 'payload':payload}
    self.client.send(message)
    return(True)
  def raw_push(self, queue, payload):
    try:    self.db[self.namespace]  ## Ugly...
    except: self.db[self.namespace] = {}  ## Ugly...
    try:    self.db[self.namespace]['queue']  ## Ugly...
    except: self.db[self.namespace]['queue'] = {}  ## Ugly...
    try:    self.db[self.namespace]['queue'][queue]  ## Ugly...
    except: self.db[self.namespace]['queue'][queue] = []  ## Ugly...
    self.db[self.namespace]['queue'][queue].append(payload)
    return(True)
  def pop(self, queue):
    message = {'type':'queue', 'action':'pop', 'queue':queue} 
    if self.client.send(message):
      retVal = self.client.recv()['payload']
      return(retVal)
  def raw_pop(self, queue):
    try:
      retVal = self.db[self.namespace]['queue'][queue].pop()
      return(retVal)
    except:
      return(False)
  def index(self, queue):
    message = {'type':'queue', 'action':'index', 'queue':queue} 
    if self.client.send(message):
      print 'top'
      retVal = self.client.recv()
      print 'bottom'
      print retVal
      print '!!!'
      return(retVal)
  def raw_index(self, queue):
    #try:
    if 1:
      retVal = self.db[self.namespace]['queue'][queue]['payload']
      print '!!!!!!!'
      print retVal
      print '!!!!!!!'
      return(retVal)
    #except:
    #  return(False)
  def drop(self, queue):
    message = {'type':'queue', 'action':'drop', 'queue':queue} 
    if self.client.send(message):
      retVal = self.client.recv()
      return(retVal)
  def raw_drop(self, queue):
    try:
      del self.db[self.namespace]['queue'][queue]
      return(True)
    except:
      return(False)


  ## Documents
  def create(self, doc, content={}):
    message = {'type':'doc', 'action':'create', 'doc':doc, 'content':content}
    if self.client.send(message):
      return(self.client.recv())
    else:
      return(False)
  def raw_create(self, doc, content={}):
    try:    self.db[self.namespace]  ## Ugly...
    except: self.db[self.namespace] = {}  ## Ugly...
    try:    self.db[self.namespace]['doc']  ## Ugly...
    except: self.db[self.namespace]['doc'] = {}  ## Ugly...
    try:    self.db[self.namespace]['doc'][doc]  ## Ugly...
    except: self.db[self.namespace]['doc'][doc] = {}  ## Ugly...
    self.db[self.namespace]['doc'][doc] = content
    print content
    return(True)
  def read(self, doc):
    message = {'type':'doc', 'action':'read', 'doc':doc}
    if self.client.send(message):
      return(self.client.recv()['content'])
    else:
      return(False)
  def raw_read(self, doc):
    return(self.db[self.namespace]['doc'][doc])
  def update(self, doc, content={}):
    message = {'type':'doc', 'action':'update', 'doc':doc, 'content':content}
    self.client.send(message)
    return(True)
  def raw_update(self, doc, content={}):
    #try:
    if 1:
      current = self.db[self.namespace]['doc'][doc]
      print current
      print content
      self.db[self.namespace]['doc'][doc] = dict( current.items() + content.items() )
      return(True)
    #except:
    #  return(False)
  def delete(self, doc):
    message = {'type':'doc', 'action':'delete', 'doc':doc}
    self.client.send(message)
    return(True)
  def raw_delete(self, doc):
    try:
      del self.db[self.namespace]['doc'][doc]
      return(True)
    except:
      return(False)


  ## Handler thread
  def handler_request(self):
    try:
      server = Client()
      server.getSocket = server.serverSocket
      unknown_type = {'status':False, 'message':'Action Unknown'}
      while self.running:
        self.ismaster = True
        message = server.recv()
        response = {'status':True}
        if not message == False:
          if message['type'] == 'keyvalue':
            if message['action'] == 'get':
              retVal = {'key':message['key'], 'value':self.raw_get(message['key'])}
            elif message['action'] == 'getall':
              retVal = self.raw_getall()
            elif message['action'] == 'set':
              retVal = {'key':message['key'], 'value':self.raw_set(message['key'], message['value'], message['expire'])}
            elif message['action'] == 'unset':
              retVal = {'key':message['key'], 'value':self.raw_unset(message['key'])}
            else:
              retVal = unknown_type
          elif message['type'] == 'doc':
            if message['action'] == 'create':
              self.raw_create(message['doc'],message['content'])
              retVal = {'doc':message['doc'],'content':message['content']}
            elif message['action'] == 'read':
              retVal = {'doc':message['doc'],'content':self.raw_read(message['doc'])}
            elif message['action'] == 'update':
              self.raw_update(message['doc'],message['content'])
              retVal = {'doc':message['doc'],'content':message['content']}
            elif message['action'] == 'delete':
              self.raw_delete(message['doc'])
              retVal = {'doc':message['doc']}
          elif message['type'] == 'queue':
            if message['action'] == 'push':
              retVal = {'queue':message['queue'], 'payload':self.raw_push(message['queue'], message['payload'])}
            elif message['action'] == 'pop':
              retVal = {'queue':message['queue'], 'payload':self.raw_pop(message['queue'])}
            elif message['action'] == 'index':
              retVal = {'queue':message['queue'], 'payload':self.raw_index(message['queue'])}
            elif message['action'] == 'drop':
              retVal = {'queue':message['queue'], 'payload':self.raw_drop(message['queue'])}
          elif message['type'] == 'control':
            pass
          else:
            retVal = {'status':False, 'message':'Type Unknown'}
          response = dict(response.items() + retVal.items())
          server.send(response)
        else:
          self.log.err('Message returned error.')
    except:
      self.ismaster = False


  ## Daemon
  def daemon(self):
    m = False
    while self.running:
      if self.ismaster:
        if not m: self.loadPersist()
        m = True
        self.dumpPersist()
      else:
        m = False
        r = self.threading.Thread(target=self.handler_request)
        r.isDaemon()
        r.start()
        self.threads.append({"name":"handler","thread":r})
      self.time.sleep(1)


  ## Stop
  def stop(self):
    self.ismaster = False
    self.running  = False
    self.time.sleep(1)
    self.os.kill(self.os.getpid(), self.signal.SIGQUIT)









class CLI:


  ## Imports
  import sys
  import yaml
  from prettytable import PrettyTable


  ## Instances
  db = Database(daemonize=False)


  ## Globals
  namespace = 'default'
  pretty = 'yaml'


  ## Constructor
  def __init__(self, namespace='default'):
    self.namespace = namespace


  ## Show table of data
  def show(self, type, extra=None):
    if type == 'keyval':
      data = self.db.getall()['data']
      if self.pretty == 'table':
        t = self.PrettyTable(['Key', 'Value'])
        for i in data:
          key = i
          value = data[i]
          t.add_row([key, value])
        return(t)
      elif self.pretty == 'yaml':
        return(self.yaml.dump(data, default_flow_style=False, default_style=''))
    elif type == 'queue':
      data = self.db.index(extra)
      if self.pretty == 'table':
        t = self.PrettyTable(['Key', 'Value'])
        for i in data:
          key = i
          value = data[i]
          t.add_row([key, value])
        return(t)
      elif self.pretty == 'yaml':
        return(self.yaml.dump(data, default_flow_style=False, default_style=''))


  ## Shell method
  def shell(self):
    running = True
    print('DatabaseMQ Shell')
    while running:
      input = raw_input('> ')
      if input.lower() == 'quit' or input.lower() == 'exit':
        running = False
      elif input.lower().startswith('sh'):
        if input.lower().split(' ')[1] == 'keyval' or input.lower().split(' ')[1] == 'kv':
          print('\n' + self.show('keyval'))
        elif input.lower().split(' ')[1] == 'queue' or input.lower().split(' ')[1] == 'q':
          print('\n' + self.show('queue', input.lower().split(' ')[2]))
        else:
          print('Unknown type')
      elif input.lower().startswith('set'):
        key = input.split(' ')[1]
        value = input.split(' ')[2]
        print(self.db.set(key, value))
      elif input.lower().startswith('uns'):
        key = input.split(' ')[1]
        print(self.db.unset(key))
      elif input.lower().startswith('get'):
        key = input.split(' ')[1]
        print(self.db.get(key))
      elif input.lower().startswith('pu'):
        queue = input.split(' ')[1]
        payload = input.split(' ')[2]
        print(self.db.push(queue, payload))
      elif input.lower().startswith('po'):
        queue = input.split(' ')[1]
        print(self.db.pop(queue))
      elif input.lower().startswith('in'):
        queue = input.split(' ')[1]
        print('')
        for i in self.db.index(queue)['payload']:
          print(i)
        print('')
      elif input.lower().startswith('dr'):
        queue = input.split(' ')[1]
        print(self.db.drop(queue))
      elif input.lower().startswith('cr'):
        doc = input.split(' ')[1]
        print('\nCreate YAML Doc:\n')
        buffer = ''
        while True:
          line = raw_input()
          if not line: break
          buffer += line+'\n'
        content = self.yaml.load(buffer)
        print self.db.create(doc,content)
      elif input.lower().startswith('re'):
        doc = input.split(' ')[1]
        print('')
        print(self.yaml.dump(self.db.read(doc), default_flow_style=False, default_style=''))
      elif input.lower().startswith('up'):
        doc = input.split(' ')[1]
        print('\nUpdate YAML Doc:\n')
        buffer = ''
        while True:
          line = raw_input()
          if not line: break
          buffer += line+'\n'
        content = self.yaml.load(buffer)
        print self.db.update(doc,content)
      elif input.lower().startswith('del'):
        doc = input.split(' ')[1]
        print(self.yaml.dump(self.db.delete(doc)))
    self.sys.exit(0)









