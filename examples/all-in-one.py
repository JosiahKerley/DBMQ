#!/usr/bin/python


## Imports
import time
import threading
from dbmq import Database


## Setup
daemon = Database(daemonize=True,linger=100)
db     = Database(linger=100)


## Test for running workers
def workersAlive(workers):
  still_alive = False
  for i in workers:
    if i.isAlive():
      still_alive = True
  return(still_alive)


## Worker thread
def worker(id):
  while running:
    print('Worker %s is waiting for work'%(str(id)))
    work = db.pop('work')
    if work:
      print('Worker %s got %s'%(id,work))
      while not db.push('done',work):
        print 'Cannot complete my work'
    else:
      print('Worker %s cannot find work'%(id))
    time.sleep(1)


## Assign work
for i in range(0,99):
  print('Creating work %s'%(str(i)))
  db.push('work',i)
print('All work enqueued\n\n')
time.sleep(3)


## Start the workers
running = True
workers = []
for id in range(0,1):
  print('Starting worker %s'%(str(id)))
  t = threading.Thread(target=worker,args=(id,))
  t.start()
  workers.append(t)
  time.sleep(0.5)
time.sleep(5)


## Finish
for i in range(0,1000):
  print db.index('work')
  time.sleep(0.001)
while db.index('work') and workersAlive(workers):
  print db.index('work')
  time.sleep(1)
running = False
while True:
  stillWorking = False
  if not workersAlive(workers):
    break
time.sleep(3)
print('\n\n')
daemon.stop()