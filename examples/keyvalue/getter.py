#!/usr/bin/python
import time
from dbmq import Database
db = Database()
while True:
  print db.get('foo')
  time.sleep(1)