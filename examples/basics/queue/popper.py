#!/usr/bin/python
import time
from dbmq import Database
db = Database()
while True:
  print db.pop('foo')
  time.sleep(1)