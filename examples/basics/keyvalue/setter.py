#!/usr/bin/python
from dbmq import Database
db = Database()
print db.set('foo','bar')