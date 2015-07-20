#!/usr/bin/python
import json
from dbmq import Database
db = Database()
db.delete('foo')