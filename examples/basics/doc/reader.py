#!/usr/bin/python
import json
from dbmq import Database
db = Database()
print(json.dumps(db.read('foo'),indent=2)