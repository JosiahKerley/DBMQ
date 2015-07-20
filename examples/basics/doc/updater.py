#!/usr/bin/python
from dbmq import Database
db = Database()

document = {
              "foo":"foobar"
            }

db.update('foo',document)