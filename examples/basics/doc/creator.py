#!/usr/bin/python
from dbmq import Database
db = Database()

document = {
              "foo":"bar",
              "hello":"world"
            }

db.create('foo',document)