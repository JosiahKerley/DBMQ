#!/usr/bin/env python
import os
from setuptools import setup


## Main Setup
setup(
  name             = 'dbmq',
  packages         = ['dbmq'],
  version          = '1.0.0',
  description      = 'Redis-like database built on ZeroMQ',
  author           = 'Josiah Kerley',
  author_email     = 'josiahkerley@gmail.com',
  url              = 'https://github.com/JosiahKerley/dbmq',
  install_requires = ['pyzmq','pyYAML','prettytable'],
  zip_safe         = False,
  data_files=[
    ('/usr/bin', ['dbmq-cli']),
  ]
)

if os.path.isfile('/usr/bin/dbmq-cli'):
  os.system('chmod +x /usr/bin/dbmq-cli')