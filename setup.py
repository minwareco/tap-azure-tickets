#!/usr/bin/env python

from setuptools import setup, find_packages
import os

UTILS_VERSION = os.environ.get("UTILS_VERSION")

setup(name='tap-azure-tickets',
      version='0.1',
      description='Singer tap for Azure DevOps ticket data',
      author='minWare',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_azure_tickets'],
      install_requires=[
          'singer-python==6.1.0',
          'requests==2.20.0',
          'psutil==5.8.0',
          'minware_singer_utils@git+https://{}github.com/minwareco/minware-singer-utils.git@{}'.format(
              "{}@".format(os.environ.get("GITHUB_TOKEN")) if os.environ.get("GITHUB_TOKEN") else "",
              UTILS_VERSION
          )
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'nose',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-azure-tickets=tap_azure_tickets:main
      ''',
      packages=['tap_azure_tickets'],
      package_data = {
          'tap_azure_tickets': ['tap_azure_tickets/schemas/*.json']
      },
      include_package_data=True
)
