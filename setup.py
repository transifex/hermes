# -*- coding: utf-8 -*-
import re
import os

from setuptools import setup, find_packages


install_requires = [
    'psycopg2==2.5.4',
    'watchdog==0.8.1'
]

tests_require = [
    'nose',
    'mock',
    'coverage',
    'pyaml',
    'nosexcover'
]

setup(
    name="hermes",
    version='0.0.2',
    description="Event-driven Postgres client library",
    author="Liam Costello",
    author_email="liam@transifex.com",
    url="https://github.com/transifex/hermes",
    install_requires=install_requires,
    tests_require=tests_require,
    dependency_links=tests_require,
    test_suite="test_hermes.run_tests.run_all",
    packages=find_packages(
        where='.',
        exclude=('test_hermes*', )
    )
)
