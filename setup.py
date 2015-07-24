# -*- coding: utf-8 -*-
from os.path import join, dirname

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

with open(join(dirname(__file__), 'README')) as _file:
    long_desc = _file.read().strip()

setup(
    name="hermes-pg",
    version='0.3.1',
    description="Event-driven Postgres client library",
    long_description=long_desc,
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
    ),
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Software Development :: Libraries :: Application Frameworks"
    ]
)
