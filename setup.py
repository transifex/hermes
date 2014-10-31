# -*- coding: utf-8 -*-
import re
import os

from setuptools import setup, find_packages


def get_requirements(filename):
    """
    Read requirements and dependency links from a file passed by parameter
    and return them as two lists in a tuple.
    """

    def add_dependency_link(line):
        link = re.sub(r'\s*-[ef]\s+', '', line)
        filename = os.path.basename(link.split('://')[1])
        url = link.split(filename)[0]
        if url not in dependency_links:
            dependency_links.append(url)

    requirements = []
    dependency_links = []
    for line in open(filename, 'r').read().split('\n'):
        if re.match(r'(\s*#)|(\s*$)', line):
            continue
        if re.match(r'\s*-e\s+', line):
            # TODO support version numbers
            requirements.append(re.sub(r'\s*-e\s+.*#egg=(.*)$', r'\1', line))
            add_dependency_link(line)
        elif re.match(r'\s*-f\s+', line):
            add_dependency_link(line)
        else:
            requirements.append(line)
    return requirements, dependency_links


requirements, dependency_links = get_requirements(
    'requirements/base.txt'
)

setup(
    name="hermes",
    version='0.0.1',
    description="Event-driven Postgres client library",
    author="Transifex",
    author_email="admin@transifex.com",
    url="https://www.transifex.com",
    install_requires=requirements,
    dependency_links=dependency_links,
    test_suite="tests",
    data_files=[],
    zip_safe=False,
    packages=find_packages(),
)
