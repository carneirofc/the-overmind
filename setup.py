#!/usr/bin/env python3
from setuptools import setup, find_namespace_packages


with open('zerg/VERSION', 'r') as _f:
    __version__ = _f.read().strip()


with open('requirements.txt', 'r') as _f:
    _requirements = _f.read().strip().split('\n')

setup(
    name='zerg',
    version=__version__,
    author='Claudio F. Carneiro',
    description='Master/Slave data pipeline using Redis',
    url='https://github.com/carneirofc/the-overmind/',
    download_url='https://github.com/carneirofc/the-overmind',
    license='GNU GPLv2',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Communications'
    ],
    install_requires=_requirements,
    packages=find_namespace_packages(include=['zerg']),
    scripts=[
        'scripts/zerg-master-socket-stream.py',
        'scripts/zerg-slave-serial-stream.py',
    ],
    include_package_data=True,
    zip_safe=False
)
