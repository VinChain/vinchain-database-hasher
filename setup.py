#!/usr/bin/env python3

from setuptools import (
    find_packages,
    setup,
)

import codecs
try:
    codecs.lookup('mbcs')
except LookupError:
    ascii = codecs.lookup('ascii')
    codecs.register(lambda name, enc=ascii: {True: enc}.get(name == 'mbcs'))

VERSION = '1.0.0'

setup(
    name='vinchain-database-hasher',
    version=VERSION,
    description='Library for hashing databse vinchain.io',
    long_description=open('README.md').read(),
    author='Vinchain.io',
    author_email='info@vinchain.io',
    maintainer='Vinchain.io',
    maintainer_email='info@vinchain.io',
    url='http://vinchain.io',
    keywords=['vinchain.io', ],
    packages=find_packages(),
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Topic :: Office/Business :: Financial',
    ],
    install_requires=[
        "Django==2.2.13",
        "requests==2.18.4",
    ],
    include_package_data=True,
)
