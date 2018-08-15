# -*- coding: ISO-8859-15 -*-
# =================================================================
#
# Terms and Conditions of Use
#
# Unless otherwise noted, computer program source code of this
# distribution is covered under Crown Copyright, Government of
# Canada, and is distributed under the MIT License.
#
# The Canada wordmark and related graphics associated with this
# distribution are protected under trademark law and copyright law.
# No permission is granted to use them outside the parameters of
# the Government of Canada's corporate identity program. For
# more information, see
# http://www.tbs-sct.gc.ca/fip-pcim/index-eng.asp
#
# Copyright title to all 3rd party software distributed with this
# software is held by the respective copyright holders as noted in
# those files. Users are asked to read the 3rd Party Licenses
# referenced with those assets.
#
# Copyright (c) 2017 Government of Canada
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import os
from setuptools import setup, Command

with open('VERSION.txt') as ff:
    VERSION = ff.read().strip()

# set dependencies
with open('requirements.txt') as ff:
    INSTALL_REQUIRES = []
    DEPENDENCY_LINKS = []
    for line in ff:
        if 'git+' in line:
            DEPENDENCY_LINKS.append(line.strip())
        else:
            INSTALL_REQUIRES.append(line.strip())

KEYWORDS = [
    'Ozone',
    'O3',
    'Ultraviolet Radiation',
    'UV',
    'UV Index',
    'Dobson Units',
    'Archives',
    'World Data Centre',
    'WOUDC',
    'Non-standard Format'
    'SHADOZ',
    'AMES',
    'BAS',
    'Masterfile'
]

DESCRIPTION = '''
Python library for converting non-standard formats to
WOUDC extended CSV format.
'''

try:
    import pypandoc
    LONG_DESCRIPTION = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError, OSError):
    with open('README.md') as f:
        LONG_DESCRIPTION = f.read()

CONTACT = 'Meteorological Service of Canada, Environment Canada'

EMAIL = 'ec.woudc.ec@canada.ca'

URL = 'https://github.com/woudc/woudc-formats'


class PyTest(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import subprocess
        import sys
        errno = subprocess.call([sys.executable, 'tests/test.py'])
        raise SystemExit(errno)


# from https://wiki.python.org/moin/Distutils/Cookbook/AutoPackageDiscovery
def is_package(path):
    """decipher whether path is a Python package"""
    return (
        os.path.isdir(path) and
        os.path.isfile(os.path.join(path, '__init__.py'))
    )


def find_packages(path, base=''):
    """Find all packages in path"""
    packages = {}
    for item in os.listdir(path):
        directory = os.path.join(path, item)
        if is_package(directory):
            if base:
                module_name = "%(base)s.%(item)s" % vars()
            else:
                module_name = item
            packages[module_name] = directory
            packages.update(find_packages(directory, module_name))
    return packages


setup(
    name='woudc-formats',
    version=VERSION,
    description=DESCRIPTION.strip(),
    long_description=LONG_DESCRIPTION,
    license='MIT',
    platforms='all',
    keywords=' '.join(KEYWORDS),
    author=CONTACT,
    author_email=EMAIL,
    maintainer=CONTACT,
    maintainer_email=EMAIL,
    url=URL,
    install_requires=INSTALL_REQUIRES,
    dependency_links=DEPENDENCY_LINKS,
    packages=find_packages('.'),
    package_data={'woudc_formats': ['resource.cfg', 'PI_list.txt']},
    entry_points={
        'console_scripts': [
            'woudc-formats.py=woudc_formats:cli'
        ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Atmospheric Science'
    ],
    cmdclass={'test': PyTest},
    test_suite='tests.test'
)
