# -*- coding: utf-8 -*-
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
# Copyright (c) 2015 Government of Canada
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
"""Utility module to support fetching data from WOUDC WAF or WFS"""

import errno
import logging
import time
import zipfile
import requests
import ConfigParser
import os
import csv
from woudc_extcsv import loads
from StringIO import StringIO
from socket import error as SocketError
from urllib import quote
from urllib2 import urlopen
from urllib2 import URLError

LOGGER = logging.getLogger(__name__)

__DIRPATH = os.path.dirname(os.path.realpath(__file__))


def get_config_value(section, key, where='config_file'):
    if where == 'config_file':
        filepath = os.path.join(__DIRPATH, 'resource.cfg')
        config = ConfigParser.ConfigParser()
        config.read(filepath)
        return config.get(section, key)


def get_NDACC_agency(PI):
    with open(os.path.join(__DIRPATH, 'PI_list.txt')) as ff:
        for line in ff.readlines():
            if PI in line:
                return line.split(',')[1].strip()


def get_NDACC_station(station):
    with open(os.path.join(__DIRPATH, 'Stations_list.txt')) as ff:
        for line in ff.readlines():
            if station in line:
                return line.split(',')[1], line.split(',')[2]


def get_extcsv(url):
    """Get an Extended CSV from WOUDC WAF."""
    url = quote(url, "%/:=&?~#+!$,;'@()*[]|")
    try:
        content = urlopen(url).read()
        return loads(content)
    except (SocketError, URLError) as err:
        LOGGER.warn(str(err))
        if err.errno in (
                errno.ECONNRESET, errno.ECONNREFUSED, errno.ETIMEDOUT):
            time.sleep(5)
            LOGGER.info('Retrying...')
            return get_extcsv(url)
        else:
            return


def download_zip(path, filename):
    """Download zipfile"""
    response = requests.get(path)
    zip_file = zipfile.ZipFile(StringIO(response.content))
    s = StringIO()
    with zip_file.open(filename) as ff:
        s.write(ff.read())
    LOGGER.info('Zip file downloaded to path: %s ', path)
    return s


def extract_data(filename, path):
    zip_file = zipfile.ZipFile(filename, 'r')
    zip_file.extractall(path)
    zip_file.close()


def zip_file(output, outpath, zipfilename):
    try:
        compression = zipfile.ZIP_DEFLATED
    except:
        compression = zipfile.ZIP_STORED
    if not os.path.isdir(outpath):
        os.mkdir(outpath)
    zf = zipfile.ZipFile(outpath + zipfilename, mode='w')
    try:
        zf.writestr('totalozone.dat', output.getvalue(),
                    compress_type=compression)
    finally:
        zf.close()
    output.close()


def get_extcsv_value(extcsv, table, field, payload=False):
    """helper for getting value from extCSV object"""
    if payload is False:
        value = None
        if table in extcsv.sections.keys():
            if field in extcsv.sections[table].keys():
                try:
                    value = extcsv.sections[table][field]
                    return value
                except Exception as err:
                    msg = 'Unable to get value for table: %s,\
                    field: %s. Due to: %s.' % (table, field, str(err))
                    LOGGER.error(msg)
                    raise BPSExtCSVValueRetrievalError(msg)
        return value
    if payload:
        value = None
        if table in extcsv.sections.keys():
            body = StringIO(extcsv.sections[table]['_raw'])
            data_rows = csv.reader(body)
            fields = data_rows.next()
            value = []
            for row in data_rows:
                if field in fields:
                    try:
                        value.append(row[fields.index(field)])
                    except IndexError as err:
                        msg = 'Empty column for table: %s, field: %s.\
                        Putting in blank' % (table, field)
                        value.append('')
                    except Exception as err:
                        msg = 'Unable to get value for table: %s, field: %s.\
                        Due to: %s' % (table, field, str(err))
                        LOGGER.error(msg)
                        raise BPSExtCSVValueRetrievalError(msg)
        return value


def setup_logger(logfile, loglevel):
    """
    Setup logging mechanism

    :param logfile: path to log file
    :param loglevel: logging level
    """

    # regular logging format
    msg_format = '[%(asctime)s] [%(levelname)s] file=%(pathname)s \
    line=%(lineno)s module=%(module)s function=%(funcName)s [%(message)s]'

    # UAT logging format
    # msg_format = '[%(message)s]'

    datetime_format = '%a, %d %b %Y %H:%M:%S'

    loglevels = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'NOTSET': logging.NOTSET
    }

    logging.basicConfig(filename=logfile,
                        format=msg_format,
                        datefmt=datetime_format,
                        level=loglevels[loglevel])


def BPSExtCSVValueRetrievalError(Exception):
    """
    Unable to get value for given table field pair
    from ext CSV file
    """
    pass
