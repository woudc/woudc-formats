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
from time import strptime
import operator
import zipfile
import requests
import ConfigParser
import os
import tarfile
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
    except Exception:
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


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


# convert full month to number
def month_to_number(month):
    return strptime(month, '%B').tm_mon


# convert number of month
def number_to_month(number):
    month_map = {1: 'January', 2: 'February', 3: 'March', 4: 'April',
                 5: 'May', 6: 'June', 7: 'July', 8: 'August',
                 9: 'September', 10: 'October', 11: 'November', 12: 'December'}
    return month_map.get(int(number.lstrip('0')))


# sort a table by multiple columns
def sort_table(table, cols):
    """ sort a table by multiple columns
        table: a list of lists (or tuple of tuples) where each inner list
               represents a row
        cols:  a list (or tuple) specifying the column numbers to sort by
               e.g. (1,0) would sort by column 1, then by column 0
    """
    for col in reversed(cols):
        table = sorted(table, key=operator.itemgetter(col))
    return table


def zip(in_filename, out_filename):
    try:
        compression = zipfile.ZIP_DEFLATED
    except Exception:
        compression = zipfile.ZIP_STORED

    zf = zipfile.ZipFile(out_filename, mode='w')
    try:
        zf.write(in_filename, compress_type=compression)
    finally:
        zf.close()


# unzip tar, tar.gz, etc
def unzip(filename, dir_path=None):
    if dir_path is not None:
        if not os.path.isdir(os.path.join(dir_path, filename[0:filename.index('.')])):  # noqa
            os.mkdir(os.path.join(dir_path, filename[0:filename.index('.')]))
    try:
        tar = tarfile.open(os.path.join(dir_path, filename))
        for item in tar:
            tar.extract(item, os.path.join(dir_path, filename[0:filename.index('.')]))  # noqa
    except Exception:
        print 'ERROR: Unable to unzip %s!' % filename
    return


# returns date in YYYY-MM-DD format given YYYY-M-D
def date_YYYYMMDD(date):
    token = date.split('-')
    YYYY = token[0]
    MM = token[1]
    DD = token[2]
    if len(MM) == 1:
        MM = '0%s' % MM
    if len(DD) == 1:
        DD = '0%s' % DD
    return '%s-%s-%s' % (YYYY, MM, DD)


# returns total size of directory and number of files
def get_dir_stat(dir_path, ignore=None):
    dir_size = 0
    num_of_files = 0
    for dirname, dirnames, filenames in os.walk(dir_path):
        if ignore is not None and ignore in dirnames:
            dirnames = dirnames.remove(ignore)
        for filename in filenames:
            dir_size += os.path.getsize(os.path.join(dirname, filename))
            num_of_files += 1
    return [int(dir_size), num_of_files]


# print WOUDC extCSV standard format of TotalOzone data
def print_extCSV(extCSVObj, filepath):
    filename = '%s.%s.%s.%s.%s.csv' % (extCSVObj.timestamp['date'].replace('-','')[:6]+'01', extCSVObj.instrument['name'], extCSVObj.instrument['model'], extCSVObj.instrument['number'], extCSVObj.data_generation['agency'])  # noqa
    out_file = open(os.path.join(filepath, filename), 'w')

    # file content
    out_file.write('*\n')
    out_file.write('* This file was generated using data send via email from agency\n')  # noqa
    out_file.write('* \'na\' is used where a model or instrument number is not available\n')  # noqa
    out_file.write('*\n')
    out_file.write('\n')

    out_file.write('#CONTENT\n')
    out_file.write('Class,Category,Level,Form\n')
    out_file.write('%s,%s,%s,%s\n' % (extCSVObj.content['class'], extCSVObj.content['category'], extCSVObj.content['level'], extCSVObj.content['form']))  # noqa
    out_file.write('\n')

    out_file.write('#DATA_GENERATION\n')
    out_file.write('Date,Agency,Version,ScientificAuthority\n')
    out_file.write('%s,%s,%s,%s\n' % (extCSVObj.data_generation['date'], extCSVObj.data_generation['agency'], extCSVObj.data_generation['version'], extCSVObj.data_generation['sci_auth']))  # noqa
    out_file.write('\n')

    out_file.write('#PLATFORM\n')
    out_file.write('Type,ID,Name,Country,GAW_ID\n')
    out_file.write('%s,%s,%s,%s,%s\n' % (extCSVObj.platform['type'], extCSVObj.platform['id'], extCSVObj.platform['name'], extCSVObj.platform['country'], extCSVObj.platform['gaw_id']))  # noqa
    out_file.write('\n')

    out_file.write('#INSTRUMENT\n')
    out_file.write('Name,Model,Number\n')
    out_file.write('%s,%s,%s\n' % (extCSVObj.instrument['name'], extCSVObj.instrument['model'], extCSVObj.instrument['number']))  # noqa
    out_file.write('\n')

    out_file.write('#LOCATION\n')
    out_file.write('Latitude,Longitude,Height\n')
    out_file.write('%s,%s,%s\n' % (extCSVObj.location['latitude'], extCSVObj.location['longitude'], extCSVObj.location['height']))  # noqa
    out_file.write('\n')

    out_file.write('#TIMESTAMP\n')
    out_file.write('UTCOffset,Date,Time\n')
    out_file.write('%s,%s,%s\n' % (extCSVObj.timestamp['utcoffset'], extCSVObj.timestamp['date'], extCSVObj.timestamp['time']))  # noqa
    out_file.write('\n')

    out_file.write('#DAILY\n')
    out_file.write('Date,WLCode,ObsCode,ColumnO3,StdDevO3,UTC_Begin,UTC_End,UTC_Mean,nObs,mMu,ColumnSO2\n')  # noqa
    for item in extCSVObj.payload:
        out_file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (item.date, item.wlcode, item.obscode, item.columnO3, item.stdDevO3, item.utcBegin, item.utcEnd, item.utcMean, item.nObs, item.mMu, item.columnSO2))  # noqa


# print any extended CSV format
def print_csx(where, csx, dirname=None, host=None):
    # print to file
    if where == 'file' and dirname is not None:
        try:
            out_file = open(os.path.join(dirname, csx.get_filename()), 'w')

            # write comments
            comments = csx.get_comments()
            for comment in comments:
                if comment != '\n':
                    if '\n' not in comment:
                        out_file.write('* %s\n' % comment)
                    else:
                        out_file.write('* %s' % comment)
                else:
                    out_file.write('%s' % comment)

            out_file.write('\n')
            # write tables
            tables = csx.get_tables()
            for table in tables:
                out_file.write('#%s\n' % table.get_name())
                out_file.write('%s\n' % table.get_header())
                for row in table.get_data():
                    out_file.write('%s\n' % ','.join(map(str, row)))
                for comment in table.get_comments():
                    # print comment
                    if '*' in comment:
                        out_file.write('%s' % comment)
                    else:
                        out_file.write('* %s' % comment)
                out_file.write('\n')
        except Exception:
            print 'ERROR: Unable to create output extended CSV file %s' % (os.path.join(dirname, csx.get_filename()))  # noqa

        # close file
        out_file.close()

    # write file to ftp folder
    if where == 'ftp' and dirname is not None and host is not None:
        try:
            ftp_file = host.file(host.path.join(dirname, csx.get_filename()), 'w')  # noqa

            # write comments
            comments = csx.get_comments()
            for comment in comments:
                if comment != '\n':
                    if '\n' not in comment:
                        ftp_file.write('* %s\n' % comment)
                    else:
                        ftp_file.write('* %s' % comment)
                else:
                    ftp_file.write('%s' % comment)

            ftp_file.write('\n')
            # write tables
            tables = csx.get_tables()
            for table in tables:
                ftp_file.write('#%s\n' % table.get_name())
                ftp_file.write('%s\n' % table.get_header())
                for row in table.get_data():
                    ftp_file.write('%s\n' % ','.join(map(str, row)))
                for comment in table.get_comments():
                    # print comment
                    if '*' in comment:
                        ftp_file.write('%s' % comment)
                    else:
                        ftp_file.write('* %s' % comment)
                ftp_file.write('\n')
        except Exception:
            print 'ERROR: Unable to write file %s to WOUDC FTP.' % (host.path.join(dirname, csx.get_filename()))  # noqa

        # close file
        ftp_file.close()


# helper function, return a data table for CSX
def new_table(name, header):
    table = CSX.Table()
    table.set_name(name)
    table.set_header(header)
    return table


# return average
def average(data_list):
    return float(sum(data_list)) / len(data_list)


class WOUDCextCSVReader(object):
    def __init__(self, filepath):
        """
        Read WOUDC extCSV file and objectify
        """
        self.sections = {}
        f = open(filepath)
        blocks = f.read().split('#')
        # get rid of first element of cruft
        blocks.pop(0)
        for b in blocks:
            s = StringIO(b.strip())
            c = csv.reader(s)
            header = c.next()[0]
            if header != 'DAILY':  # metadata
                self.sections[header] = {}
                self.sections[header]['_raw'] = b.strip()
                try:
                    fields = c.next()
                    values = c.next()
                except StopIteration:
                    pass
                i = 0
                for field in fields:
                    try:
                        self.sections[header][field] = values[i]
                        i += 1
                    except Exception:
                        self.sections[header][field] = None
                        # print 'ERROR: corrupt format.  Section: %s.  Skipping' % header  # noqa
            else:  # payload
                buf = StringIO(None)
                w = csv.writer(buf)
                for row in c:
                    w.writerow(row)
                if header not in self.sections:
                    self.sections[header] = {'_raw': buf.getvalue()}
                else:
                    self.sections[header] = {'_raw': self.sections[header]['_raw'] + buf.getvalue()[80:]}  # noqa


class CSX (object):

    def __init__(self):
        self.tables = []
        self.file_name = None
        self.comment = []

    def set_filename(self, name):
        self.file_name = name

    def add_comment(self, comment):
        self.comment.append(comment)

    def add_table_to_file(self, table):
        self.tables.append(table)

    def get_filename(self):
        return self.file_name

    def view_comments(self):
        for comment in self.comment:
            print comment

    def get_comments(self):
        return self.comment

    def view_tables(self):
        for table in self.tables:
            print table.get_name()
            print table.get_header()
            print table.data_to_string()

    def get_tables(self):
        return self.tables

    class Table(object):

        def __init__(self):
            self.name = None
            self.header = None
            self.comment = []
            self.data = []
            self.order = None

        def set_name(self, name):
            self.name = name

        def set_header(self, header):
            self.header = header

        def set_order(self, order):
            self.order = order

        def add_comment(self, comment):
            self.comment.append(comment)

        def store_data(self, data):
            self.data.append(data)

        def get_name(self):
            return self.name

        def get_header(self):
            return self.header

        def get_order(self):
            return self.order

        def get_data(self):
            return self.data

        def get_comments(self):
            return self.comment

        def view_data(self):
            for data in self.data:
                print data
