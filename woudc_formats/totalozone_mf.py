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
# Copyright (c) 2016 Government of Canada
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
import logging
import csv
from StringIO import StringIO

LOGGER = logging.getLogger(__name__)


class TotalOzone_MasterFile(object):

    def __init__(self):
        """
        Instantiate totalozone master file object
        """

    '''
    def sort(self, instance):
        """
        :parm instance: An StringIO object that contains information
        from totalozone.csv file
        :return Sortedlist: list of a list, contain data in the order
        that is needed
        :return title: header of columns, used to reference the index
        of specific data in list

        Pre-treatment of the data. Use csv.reader method to read
        data out from StringIO object, go through them one by one
        to check if key information is missing.
        Trigger csv_reader method if key information is missing.
        At the end, all metadata record will be sorted
        according to station type first, then station id and
        instance date time.

        """

        unSortedList = []
        SortedList = []
        csv_lib = []
        read = csv.reader(instance)

        try:
            title = read.next()
            LOGGER.info('Sorting started...')
            for row in read:
                if ''.join(row) == '':
                    continue
                if row[18] == '':
                    LOGGER.info(
                        'No date, alternate process start for: %s', row[5]
                    )
                    #if row[8] == '':
                    #    row[8] = row[5][63:66]
                    if row[5] not in csv_lib:
                    #    content = {}
                    #    new_row = ['']*30
                    #    try:
                    #        content =\
                    #            self.csv_reader(row[5])
                        csv_lib.append(row[5])
                    #    except Exception, err:
                    #        msg = 'Unable to fill in data for: %s' % row[5]
                    #        LOGGER.error(msg)
                    #    if content is None:
                    #        msg = 'Cannot process extCSV: %s' % row[5]
                    #        LOGGER.error(msg)
                    #        continue
                    #    number = 0
                    #    while number < len(content['date']):
                    #        if content['col_ozone'][number] == '':
                    #            number = number + 1
                    #            continue
                    #        new_row[18] = content['date'][number]
                    #        new_row[19] = content['wl_code'][number]
                    #        new_row[20] = content['obs_code'][number]
                    #        new_row[21] = content['col_ozone'][number]
                    #        new_row[23] = content['utc_begin'][number]
                    #        new_row[24] = content['utc_end'][number]
                    #        new_row[11] = content['instrument_name']
                    #        new_row[13] = content['instrument_number']
                    #        new_row[25] = content['utc_mean'][number]
                    #        new_row[26] = content['nObs'][number]
                    #        new_row[8] = row[8]
                    #        new_row[7] = 'STN'
                    #        unSortedList.append(new_row)
                    #        number = number + 1
                        continue
                    else:
                        LOGGER.info('URL had been already processed.')
                        continue
                unSortedList.append(row)
        except Exception, err:
            msg = 'Sorting of totalozone data failed due to: %s' % str(err)
            LOGGER.error(msg)
            raise err

        return unSortedList, title

    def csv_reader(self, url):
        """
        :parm url: url linked to ext-csv file that generated record
        with missing information
        :return content: dictionary that contains all the key data
        that are required to produce masterfile

        Grab data from ext-csv, and return them to sorting method,
        and those information will be inserted into the list with
        specific index that represents their meaning.
        """

        LOGGER.info('Getting extcsv at URL: %s...', url)
        try:
            extcsv = util.get_extcsv(url)
        except Exception, err:
            msg = 'Unable to get extcsv at URL: %s, due to: %s' %\
                (url, str(err))
            LOGGER.error(msg)
            return None
        if extcsv is None:
            return None

        try:
            daily_raw = extcsv.sections['DAILY']['_raw']
        except Exception, err:
            LOGGER.error(str(err))
            return None

        daily = StringIO(daily_raw)
        daily_rows = csv.reader(daily)
        instrument_name = util.get_extcsv_value(
            extcsv,
            'INSTRUMENT',
            'Name',
            payload=False
        )
        instrument_number = util.get_extcsv_value(
            extcsv,
            'INSTRUMENT',
            'Number',
            payload=False
        )
        header = daily_rows.next()
        column_idex = {}
        x = 0
        header_lower = []

        while x < len(header):
            header_lower.insert(x, header[x].lower())
            x = x + 1
        if header_lower[6] == 'utc':
            header_lower[6] = 'utc_end'
            header_lower[7] = 'utc_mean'
            header_lower[8] = 'nobs'
        column_idex = {
            'Date': header_lower.index('date'),
            'ObsCode': header_lower.index('obscode'),
            'ColumnO3': header_lower.index('columno3'),
            'WLCode': header_lower.index('wlcode'),
            'UTC_Begin': header_lower.index('utc_begin'),
            'UTC_End': header_lower.index('utc_end'),
            'UTC_Mean': header_lower.index('utc_mean'),
            'nObs': header_lower.index('nobs')
            }

        date = []
        obs_code = []
        col_ozone = []
        wl_code = []
        utc_begin = []
        utc_end = []
        utc_mean = []
        nObs = []

        # Collect data points from the daily table
        for row in daily_rows:
            date.append(str(row[column_idex['Date']]))
            # Get observation code
            try:
                obs_code_str = str(row[column_idex['ObsCode']])
                obs_code.append(obs_code_str)
            except (IndexError):
                obs_code.append('')

            try:
                wl_code_str = str(row[column_idex['WLCode']])
                wl_code.append(wl_code_str)
            except (IndexError):
                wl_code.append('')

            # Get column ozonesonde
            try:
                col_ozone_str = row[column_idex['ColumnO3']]
                col_ozone.append(str(int(round(float(col_ozone_str)))))
            except (IndexError, ValueError):
                col_ozone.append('')

            try:
                utc_begin_str = row[column_idex['UTC_Begin']]
                if not utc_begin_str == '':
                    utc_begin_int = str(int(round(float(utc_begin_str))))
                    utc_begin.append(utc_begin_int)
                else:
                    utc_begin.append(utc_begin_str)
            except (IndexError):
                utc_begin.append('')

            try:
                utc_end_str = row[column_idex['UTC_End']]
                if not utc_end_str == '':
                    utc_end_int = str(int(round(float(utc_end_str))))
                    utc_end.append(utc_end_int)
                else:
                    utc_end.append(utc_end_str)
            except (IndexError):
                utc_end.append('')

            try:
                utc_mean_str = row[column_idex['UTC_Mean']]
                if not utc_mean_str == '':
                    utc_mean_int = str(int(round(float(utc_end_str))))
                    utc_mean.append(utc_mean_int)
                else:
                    utc_mean.append(utc_mean_str)
            except (IndexError):
                utc_mean.append('')

            try:
                nObs_str = str(row[column_idex['nObs']])
                nObs.append(nObs_str)
            except (IndexError):
                nObs.append('')

        content = {'date': date, 'wl_code': wl_code, 'obs_code': obs_code,
                   'col_ozone': col_ozone, 'utc_begin': utc_begin,
                   'utc_end': utc_end, 'instrument_name': instrument_name,
                   'instrument_number': instrument_number,
                   'utc_mean': utc_mean, 'nObs': nObs}

        return content
    '''

    def execute(self, instance):
        """
        :parm data: Sortedlist from sorting method, contains all required data
        :parm header: data header, used to reference index
        :return s: A StringIO object that contains the entire masterfile
        that is ready to be written to a file

        This is the method that perform all the processings to generate
        masterfile. All the logics are in this method
        """
        with open(instance, 'r') as f:
            data = csv.reader(f)
            header = data.next()
            column_idex = {
                'Date': header.index('daily_date'),
                'ObsCode': header.index('daily_obscode'),
                'ColumnO3': header.index('daily_columno3'),
                'WLCode': header.index('daily_wlcode'),
                'UTC_Begin': header.index('daily_utc_begin'),
                'UTC_End': header.index('daily_utc_end'),
                'Instrument_Name': header.index('instrument_name'),
                'Instrument_Number': header.index('instrument_number'),
                'Platform_ID': header.index('platform_id'),
                'UTC_Mean': header.index('daily_utc_mean'),
                'nObs': header.index('daily_nobs')
            }

            s = StringIO()
            url = []
            for item in data:
                if item[8] == '':
                    url.append(item[5])
                    continue
                SSS = item[column_idex['Platform_ID']]
                if SSS == '':
                    LOGGER.error('No Platform ID, ' + item[5])
                    continue
                str_date = item[column_idex['Date']]

                YYYY = str_date[0:4]
                MM = str_date[5:7]
                DD = str_date[8:]
                HH = item[column_idex['UTC_Begin']]
                GG = item[column_idex['UTC_End']]
                if HH == '':
                    HH = '  '
                elif '-' in HH:
                    if -1.5 >= float(HH):
                        HH = '-0'
                    else:
                        HH = '00'
                elif '.' not in HH:
                    if len(HH) > 2:
                        HH = HH[0:2]
                    elif len(HH) == 1:
                        HH = '0' + HH
                else:
                    try:
                        HH = '%.0f' % round(float(HH), 0)
                    except Exception, err:
                        LOGGER.error(str(err) + HH + ', ' + item[5])
                        continue
                    if int(HH) in range(10):
                        HH = '0' + HH
                if GG == '':
                    GG = item[column_idex['nObs']].strip()
                    if len(GG) == 1:
                        GG = '0' + GG
                    elif GG == '':
                        GG = item[column_idex['UTC_Mean']].strip()
                        if len(GG) == 1:
                            GG = '0' + GG
                        elif GG == '':
                            GG = '  '
                    if len(GG) > 2:
                        GG = '  '
                elif '-' in GG:
                    if -1.5 >= float(GG):
                        GG = '-0'
                    else:
                        GG = '00'
                elif '.' not in GG:
                    if len(GG) > 2:
                        GG = GG[0:2]
                    elif len(GG) == 1:
                        GG = '0' + GG
                else:
                    try:
                        GG = '%.0f' % round(float(GG), 0)
                    except Exception, err:
                        LOGGER.error(str(err) + GG + ', ' + item[5])
                        continue
                    if int(GG) in range(10):
                        GG = '0' + GG
                L = item[column_idex['WLCode']]
                if L == '10':
                    L = '1'
                S = item[column_idex['ObsCode']]
                if S == "DS":
                    S = '0'
                elif S == "FM":
                    S = '1'
                elif S == "ZS":
                    S = '2'
                try:
                    int(S)
                except:
                    S = '9'
                XXX = item[column_idex['ColumnO3']]
                if XXX == '':
                    LOGGER.error('No O3 Value, ' + item[5])
                    continue
                try:
                    XXX = int(round(float(XXX)))
                except Exception, err:
                    LOGGER.error(str(err) + ', ' + item[5])
                if len(str(XXX)) < 3:
                    XXX = (3 - len(str(XXX))) * ' ' + str(XXX)
                EEE = '   '
                II = item[column_idex['Instrument_Name']].lower()
                if II == 'brewer':
                    II = ' 1'
                    if L == '':
                        L = 9
                elif II == 'dobson':
                    II = ' 3'
                    if L == '':
                        L = 0
                elif II == 'japanese dobson':
                    II = ' 4'
                    if L == '':
                        L = 0
                elif II == 'filter':
                    II = ' 9'
                    if L == '':
                        L = 8
                else:
                    II = '99'
                    if L == '':
                        L = ' '
                NNNN = item[column_idex['Instrument_Number']]
                '''if SSS == '001':
                    x = 0
                    while x == 0:
                        print NNNN'''
                if NNNN[0:1] == '0':
                    NNNN = NNNN[1:]
                if NNNN == '00':
                    NNNN = '0'
                if len(NNNN) < 4:
                    NNNN = (4 - len(NNNN)) * ' ' + NNNN
                if NNNN == 'UNKNOWN':
                    NNNN = '    '
                line = '%s%s%s%s%s%s%s%s%s%s%s%s' %\
                    (
                        str(SSS),
                        str(YYYY),
                        str(MM),
                        str(DD),
                        str(HH),
                        str(GG),
                        str(L),
                        str(S),
                        str(XXX),
                        str(EEE),
                        str(II),
                        str(NNNN)
                    )
                if len(line) != 29:
                    LOGGER.error('WrongLength: %s %s %s'
                                 % (str(len(line)), line, item[5]))
                    continue
                s.write(line + '\n')
        return s
