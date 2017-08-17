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
from datetime import datetime
import os
import re
import time
import util
from StringIO import StringIO
from util import WOUDCextCSVReader

LOGGER = logging.getLogger(__name__)


class Old_TotalOzone_MasterFile(object):

    def __init__(self):
        pass

    def update_totalOzone_master_file(self, dir, master_file, date, mode, heading):  # noqa
        """Updates Total Ozone Master File"""
        # Initialization
        write_output = 1
        current_time = (datetime.now()).strftime("%Y_%m_%d")
        log_file = open('totalOzone_processing_log_%s' % current_time, 'wb')  # noqa
        data_file = None
        global tmp_filename
        tmp_filename = master_file
        if mode == 'overwrite':
            data_file = open(tmp_filename, 'wb')
        else:
            data_file = open(tmp_filename, 'ab')
        if heading == 'on':
            data_file.write('Platform_ID,Year,Month,Day,Start_Hour,Finish_Hour,Wavelength_Pair,Observation_Type,Total_Column_Ozone_Amount,Ozone_Std_Error,Instrument_Type,Instrument_Number\r\n')  # noqa

        # external ftp file
        global output_file
        output_file = 'Summaries/TotalOzone/Daily_Summary/o3tot.zip'  # noqa

        # traverse the given directory
        for dirname, dirnames, filenames in os.walk(dir):
            dirnames.sort()
            filenames.sort()
            for filename in filenames:
                try:
                    # print filename
                    file_last_modified_date = time.strftime ("%Y-%m-%d",time.localtime(os.path.getmtime(os.path.join(dirname, filename))))  # noqa
                    # date comparison
                    if date is not None and file_last_modified_date <= date:  # noqa
                        log_file.write('PROCESSED#%s        last modified date: %s\r\n' % ((os.path.join(dirname, filename)), file_last_modified_date))  # noqa
                        extCSV = WOUDCextCSVReader (os.path.join(dirname, filename))  # noqa
                    if date is not None and file_last_modified_date > date:
                        continue
                    if date is None:
                        log_file.write('PROCESSED#%s        last modified date: %s\r\n' % ((os.path.join(dirname, filename)), file_last_modified_date))  # noqa
                        extCSV = WOUDCextCSVReader (os.path.join(dirname, filename))  # noqa

                    # store data into variables
                    platform_id = '   '
                    if 'PLATFORM' in extCSV.sections:
                        p_id = extCSV.sections['PLATFORM']['ID']
                        if p_id is not None and len(p_id) != 0:
                            platform_id = p_id
                            if len(platform_id) == 1:
                                platform_id = '00%s' % platform_id
                            if len(platform_id) == 2:
                                platform_id = '0%s' % platform_id
                    else:
                        log_file.write('ERROR#E01:Could not find PLATFORM in input file: %s. Data is ignored\r\n' % os.path.join(dirname, filename))  # noqa

                    inst_type_id = '  '
                    inst_number = '   0'
                    if 'INSTRUMENT' in extCSV.sections:
                        inst_name = extCSV.sections['INSTRUMENT']['Name']
                        inst_model = extCSV.sections['INSTRUMENT']['Model']
                        if inst_name is not None and len(inst_name) != 0 and inst_name:  # noqa
                            try:
                                inst_type_id  = util.get_config_value('Instrument Type ID', inst_name)  # noqa
                                if inst_model == 'Japanese':
                                    inst_type_id  = util.get_config_value('Instrument Type ID', inst_model+' '+inst_name)  # noqa
                                if len(inst_type_id) == 1:
                                    inst_type_id = ' %s' % inst_type_id
                            except Exception, err:
                                log_file.write('ERROR#E02:There is no instrumet type id for \'%s\' in file %s. Data is ignored\r\n' % (inst_name,os.path.join(dirname, filename)))  # noqa
                                write_output = 0
                                pass
                        i_num = extCSV.sections['INSTRUMENT']['Number']
                        if i_num is not None and len(i_num) != 0:
                            inst_number = i_num
                            inst_number = re.sub("^0{1,2}", "", inst_number)  # noqa
                            if len(inst_number) == 1:
                                inst_number = '   %s' % inst_number
                            if len(inst_number) == 2:
                                inst_number = '  %s' % inst_number
                            if len(inst_number) == 3:
                                inst_number = ' %s' % inst_number
                            if i_num == 'na':
                                inst_number = '   0'
                    else:
                        log_file.write('ERROR#E03:Could not find INSTRUMENT in input file: %s. Data is ignored\r\n' % os.path.join(dirname, filename))  # noqa

                    if 'DAILY' in extCSV.sections:
                        data = StringIO((extCSV.sections['DAILY']['_raw']).strip())  # noqa
                        if data is not None:
                            try:
                                data_rows = csv.reader(data)
                                data_rows.next()
                            except StopIteration:
                                log_file.write('ERROR#E04:Error reading DAILY block in file %s. Data is ignored\r\n' % os.path.join(dirname, filename))  # noqa
                                write_output = 0
                                pass
                            for row in data_rows:
                                year = '    '
                                month = '  '
                                day = '  '
                                UTC_Begin = '  '
                                UTC_End = '  '
                                WLCode = ' '
                                ObsCode = ' '
                                ozone_std_error = '   '
                                ColumnO3 = '   '
                                UTC_Mean = '  '
                                nObs = ' '
                                if len(row) > 1 and "*" not in row[0]:
                                    if len(row[0]) != 0:
                                        year = row[0].split('-')[0]
                                        month = row[0].split('-')[1]
                                        day = row[0].split('-')[2]
                                    if len(row) >= 2:
                                        if len(row[1]) != 0:
                                            WLCode = row[1]
                                            if len(WLCode) > 1:
                                                try:
                                                    WLCode  = util.get_config_value('WLCode', WLCode)  # noqa
                                                except Exception, err:
                                                    log_file.write('ERROR#E05:There is no one character WLCode code for \'%s\' in file %s. Data is ignored\r\n' % (WLCode,os.path.join(dirname, filename)))  # noqa
                                                    write_output = 0
                                                    pass
                                        else:
                                            if inst_name == 'Dobson':
                                                WLCode = util.get_config_value('WLCode', 'Dobson')  # noqa
                                            if inst_name == 'Brewer':
                                                WLCode = util.get_config_value('WLCode', 'Brewer')  # noqa
                                            if inst_name == 'Filter':
                                                WLCode = util.get_config_value('WLCode', 'Filter')  # noqa
                                            if inst_name == 'Microtops':
                                                WLCode = util.get_config_value('WLCode', 'Microtops')  # noqa
                                    if len(row) >= 3:
                                        if len(row[2]) != 0:
                                            ObsCode = row[2]
                                            if util.is_number(ObsCode) == False and len(ObsCode) != 1:  # noqa
                                                try:
                                                    ObsCode  = util.get_config_value('Obs Code', ObsCode)  # noqa
                                                except Exception, err:
                                                    log_file.write('ERROR#E06:There is no obs code for \'%s\' in file %s. Data is ignored\r\n' % (ObsCode,os.path.join(dirname, filename)))  # noqa
                                                    write_output = 0
                                                    pass
                                        else:
                                            ObsCode = '9'
                                    if len(row) >= 4:
                                        if len(row[3]) != 0 and row[3] != '0.0' and row[3] != '0' and not "-" in row[3]:  # noqa
                                            try:
                                                ColumnO3= '%.0f' % round (float(re.findall("[0-9]*.[0-9]*", row[3])[0]), 0)  # noqa
                                                if ColumnO3 == '0':
                                                    write_output = 0
                                            except Exception, err:
                                                log_file.write('ERROR#E07:Could not round ColumnO3 value of: %s in file %s. Data ignored.\r\n' % (ColumnO3,os.path.join(dirname, filename)))  # noqa
                                                write_output = 0
                                            if len(ColumnO3) == 1:
                                                ColumnO3 = '  %s' % ColumnO3  # noqa
                                            if len(ColumnO3) == 2:
                                                ColumnO3 = ' %s' % ColumnO3  # noqa
                                        else:
                                            write_output = 0
                                    if len(row) >= 6:
                                        if len(row[5]) != 0:
                                            UTC_Begin = row[5]
                                            if len(re.findall("[0-9]*", UTC_Begin)[0]) > 2:  # noqa
                                                UTC_Begin = UTC_Begin[:2]
                                            elif "-" in UTC_Begin:
                                                if -1.5 >= float(UTC_Begin):  # noqa
                                                    UTC_Begin = '-0'
                                                else:
                                                    UTC_Begin = '00'
                                            else:
                                                try:
                                                    UTC_Begin =  '%.0f' % round (float(UTC_Begin), 0)  # noqa
                                                except Exception, err:
                                                    log_file.write('ERROR#E08:Could not round UTC_Begin value of: %s in file %s. Data ignored.\r\n' % (UTC_Begin,os.path.join(dirname, filename)))  # noqa
                                                    write_output = 0
                                                if int(UTC_Begin) in range(10):  # noqa
                                                    UTC_Begin = '0%s' % UTC_Begin  # noqa
                                    if len(row) >= 7:
                                        if len(row[6]) != 0:
                                            UTC_End = row[6]
                                            if len(re.findall("[0-9]*", UTC_End)[0]) > 2:  # noqa
                                                UTC_End = UTC_End[:2]
                                            elif "-" in UTC_End:
                                                if -1.5 >= float(UTC_End):
                                                    UTC_End = '-0'
                                                else:
                                                    UTC_End = '00'
                                            else:
                                                try:
                                                    UTC_End =  '%.0f' % round (float(UTC_End), 0)  # noqa
                                                except Exception, err:
                                                    log_file.write('ERROR#E09:Could not round UTC_End value of: %s in file %s. Data ignored.\r\n' % (UTC_End,os.path.join(dirname, filename)))  # noqa
                                                    write_output = 0
                                                if int(UTC_End) in range(10):  # noqa
                                                    UTC_End = '0%s' % UTC_End  # noqa
                                    if len(row) >= 8:
                                        if len(row[7]) != 0:
                                            UTC_Mean = row[7]
                                            if len(row[6]) == 0:
                                                UTC_End = UTC_Mean
                                                if "-" in UTC_End:
                                                    if float(UTC_End) <= -1.5 and float(UTC_End) > -2:  # noqa
                                                        UTC_End = '-0'
                                                    elif float(UTC_End) <= -1 and float(UTC_End) > -2:  # noqa
                                                        UTC_End = '00'
                                                    elif float(UTC_End) >= -1:  # noqa
                                                        UTC_End = '00'
                                                    elif float(UTC_End) <= -2 and float(UTC_End) >= -10:  # noqa
                                                        UTC_End = '-0'
                                                    elif float(UTC_End) < -10 and float(UTC_End) >= -10.5:  # noqa
                                                        UTC_End = '-0'
                                                    else:
                                                        UTC_End = '-1'
                                                else:
                                                    try:
                                                        UTC_End =  '%.0f' % round (float(UTC_End), 0)  # noqa
                                                    except Exception, err:
                                                        log_file.write('ERROR#E09:Could not round UTC_End value of: %s in file %s. Data ignored.\r\n' % (UTC_End,os.path.join(dirname, filename)))  # noqa
                                                        write_output = 0
                                                    if int(UTC_End) in range(10):  # noqa
                                                        UTC_End = '0%s' % UTC_End  # noqa
                                    if len(row) >= 9:
                                        if len(row[8]) != 0 and row[8] != '-':  # noqa
                                            nObs = row[8]
                                            if len(nObs) > 2 and not "-" in nObs:  # noqa
                                                nObs = nObs[:2]
                                            if len(row[6]) == 0 and len(row[7]) == 0 and nObs != '00' and row[8] != '0' and row[8] != '-1' and row[8] != '-2' and row[8] != '-3':  # noqa
                                                UTC_End = nObs
                                                if int(UTC_End) < -3 and int(UTC_End) >= -10:  # noqa
                                                    UTC_End = '-0'
                                                if int(UTC_End) == -11:
                                                    UTC_End = '-1'
                                                if len(UTC_End) == 1:
                                                    UTC_End = '0%s' % UTC_End  # noqa

                                    # build output string and
                                    # write/append to file
                                    if heading == 'off' or heading is None:
                                        output_line='%s%s%s%s%s%s%s%s%s%s%s%s' % (platform_id,year,month,day,UTC_Begin,UTC_End,WLCode,ObsCode,ColumnO3,ozone_std_error,inst_type_id,inst_number)  # noqa
                                    else:
                                        output_line_header='%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (platform_id,year,month,day,UTC_Begin,UTC_End,WLCode,ObsCode,ColumnO3,ozone_std_error,inst_type_id,inst_number)  # noqa
                                    if write_output == 1 and ColumnO3 != '   ' and (heading == 'off' or heading is None):  # noqa
                                        if len(output_line) == 29:
                                            data_file.write('%s\r\n' % output_line)  # noqa
                                        else:
                                            if len(output_line) > 29:
                                                log_file.write('ERROR#E10:This output line: \'%s\' exceeds 29 characters from file %s. Data is ignored.\r\n' % (output_line,os.path.join(dirname, filename)))  # noqa
                                            else:
                                                log_file.write('ERROR#E11:This output line: \'%s\' is less than 29 characters from file %s. Data is ignored.\r\n' % (output_line,os.path.join(dirname, filename)))  # noqa
                                    if heading == 'on':
                                        data_file.write('%s\r\n' % output_line_header)  # noqa

                                    write_output = 1
                    else:
                        log_file.write('ERROR#E12:Could not find DAILY in input file: %s. Data is ignored\r\n' % os.path.join(dirname, filename))  # noqa
                except Exception, err:
                    print str(err)
                    log_file.write('ERROR: Unable to process file: %s \n' % os.path.join(dirname, filename))  # noqa
        # data file close
        data_file.close()

        # zip data file
        util.zip(os.path.abspath(str(master_file)), "o3tot.zip")
        tmp_filename = "o3tot.zip"

        # log file close
        log_file.close()
        print 'log file is located here: %s' % os.path.abspath('totalOzone_processing_log_%s' % current_time)  # noqa


'''
class TotalOzone_MasterFile(object):

    def __init__(self):
        """
        Instantiate totalozone master file object
        """

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
                # if SSS == '001':
                #     x = 0
                #     while x == 0:
                #         print NNNN
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
'''
