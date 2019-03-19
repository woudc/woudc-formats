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
# Copyright (c) 2019 Government of Canada
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

import csv
from datetime import datetime
import logging
import os
import re
import shutil
from StringIO import StringIO
import tempfile
import time
import zipfile

import util

LOGGER = logging.getLogger(__name__)


class TotalOzone_MasterFile(object):

    def __init__(self):
        pass

    def update_totalOzone_master_file(self, directory, master_file, date, mode, heading):  # noqa
        """Updates Total Ozone Master File"""
        # Initialization
        write_output = 1
        current_time = (datetime.now()).strftime("%Y_%m_%d")
        log_file = open('totalOzone_processing_log_%s' % current_time, 'wb')  # noqa
        data_file = None
        global tmp_filename
        tmp_filename = os.path.join(master_file, 'o3tot.dat')
        if mode == 'overwrite':
            data_file = open(tmp_filename, 'wb+')
        else:
            data_file = open(tmp_filename, 'ab+')
        if heading == 'on':
            data_file.write('Platform_ID,Year,Month,Day,Start_Hour,Finish_Hour,Wavelength_Pair,Observation_Type,Total_Column_Ozone_Amount,Ozone_Std_Error,Instrument_Type,Instrument_Number\r\n')  # noqa

        # external ftp file
        global output_file
        output_file = 'Summaries/TotalOzone/Daily_Summary/o3tot.zip'  # noqa

        # extract zipfile
        zip_flag = False
        path = directory
        if zipfile.is_zipfile(directory):
            zip_flag = True
            tmpdir = tempfile.mkdtemp()
            z = zipfile.ZipFile(directory)
            z.extractall(path=tmpdir)
            path = tmpdir

        # traverse the given directory
        for dirname, dirnames, filenames in os.walk(path):
            dirnames.sort()
            filenames.sort()
            for filename in filenames:
                try:
                    # print filename
                    file_last_modified_date = time.strftime ("%Y-%m-%d",time.localtime(os.path.getmtime(os.path.join(dirname, filename))))  # noqa
                    # date comparison
                    if date is not None and file_last_modified_date <= date:  # noqa
                        log_file.write('PROCESSED#%s        last modified date: %s\r\n' % ((os.path.join(dirname, filename)), file_last_modified_date))  # noqa
                        extCSV = util.WOUDCextCSVReader (os.path.join(dirname, filename))  # noqa
                    if date is not None and file_last_modified_date > date:
                        continue
                    if date is None:
                        log_file.write('PROCESSED#%s        last modified date: %s\r\n' % ((os.path.join(dirname, filename)), file_last_modified_date))  # noqa
                        extCSV = util.WOUDCextCSVReader (os.path.join(dirname, filename))  # noqa

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
                            except Exception as err:
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
                            if i_num.lower() == 'na':
                                inst_number = '   0'
                        inst_number = str(int(inst_number)).rjust(4)
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
                                                except Exception as err:
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
                                                except Exception as err:
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
                                            except Exception as err:
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
                                                except Exception as err:
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
                                                except Exception as err:
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
                                                    except Exception as err:
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
                except Exception as err:
                    print(err)
                    log_file.write('ERROR: Unable to process file: {}\n'.format(os.path.join(dirname, filename)))  # noqa
        # data file close
        data_file.close()

        # zip data file
        out_zip = zipfile.ZipFile(os.path.join(master_file, 'o3tot.zip'), 'w',
                                  zipfile.ZIP_DEFLATED)
        out_zip.write(tmp_filename, 'o3tot.dat')
        out_zip.close()

        os.remove(tmp_filename)

        if zip_flag:
            shutil.rmtree(tmpdir)

        # log file close
        log_file.close()
        print('log file is located here: {}'.format(os.path.abspath('totalOzone_processing_log_{}'.format(current_time))))  # noqa
