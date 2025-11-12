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
from io import StringIO
import tempfile
import time
import zipfile

from woudc_formats import util

LOGGER = logging.getLogger(__name__)


class TotalOzone_MasterFile(object):

    def __init__(self):
        pass

    def update_totalOzone_master_file(self, directory, master_file, date, mode, heading):  # noqa
        """Updates Total Ozone Master File"""
        # Initialization
        write_output = 1
        current_time = (datetime.now()).strftime("%Y_%m_%d")
        log_file = open('totalOzone_processing_log_%s' % current_time, 'w')  # noqa
        data_file = None
        global tmp_filename
        tmp_filename = os.path.join(master_file, 'o3tot.dat')
        if mode == 'overwrite':
            data_file = open(tmp_filename, 'w+')
        else:
            data_file = open(tmp_filename, 'a+')
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
                filepath = os.path.join(dirname, filename)
                num_errors = 0
                try:
                    # print filename
                    file_last_modified_date = time.strftime ("%Y-%m-%d",time.localtime(os.path.getmtime(filepath)))  # noqa
                    # date comparison
                    if date is not None and file_last_modified_date <= date:  # noqa
                        log_file.write(f'PROCESSING: {filepath}        last modified date: {file_last_modified_date}\r\n')  # noqa
                        extCSV = util.WOUDCextCSVReader(filepath)  # noqa
                    if date is not None and file_last_modified_date > date:
                        continue
                    if date is None:
                        log_file.write(f'PROCESSING: {filepath}        last modified date: {file_last_modified_date}\r\n')  # noqa
                        extCSV = util.WOUDCextCSVReader(filepath)  # noqa

                    # store data into variables
                    platform_id = '   '
                    if 'PLATFORM' in extCSV.sections:
                        p_id = extCSV.sections['PLATFORM']['ID']
                        if p_id is not None and len(p_id) != 0:
                            platform_id = p_id.zfill(3)  # Zero-pad to 3 characters # noqa
                    else:
                        log_file.write(f'ERROR#E01:Could not find PLATFORM in input file: {filepath}. Data ignored\r\n')  # noqa
                        num_errors += 1

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
                                log_file.write('ERROR#E02: There is no instrument type id for \'%s\' in file %s. Data ignored\r\n' % (inst_name,filepath))  # noqa
                                num_errors += 1
                                LOGGER.warning('ERROR E02: Invalid instrument type caused by {}'.format(err))  # noqa
                                # write_output = 0
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
                        log_file.write('ERROR#E03:Could not find INSTRUMENT in input file: %s. Data ignored\r\n' % filepath)  # noqa
                        num_errors += 1

                    if 'DAILY' in extCSV.sections:
                        data = StringIO((extCSV.sections['DAILY']['_raw']).strip())  # noqa
                        num_daily_rows_written = 0
                        if data is not None:
                            try:
                                data_rows = csv.reader(data)
                                header_row = next(data_rows)
                                expected_columns = len(header_row)

                                # Create a mapping of column names to indices for robustness
                                col_map = {col.strip(): idx for idx, col in enumerate(header_row)}
                            except StopIteration:
                                log_file.write(f"ERROR#E04:Error reading DAILY block in file {filepath}. Data omitted\r\n")  # noqa
                                num_errors += 1
                                write_output = 0
                                pass
                            for row in data_rows:
                                # Skip empty rows (double spacing issue)
                                if not row or all(cell == '' for cell in row):
                                    continue

                                # Normalize row length to match header
                                row = self.normalize_csv_row(row, expected_columns)

                                # Initialize expected variables
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

                                # Column map to each index
                                date_idx = col_map.get('Date', 0)
                                wlcode_idx = col_map.get('WLCode', 1)
                                obscode_idx = col_map.get('ObsCode', 2)
                                columno3_idx = col_map.get('ColumnO3', 3)
                                utc_begin_idx = col_map.get('UTC_Begin', 5)
                                utc_end_idx = col_map.get('UTC_End', 6)
                                utc_mean_idx = col_map.get('UTC_Mean', 7)
                                nobs_idx = col_map.get('nObs', 8)

                                # Check for rows with only date (like "2020-01-01" with no data)
                                if len(row[0]) != 0 and "*" not in row[0]:
                                    try:
                                        year = row[date_idx].split('-')[0]
                                        month = row[date_idx].split('-')[1].zfill(2)
                                        day = row[date_idx].split('-')[2].zfill(2)
                                    except (IndexError, ValueError):
                                        log_file.write(
                                            f"ERROR#E12:Invalid date format: {row[date_idx]} in row {row}. "
                                            "Data ignored\r\n"
                                        )
                                        num_errors += 1
                                        continue

                                    # Check if this is a data-less row (only has date)
                                    has_data = any(row[i].strip() for i in range(1, len(row)))
                                    if not has_data:
                                        log_file.write(
                                            f"WARNING:Row with date {row[date_idx]} has no data. Data ignored\r\n"
                                        )
                                        continue
                                else:
                                    # Skip rows without valid dates
                                    continue

                                # WLCode processing
                                if wlcode_idx < len(row) and len(row[wlcode_idx]) != 0:  # noqa
                                    WLCode = row[wlcode_idx]
                                    # Strip leading zeros if WLCode is numeric and more than one character long
                                    if len(WLCode) > 1 and WLCode.isdigit():
                                        WLCode = str(int(WLCode))  # Strip leading zeros

                                    if len(WLCode) > 1:
                                        try:
                                            WLCode = util.get_config_value('WLCode', WLCode)
                                        except Exception as err:
                                            log_file.write(f"ERROR#E05:There is no one-character WLCode code for '{WLCode}' in row {row}. Data ignored\r\n")
                                            num_errors += 1
                                            LOGGER.error('E05: Invalid WLCode caused by {}'.format(err))
                                            WLCode = ' '  # set as empty instead of omitting row
                                            # write_output = 0
                                else:
                                    # Default WLCode based on instrument
                                    if inst_name == 'Dobson':
                                        WLCode = util.get_config_value(
                                            'WLCode', 'Dobson')
                                    elif inst_name == 'Brewer':
                                        WLCode = util.get_config_value(
                                            'WLCode', 'Brewer')
                                    elif inst_name == 'Filter':
                                        WLCode = util.get_config_value(
                                            'WLCode', 'Filter')
                                    elif inst_name == 'Microtops':
                                        WLCode = util.get_config_value(
                                            'WLCode', 'Microtops')

                                # ObsCode processing
                                if obscode_idx < len(row) and len(row[obscode_idx]) != 0:  # noqa
                                    ObsCode = row[obscode_idx]
                                    if not ObsCode.isdigit() or len(ObsCode) != 1:  # noqa
                                        try:
                                            ObsCode = util.get_config_value('Obs Code', ObsCode)
                                        except Exception as err:
                                            log_file.write(f"ERROR#E06:There is no obs code for \'{ObsCode}\' in row {row}. Data ignored\r\n")
                                            num_errors += 1
                                            LOGGER.error('E06: Missing observation code caused by {}'.format(err))
                                            ObsCode = ' '  # set as empty instead of omitting row
                                            # write_output = 0
                                else:
                                    ObsCode = '9'

                                # ColumnO3 processing
                                if columno3_idx < len(row) and len(row[columno3_idx]) != 0 and row[columno3_idx] != '0.0' and row[columno3_idx] != '0' and "-" not in row[columno3_idx]:  # noqa
                                    try:
                                        ColumnO3 = '%.0f' % round(float(re.findall("[0-9]*.[0-9]*", row[columno3_idx])[0]), 0)
                                        if ColumnO3 == '0':
                                            log_file.write(f"ERROR#E07:ColumnO3 value is {row[columno3_idx]}. Data row omitted\r\n")  # noqa
                                            num_errors += 1
                                            write_output = 0
                                    except Exception as err:
                                        log_file.write(f"ERROR#E07:Could not round ColumnO3 value of: {row[columno3_idx]} in row {row}. Data row omitted\r\n")  # noqa
                                        num_errors += 1
                                        LOGGER.error('E07: Invalid ColumnO3 value. {}'.format(err))
                                        write_output = 0
                                    if len(ColumnO3) == 1:
                                        ColumnO3 = '  %s' % ColumnO3
                                    elif len(ColumnO3) == 2:
                                        ColumnO3 = ' %s' % ColumnO3
                                    # invalid if DU over 1000
                                    elif (int(ColumnO3) >= 1000):
                                        log_file.write(f"ERROR#E07:ColumnO3 value is questionably large: \'{ColumnO3}\' in row {row}. Data row omitted\r\n")
                                        num_errors += 1
                                        write_output = 0
                                else:
                                    LOGGER.debug(f"ColumnO3 value of {row[columno3_idx]} is invalid (empty, 0 or negative). Data ignored.")  # noqa
                                    write_output = 0

                                # UTC_Begin processing
                                if utc_begin_idx < len(row) and len(row[utc_begin_idx]) != 0:  # noqa
                                    UTC_Begin = row[utc_begin_idx]
                                    # rare case match if in HH:MM:SS format
                                    hhmmss_pattern = re.match(r'^(\d{2}):(\d{2}):(\d{2})$', UTC_Begin)
                                    if hhmmss_pattern:
                                        # take just the hour part
                                        UTC_Begin = hhmmss_pattern.group(1)
                                    elif len(re.findall("[0-9]*", UTC_Begin)[0]) > 2:  # noqa
                                        UTC_Begin = UTC_Begin[:2]
                                    elif "-" in UTC_Begin:
                                        if -1.5 >= float(UTC_Begin):
                                            UTC_Begin = '-0'
                                        else:
                                            UTC_Begin = '00'
                                    else:
                                        try:
                                            UTC_Begin = '%.0f' % round(float(UTC_Begin), 0)
                                            if int(UTC_Begin) in range(10):
                                                UTC_Begin = '0%s' % UTC_Begin
                                        except Exception as err:
                                            log_file.write(f"ERROR#E08:Could not round UTC_Begin value of: {UTC_Begin} in row {row}. Data row omitted\r\n")
                                            num_errors += 1
                                            LOGGER.error('E08: Invalid UTC_Begin value. {}'.format(err))
                                            write_output = 0

                                # UTC_End processing
                                if utc_end_idx < len(row) and len(row[utc_end_idx]) != 0:  # noqa
                                    UTC_End = row[utc_end_idx]
                                    # rare case match if in HH:MM:SS format
                                    hhmmss_pattern = re.match(r'^(\d{2}):(\d{2}):(\d{2})$', UTC_End)
                                    if hhmmss_pattern:
                                        # take just the hour part
                                        UTC_End = hhmmss_pattern.group(1)
                                    if len(re.findall("[0-9]*", UTC_End)[0]) > 2:  # noqa
                                        UTC_End = UTC_End[:2]
                                    elif "-" in UTC_End:
                                        if -1.5 >= float(UTC_End):
                                            UTC_End = '-0'
                                        else:
                                            UTC_End = '00'
                                    else:
                                        try:
                                            UTC_End = '%.0f' % round(float(UTC_End), 0)
                                            if int(UTC_End) in range(10):
                                                UTC_End = '0%s' % UTC_End
                                        except Exception as err:
                                            log_file.write(f"ERROR#E09:Could not round UTC_End value of: {UTC_End} in row {row}. Data row omitted\r\n")
                                            num_errors += 1
                                            LOGGER.error('E09: Invalid UTC_End value. {}'.format(err))
                                            write_output = 0

                                # UTC_Mean processing (and fallback for UTC_End)
                                if utc_mean_idx < len(row) and len(row[utc_mean_idx]) != 0:  # noqa
                                    UTC_Mean = row[utc_mean_idx]
                                    # rare case match if in HH:MM:SS format
                                    hhmmss_pattern = re.match(r'^(\d{2}):(\d{2}):(\d{2})$', UTC_Mean)
                                    if hhmmss_pattern:
                                        # take just the hour part
                                        UTC_End = hhmmss_pattern.group(1)
                                    elif utc_end_idx >= len(row) or len(row[utc_end_idx]) == 0:  # noqa
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
                                                if int(UTC_End) in range(10):  # noqa
                                                    UTC_End = '0%s' % UTC_End  # noqa
                                            except Exception as err:
                                                log_file.write(f"ERROR#E09:Could not round UTC_End value of: {UTC_End} in row {row}. Data row omitted\r\n")  # noqa
                                                num_errors += 1
                                                LOGGER.error('E09: Invalid UTC_End value. {}'.format(err))  # noqa
                                                write_output = 0

                                # nObs processing
                                if nobs_idx < len(row) and len(row[nobs_idx]) != 0 and row[nobs_idx] != '-':
                                    nObs = row[nobs_idx]
                                    if len(nObs) > 2 and "-" not in nObs:
                                        nObs = nObs[:2]
                                    if len(row[6]) == 0 and len(row[7]) == 0 and nObs != '00' and row[8] != '0' and row[8] != '-1' and row[8] != '-2' and row[8] != '-3':  # noqa
                                        UTC_End = nObs
                                        if int(UTC_End) < -3 and int(UTC_End) >= -10:  # noqa
                                            UTC_End = '-0'
                                        if int(UTC_End) == -11:
                                            UTC_End = '-1'
                                        if len(UTC_End) == 1:
                                            UTC_End = '0%s' % UTC_End  # noqa

                                # Build output string
                                if heading == 'off' or heading is None:
                                    output_line = '%s%s%s%s%s%s%s%s%s%s%s%s' % (platform_id, year, month, day, UTC_Begin, UTC_End, WLCode, ObsCode, ColumnO3, ozone_std_error, inst_type_id, inst_number)
                                else:
                                    output_line_header = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (platform_id, year, month, day, UTC_Begin, UTC_End, WLCode, ObsCode, ColumnO3, ozone_std_error, inst_type_id, inst_number)

                                if write_output == 1 and ColumnO3 != '   ' and (heading == 'off' or heading is None):
                                    if len(output_line) == 29:
                                        data_file.write(f"{output_line}\r\n")
                                        num_daily_rows_written += 1
                                    else:
                                        if len(output_line) > 29:
                                            log_file.write(f"ERROR#E10:This output line: \'{output_line}\' exceeds 29 characters from row {row}. Data row omitted\r\n")
                                        else:
                                            log_file.write(f"ERROR#E11:This output line: \'{output_line}\' is less than 29 characters from row {row}. Data row omitted\r\n")
                                        num_errors += 1

                                if heading == 'on':
                                    data_file.write(f"{output_line_header}\r\n")

                                if write_output == 0:
                                    LOGGER.debug(f"Output line was not written to master file for row {row} from file {filepath}\r\n")

                                write_output = 1
                    else:
                        log_file.write(f"ERROR#E12:Could not find DAILY in input file: {filepath}. Data omitted\r\n")  # noqa
                        num_errors += 1
                except Exception as err:
                    LOGGER.error(err)
                    log_file.write(f"ERROR: Unable to process file: {filepath}\r\n{err}\r\n")  # noqa
                    num_errors += 1

                if (num_errors > 0):
                    log_file.write(f"DONE ({num_daily_rows_written} DAILY rows) but with {num_errors} errors: {filepath}\r\n\r\n")
                else:
                    log_file.write(f"SUCCESS ({num_daily_rows_written} DAILY rows): {filepath}\r\n\r\n")

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
        LOGGER.info('log file is located here: {}'.format(os.path.abspath('totalOzone_processing_log_{}'.format(current_time))))  # noqa

    @staticmethod
    def normalize_csv_row(row, expected_length):
        """
        Ensure row has the expected number of elements.
        Pads with empty strings if too short, truncates if too long.
        """
        if len(row) < expected_length:
            row.extend([''] * (expected_length - len(row)))
        elif len(row) > expected_length:
            row = row[:expected_length]
        return row
