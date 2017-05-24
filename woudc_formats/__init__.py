# -*- coding: utf-8 -*-
# ================================================================= #
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
# more information, see # http://www.tbs-sct.gc.ca/fip-pcim/index-eng.asp
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

from abc import ABCMeta, abstractmethod
import logging
import re
import datetime
from pywoudc import WoudcClient
import woudc_extcsv
from woudc_formats import util
import ntpath

__version__ = "0.1.0"

LOGGER = logging.getLogger(__name__)


class converter(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def parser(self, directory):
        return

    @abstractmethod
    def creater(self, data_truple, station_info):
        return


class shadoz_converter(converter):

    def __init__(self):
        """
        Create instance variables.
        """
        self.data_truple = []
        self.station_info = {}
        self.ori = []
        self.inv = []

    def parser(self, file_content, metadata_dic):
        # Place left for time and define logging
        """
        :parm file_content: opened file object for SHADOZ file.

        Processing of data, collecting required information for WOUDC EXT-CSV.
        """
        metadata_dict = {}
        client = WoudcClient()
        counter = 0
        flag = 0
        LOGGER.info('Parsing file, collecting data from file.')
        bad_value = ''

        for lines in file_content:
            if lines == "":
                continue
            if ":" in lines:
                number = lines.index(":")
                key = lines[0:number].strip()
                metadata_dict[key] = lines[number + 1:].strip()
                self.ori.append(lines.strip('\n'))
                if ('SHADOZ Principal Investigator' in lines or
                   'Station Principal Investigator' in lines):
                    self.inv.append(lines.strip('\n'))
                elif 'Missing or bad values' in lines:
                    bad_value = lines[number + 1:].strip()
            elif "sec     hPa         km       C         %" in lines:
                flag = 1
                continue
            elif flag == 1:
                if "*" in lines[16:26].strip():
                    self.data_truple.insert(counter, [lines[6:16].strip(),
                                                      lines[46:56].strip(),
                                                      lines[26:36].strip(),
                                                      lines[86:96].strip(),
                                                      lines[76:86].strip(),
                                                      lines[16:26].strip(),
                                                      lines[36:46].strip(),
                                                      lines[96:106].strip()])
                    continue
                self.data_truple.insert(counter, [lines[6:16].strip(),
                                                  lines[46:56].strip(),
                                                  lines[26:36].strip(),
                                                  lines[86:96].strip(),
                                                  lines[76:86].strip(),
                                                  lines[16:26].strip(),
                                                  lines[36:46].strip(),
                                                  lines[96:106].strip()])
                counter = counter + 1

        LOGGER.info('Parsing metadata information from file, resource.cfg, and pywoudc.')  # noqa
        try:
            LOGGER.info('Getting Content Table information from resource.cfg')
            self.station_info["Content"] = [
                util.get_config_value("SHADOZ", "CONTENT.Class"),
                util.get_config_value("SHADOZ", "CONTENT.Category"),
                util.get_config_value("SHADOZ", "CONTENT.Level"),
                util.get_config_value("SHADOZ", "CONTENT.Form")
            ]
        except Exception, err:
            msg = 'Unable to get Content Table information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)

        if "," in metadata_dict["SHADOZ format data created"]:
            re_data = metadata_dict["SHADOZ format data created"].replace(
                ",", ".")
            metadata_dict["SHADOZ format data created"] = re_data

        if "," in metadata_dict["Station Principal Investigator(s)"]:
            re_in = metadata_dict["Station Principal Investigator(s)"].replace(
                ",", ".")
            metadata_dict["Station Principal Investigator(s)"] = re_in

        if 'station' in metadata_dic:
            station = metadata_dic['station']
        else:
            try:
                number = metadata_dict["STATION"].index(",")
                station = metadata_dict["STATION"][0:number]
            except Exception, err:
                msg = 'Unable to get station name due to: %s' % str(err)
                LOGGER.error(msg)

        if 'agency' in metadata_dic:
            Agency = metadata_dic['agency']
        else:
            try:
                Agency = util.get_config_value(
                    "AGENCY", station)
            except Exception, err:
                LOGGER.error(str(err))
                Agency = 'N/A'
                pass

        try:
            station = util.get_config_value(
                "NAME CONVERTER",
                metadata_dict["STATION"][0:number])
        except Exception, err:
            LOGGER.error(str(err))
            pass

        station = station.decode('UTF-8')

        try:
            LOGGER.info('Getting Agency information from resource.cfg.')
            date_map = {'January': '01', 'February': '02', 'March': '03',
                        'April': '04', 'May': '05', 'June': '06', 'July': '07',
                        'August': '08', 'September': '09', 'October': '10',
                        'November': '11', 'December': '12'}
            for item in date_map:
                if item in metadata_dict["SHADOZ format data created"]:
                    new_date = metadata_dict["SHADOZ format data created"].replace(item, date_map[item])  # noqa
                    new_date = new_date.replace('.', '')
                    new_date = new_date.replace(' ', '-')
                    new_date_temp = new_date.split('-')
                    if len(new_date_temp[0]) == 1:
                        new_date_temp[0] = '0%s' % new_date_temp[0]
                    new_date = '%s-%s-%s' % (new_date_temp[2],
                                             new_date_temp[1],
                                             new_date_temp[0])
                    metadata_dict["SHADOZ format data created"] = new_date
            self.station_info["Data_Generation"] = [
                metadata_dict["SHADOZ format data created"],
                Agency,
                metadata_dict["SHADOZ Version"],
                metadata_dict["Station Principal Investigator(s)"]
            ]
        except Exception, err:
            msg = 'Unable to get Agency information due to: %s' % str(err)
            LOGGER.error(msg)

        self.station_info["Location"] = [metadata_dict["Latitude (deg)"],
                                         metadata_dict["Longitude (deg)"],
                                         metadata_dict["Elevation (m)"]]

        try:
            temp_datetime = metadata_dict["Launch Date"]
            Year = temp_datetime[0:4]
            Month = temp_datetime[4:6]
            Day = temp_datetime[6:].strip()
            metadata_dict["Launch Date"] = '%s-%s-%s' % (Year, Month, Day)
        except Exception, err:
            msg = 'No Launch Date'
            LOGGER.error(msg)
            metadata_dict["Launch Date"] = 'N/A'

        try:
            tok = metadata_dict["Launch Time (UT)"].split(':')
            if len(tok[0]) == 1:
                metadata_dict["Launch Time (UT)"] = '0%s' % metadata_dict["Launch Time (UT)"]  # noqa
            if len(tok) == 2:
                metadata_dict["Launch Time (UT)"] = '%s:00' % metadata_dict["Launch Time (UT)"]  # noqa
        except Exception, err:
            msg = 'Launch Time not found.'
            LOGGER.error(msg)
            metadata_dict["Launch Time (UT)"] = 'UNKNOWN'

        self.station_info["Timestamp"] = ["+00:00:00",
                                          metadata_dict["Launch Date"],
                                          metadata_dict["Launch Time (UT)"]]

        if 'Integrated O3 until EOF (DU)' in metadata_dict:
            self.station_info["Flight_Summary"] = [
                metadata_dict['Integrated O3 until EOF (DU)'],
                "", "", "", "", "", "", "", ""]

        elif 'Final Integrated O3 (DU)' in metadata_dict:
            self.station_info["Flight_Summary"] = [
                metadata_dict['Final Integrated O3 (DU)'],
                "", "", "", "", "", "", "", ""]

        Temp_Radiosonde = metadata_dict["Radiosonde, SN"]

        try:
            idx = Temp_Radiosonde.index(',')
            metadata_dict["Radiosonde, SN"] = Temp_Radiosonde[0:idx]
        except Exception, err:
            msg = 'Radiosonde invalid value'
            LOGGER.error(msg)
            metadata_dict["Radiosonde, SN"] = ""

        self.station_info["Auxillary_Data"] = [
            metadata_dict["Radiosonde, SN"], "",
            metadata_dict["Background current (uA)"],
            "", "", "PUMP", ""]

        try:
            LOGGER.info('Getting station metadata by pywoudc.')
            station_metadata = client.get_station_metadata(raw=False)
        except Exception, err:
            msg = 'Unable to get metadata due to: %S' % str(err)
            LOGGER.error(msg)

        header_list = ['type', 'ID', 'station', 'country', 'gaw_id']
        pywoudc_header_list = ['platform_type', 'platform_id', 'platform_name',
                               'country', 'gaw_id']
        temp_dict = {}
        for item in header_list:
            temp_dict[item] = ''
            if item in metadata_dic.keys():
                temp_dict[item] = metadata_dic[item]

        try:
            LOGGER.info('Processing station metadata information.')
            for row in station_metadata['features']:
                properties = row['properties']
                if (station == properties['platform_name'] and
                   Agency == properties['acronym']):
                    LOGGER.info('Station found in Woudc_System, starting processing platform information.')  # noqa
                    for item in header_list:
                        if temp_dict[item] == '':
                            temp_dict[item] = properties[pywoudc_header_list[header_list.index(item)]]  # noqa
                    break
            self.station_info["Platform"] = []

            for item in header_list:
                self.station_info["Platform"].append(temp_dict[item])

        except Exception, err:
            msg = 'Unable to process station metadata due to: %s' % str(err)
            LOGGER.error(msg)

        try:
            LOGGER.info('Processing instrument metadata information.')
            inst_model = ''
            inst_number = ''
            if 'inst model' in metadata_dic:
                inst_model = metadata_dic['inst model']
            if 'inst number' in metadata_dic:
                inst_number = metadata_dic['inst number']

            if inst_model == '' and inst_number == '':
                if (',' in metadata_dict["Sonde Instrument, SN"] or
                   ' ' in metadata_dict["Sonde Instrument, SN"].strip()):
                    key = re.split(',| ', metadata_dict["Sonde Instrument, SN"].strip())  # noqa
                    key = key[len(key) - 1]
                    metadata_dict["Sonde Instrument, SN"] = key
                else:
                    metadata_dict["Sonde Instrument, SN"] = metadata_dict["Sonde Instrument, SN"].strip()  # noqa
                if metadata_dict["Sonde Instrument, SN"] == bad_value:
                    inst_model = 'N/A'
                    inst_number = 'N/A'
                else:
                    if '-' in metadata_dict["Sonde Instrument, SN"]:
                        inst_model = 'N/A'
                        inst_number = metadata_dict["Sonde Instrument, SN"]
                    elif 'z' == metadata_dict["Sonde Instrument, SN"][0:1].lower():  # noqa
                        inst_model = metadata_dict["Sonde Instrument, SN"][0:1]  # noqa
                        inst_number = metadata_dict["Sonde Instrument, SN"][1:]  # noqa
                    elif re.search('[a-zA-Z]', metadata_dict["Sonde Instrument, SN"][0:2]):  # noqa:
                        inst_model = metadata_dict["Sonde Instrument, SN"][0:2]
                        inst_number = metadata_dict["Sonde Instrument, SN"][2:]
                    else:
                        inst_model = 'UNKNOWN'
                        inst_number = metadata_dict["Sonde Instrument, SN"]

            self.station_info["Instrument"] = [
                "ECC", inst_model, inst_number]
        except Exception, err:
            msg = 'Unable to process instrument metadata due to: %s' % str(err)
            LOGGER.error(msg)

    def creater(self, filename):
        """
        :return ecsv: ext-csv object that is ready to be dumped out

        Creating ext-csv tables and insert table values
        """
        try:
            LOGGER.info('Creating woudc extcsv template.')
            ecsv = woudc_extcsv.Writer(template=True)
        except Exception, err:
            msg = 'Unable to create woudc extcsv template due to: %s' % str(err)  # noqa
            LOGGER.error(msg)

        try:
            LOGGER.info('Adding header/Comments.')
            ecsv.add_comment('These data were originally received by the WOUDC in SHADOZ file format and')  # noqa
            ecsv.add_comment('have been translated into extCSV file format for WOUDC archiving.')  # noqa
            ecsv.add_comment('This translation process re-formats these data into comply with WOUDC standards.')  # noqa
            ecsv.add_comment('')
            ecsv.add_comment('Source File: %s' % filename)
            ecsv.add_comment('')
            x = len(self.ori)
            c = 0
            while c < x:
                ecsv.add_comment(self.ori[c])
                c = c + 1
        except Exception, err:
            msg = 'Unable to add header due to: %s' % str(err)
            LOGGER.error(msg)

        LOGGER.info('Adding Content Table.')
        ecsv.add_data("CONTENT",
                      ",".join(self.station_info["Content"]))

        LOGGER.info('Adding Data_generation Table.')
        ecsv.add_data("DATA_GENERATION",
                      ",".join(self.station_info["Data_Generation"]))
        x = len(self.inv)
        c = 0
        while c < x:
            ecsv.add_table_comment('DATA_GENERATION', self.inv[c])
            c = c + 1

        try:
            if self.station_info["Platform"][1] == "436":
                LOGGER.info('Special treatment for reunion Platform inforamtion.')  # noqa
                self.station_info["Platform"][3] = self.station_info["Platform"][3].encode('UTF-8')  # noqa
                self.station_info["Platform"][2] = self.station_info["Platform"][2].encode('UTF-8')  # noqa
                ecsv.add_data("PLATFORM", self.station_info["Platform"][0], field = 'Type')  # noqa
                ecsv.add_data("PLATFORM", self.station_info["Platform"][1], field = 'ID')  # noqa
                ecsv.add_data("PLATFORM", self.station_info["Platform"][2], field = 'Name')  # noqa
                ecsv.add_data("PLATFORM", self.station_info["Platform"][3], field = 'Country')  # noqa
                ecsv.add_data("PLATFORM", self.station_info["Platform"][4], field = 'GAW_ID')  # noqa
            else:
                LOGGER.info('Adding Platform Table.')
                ecsv.add_data("PLATFORM",
                              ",".join(self.station_info["Platform"]))
        except Exception, err:
            msg = 'Unable to add Platform Table due to: %s' % str(err)
            LOGGER.error(msg)

        LOGGER.info('Adding Instrument Table.')
        ecsv.add_data("INSTRUMENT",
                      ",".join(self.station_info["Instrument"]))

        LOGGER.info('Adding Location Table.')
        ecsv.add_data("LOCATION",
                      ",".join(self.station_info["Location"]))

        LOGGER.info('Adding Timestamp Table.')
        ecsv.add_data("TIMESTAMP",
                      ",".join(self.station_info["Timestamp"]))

        LOGGER.info('Adding Flight_Summary Table.')
        ecsv.add_data("FLIGHT_SUMMARY",
                      ",".join(self.station_info["Flight_Summary"]),
                      field="IntegratedO3,CorrectionCode,"
                      "SondeTotalO3,CorrectionFactor,TotalO3,"
                      "WLCode,ObsType,Instrument,Number")

        LOGGER.info('Adding Aixillary_Data Table.')
        ecsv.add_data("AIXILLARY_DATA",
                      ",".join(self.station_info["Auxillary_Data"]),
                      field="MeteoSonde,ib1,ib2,PumpRate,"
                      "BackgroundCorr,SampleTemperatureType,"
                      "MinutesGroundO3")

        LOGGER.info('Adding Profile Table(Payload).')
        ecsv.add_data("PROFILE",
                      ",".join(self.data_truple[0]),
                      field="Pressure,O3PartialPressure,Temperature,WindSpeed,"
                      "WindDirection,LevelCode,Duration,GPHeight,"
                      "RelativeHumidity,SampleTemperature")
        x = 1
        LOGGER.info('Insert payload value to Profile Table.')
        while x < len(self.data_truple) - 1:

            ecsv.add_data("PROFILE",
                          ",".join(self.data_truple[x]))
            x = x + 1
        return ecsv


class BAS_converter(converter):

    def __init__(self):
        """
        Create instance variables.
        """
        self.data_truple = []
        self.station_info = {}

    def parser(self, file_content):
        """
        :parm file_content: opened file object for BAS file.

        Processing of data, collecting required information for WOUDC EXT-CSV.
        """
        flag = 0
        counter = 0

        for line in file_content:
            if line == "":
                continue
            if "Halley" in line:
                station = "Halley"

            if "Vernadsky" in line:
                station = "Vernadsky"

            if "Sent" in line:
                number = line.index(":")
                number2 = line.find(":", number + 1)
                time = line[number + 1:number2 - 1].strip()
                time = time.replace(",", "")
                date_map = {'January': '01', 'February': '02', 'March': '03',
                            'April': '04', 'May': '05', 'June': '06',
                            'July': '07', 'August': '08', 'September': '09',
                            'October': '10', 'November': '11',
                            'December': '12'}
                for item in date_map:
                    if item in time:
                        time = time.replace(item, date_map[item])
                        break
                date_temp = time.split(' ')
                time = '%s-%s-%s' % (date_temp[2], date_temp[0], date_temp[1])

            if " MM DD  JJJJJ   XXX    SD   N   MU   HOUR  SPAN" in line:
                flag = 1
                continue

            if flag == 1:
                self.data_truple.insert(counter, [line[:4].strip(),
                                                  line[4:7].strip(),
                                                  line[7:14].strip(),
                                                  line[14:20].strip(),
                                                  line[20:26].strip(),
                                                  line[26:30].strip(),
                                                  line[30:36].strip(),
                                                  line[36:42].strip(),
                                                  line[42:48].strip()])
                counter = counter + 1

        self.station_info["Content"] = ["WOUDC", "TotalOzone", "1.0", "1"]

        self.station_info["Data_Generation"] = [
            time,
            util.get_config_value(station, 'agency_name'), "1.0",
            util.get_config_value(station, 'sci_auth')
        ]

        self.station_info["Platform"] = [util.get_config_value(station,
                                         'platform_type'),
                                         util.get_config_value(station,
                                         'platform_id'),
                                         util.get_config_value(station,
                                         'platform_name'),
                                         util.get_config_value(station,
                                         'platform_country'),
                                         util.get_config_value(station,
                                         'platform_gaw_id')]

        self.station_info["Instrument"] = [util.get_config_value(station,
                                           'instrument_name'),
                                           util.get_config_value(station,
                                           'instrument_model'),
                                           util.get_config_value(station,
                                           'instrument_number')]

        self.station_info["Location"] = [util.get_config_value(station,
                                         'latitude'),
                                         util.get_config_value(station,
                                         'longitude'),
                                         util.get_config_value(station,
                                         'height')]

        self.station_info["Timestamp"] = ["+00:00:00", "", ""]

    def creater(self):
        """
        :return ecsv: ext-csv object that is ready to be dumped out

        Creating ext-csv tables and insert table values
        """
        counter = 0
        dataoutput = []

        for item in self.data_truple:

            if item == ['', '', '', '', '', '', '', '', '']:
                break
            hour = float(item[7])
            span = float(item[8])

            dataoutput.insert(counter, [item[1] + "/" + item[0] + "/" +
                              str(round(float(item[2]) / 365.25 + 1900)), "",
                              "", item[3], item[4], str(round(hour + 12, 2)),
                              str(round(round(hour + 12, 2) + span / 60, 2)),
                              "", item[5], item[6], ""])
            counter = counter + 1

        ecsv = woudc_extcsv.Writer(template=True)

        ecsv.add_data("CONTENT", ",".join(self.station_info["Content"]))

        ecsv.add_data("DATA_GENERATION",
                      ",".join(self.station_info["Data_Generation"]))

        ecsv.add_data("PLATFORM", ",".join(self.station_info["Platform"]))

        ecsv.add_data("INSTRUMENT", ",".join(self.station_info["Instrument"]))

        ecsv.add_data("LOCATION", ",".join(self.station_info["Location"]))

        ecsv.add_data("TIMESTAMP", ",".join(self.station_info["Timestamp"]))

        ecsv.add_data("PROFILE", ",".join(dataoutput[0]),
                      field="Date,WLCode,ObsCode,ColumnO3,StdDevO3,UTC_Begin,UTC_End,UTC_Mean,nOBs,mMu,ColumnSO2")  # noqa
        x = 1

        while x < len(dataoutput):

            ecsv.add_data("PROFILE", ",".join(dataoutput[x]))
            x = x + 1

        return ecsv


class AMES_2160_converter(converter):
    """
    Genric AMES-2160 format to WOUDC EXT-CSV format converter.
    """

    def __init__(self):
        """
        Create instance variables.
        """
        self.data_truple = []
        self.station_info = {}
        self.mname = ''

    def parser(self, file_content, metadata_dict):
        """
        :parm file_content: opened file object for AMES file.
        :parm metadata_dict: dictionary stores user inputed station metadata

        Processing of data, collecting required information for WOUDC EXT-CSV.
        """
        client = WoudcClient()
        counter = 0
        flag = False
        sta_map_key = ['ALERT NWT', 'ANDOYA', 'DUMONT D\'URVOZONE', 'EUREKA',
                       'FARADAY', 'HILO', 'HOHENPEISSENOZONE', 'IZANA',
                       'LAUDER', 'LEGIONOWO', 'NATAL', 'NEUMAYER',
                       'NY-ALESUND', 'OHP', 'PARAMARIBO', 'PAYERNE',
                       'PRAHA', 'REUNION ISL', 'SCORESBYSUNDOZONE', 'THULE',
                       'TORONTO', 'TSUKUBA', 'UCCLE', 'YAKUTSK']
        date_map = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                    'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                    'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}
        date_map_key = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL',
                        'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        LOGGER.info('Parsing AMES-2160 file.')
        LOGGER.info('Collecting header inforamtion')
        flag_first = 0
        for line in file_content:
            format_type = None
            counter += 1
            if counter == 1:
                if '2160' in line:
                    flag_first = 1
                    flag = True
                    station_name = metadata_dict['station']
                    continue
                else:
                    if 'station' in metadata_dict:
                        station_name = metadata_dict['station']
                    else:
                        for item in sta_map_key:
                            if item in line:
                                station_name = util.get_config_value("AMES",
                                                                     item)
                                break
                    tok = line.split()
                    idex = line.index('   ')
                    if 'SA' in metadata_dict:
                        PI = metadata_dict['SA']
                    else:
                        PI = line[0:idex].strip()
                    date = tok[len(tok) - 3]
                    for item in date_map_key:
                        if item in date:
                            new_date = date.replace(item, date_map[item])
                            break
                    date = new_date
                    date_generated = datetime.datetime.utcnow().strftime('%Y-%m-%d')  # noqa
                    time = tok[len(tok) - 2][:8]
            if counter == 2:
                if flag_first == 0:
                    tok = line.split()
                    format_type = tok[1].strip()
                    if format_type == '2160':
                        flag = True
                else:
                    if 'SA' in metadata_dict:
                        PI = metadata_dict['SA'].upper().strip()
                    else:
                        if ',' in line:
                            indx = line.index(',')
                            PI = (line[0:indx + 3].replace(
                                ',', '') + '.').upper().strip()
                        else:
                            PI = line.upper().strip() + '.'
            if counter == 5:
                if flag_first == 1:
                    self.mname = line
            if counter == 6:
                if flag_first == 0:
                    self.mname = line
            if counter == 7:
                if flag_first == 1:
                    indx = line.index('   ')
                    date = line[0:indx]
                    date_tok = date.split()
                    if int(date_tok[1]) < 10:
                        date_tok[1] = "0%s" % (date_tok[1])
                    date = "%s-%s-%s" % (date_tok[0], date_tok[1], date_tok[2])
                    time = ''
                    date_generated = datetime.datetime.utcnow().strftime('%Y-%m-%d')  # noqa
                break

        if flag:
            element_mapping = dict({'Pressure at observation': 'Pressure',
                                    'Ozone partial pressure': 'O3PartialPressure',  # noqa
                                    'Temperature': 'Temperature',
                                    'Horizontal wind direction': 'WindDirection',  # noqa
                                    'Time after launch': 'Duration',
                                    'Geopotential height': 'GPHeight',
                                    'Relative humidity': 'RelativeHumidity',
                                    'Temperature inside styrofoam box': 'SampleTemperature',  # noqa
                                    'Horizontal wind speed': 'WindSpeed'})
            element_list = element_mapping.keys()
            element_index_dict = {}
            line_num = 0
            prev_tok_count = 0
            pote_payload_line_num = 0
            pote_payload_counter = 0
            element_index = 0
            inst_raw = None
            pressure_reached = False
            payload_element_done = False
            ecc_inst_reached = False
            LOGGER.info('Checking observation condition.')
            for line in file_content:
                counter += 1
                if 'Pressure at observation' in line:
                    pressure_reached = True
                if pressure_reached and len(line.strip()) in [2, 3, 4, 5] and len(re.findall('[\d]+', line)) != 0:  # noqa
                    payload_element_done = True
                if '(' in line and pressure_reached and not payload_element_done:  # noqa
                    # potential payload element
                    test_str = (line[:line.index('(')]).strip()
                    if test_str in element_list:
                        element_index_dict[element_mapping[test_str]] = element_index  # noqa
                        element_mapping.pop(test_str)
                    element_index += 1
                if 'Serial number of ECC' in line:
                    ecc_inst_reached = True
                if ecc_inst_reached and inst_raw is None:
                    inst_raw_list = re.findall('[0-9][A-Za-z][\s]*[0-9]*', line)  # noqa
                    if len(inst_raw_list) != 0:
                        inst_raw = line
                if len(re.findall('[A-Za-z]+', line)) == 0:
                    # potential payload line
                    line_tok = line.split()
                    if len(line_tok) >= 8:
                        if prev_tok_count == 0:
                            prev_tok_count = len(line_tok)
                        if prev_tok_count != 0:
                            if prev_tok_count != len(line_tok):
                                prev_tok_count = 0
                                pote_payload_line_num = 0
                                pote_payload_counter = 0
                            else:
                                prev_tok_count = len(line_tok)
                        if pote_payload_line_num == 0:
                            pote_payload_line_num = counter
                        pote_payload_counter += 1
                        if pote_payload_counter == 10:
                            break
                    else:
                        prev_tok_count = 0
                        pote_payload_line_num = 0
                        pote_payload_counter = 0
                else:
                    prev_tok_count = 0
                    pote_payload_line_num = 0
                    pote_payload_counter = 0
        try:
            LOGGER.info('Getting content table information from resource.cfg')
            self.station_info["Content"] = [util.get_config_value("NDACC", "CONTENT.Class"),  # noqa
                                       util.get_config_value("NDACC", "CONTENT.Category"),  # noqa
                                       util.get_config_value("NDACC", "CONTENT.Level"),  # noqa
                                       util.get_config_value("NDACC", "CONTENT.Form")]  # noqa

        except Exception, err:
            msg = 'Unable to get content table information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
        Agency = 'na'
        ScientificAuthority = PI
        if 'agency' in metadata_dict:
            Agency = metadata_dict['agency'].strip()
        else:
            try:
                LOGGER.info('Looking for Agency in PI list.')
                Agency = util.get_NDACC_agency(ScientificAuthority).strip()
            except Exception, err:
                msg = 'Unable to find agency due to: %s' % str(err)
                LOGGER.error(msg)

        try:
            LOGGER.info('Getting station metadata from pywoudc.')
            station_metadata = client.get_station_metadata(raw=False)
        except Exception, err:
            msg = 'Unable to get station metadata from pywoudc due to: %s' % str(err)  # noqa
            LOGGER.error(msg)

        try:
            properties_list = []
            geometry_list = []
            counter = 0
            LOGGER.info('Parsing station metadata.')
            for row in station_metadata['features']:
                properties = row['properties']
                geometry = row['geometry']['coordinates']
                if station_name.lower() == properties['platform_name'].lower():
                    properties_list.append(properties)
                    geometry_list.append(geometry)
                    counter = counter + 1
            if counter == 0:
                LOGGER.warning('Unable to find stationi: %s, start lookup process.') % station_name  # noqa
                try:
                    ID = 'na'
                    Type = 'unknown'
                    Country = 'unknown'
                    GAW = 'unknown'
                    Lat, Long = util.get_NDACC_station(station_name)
                except Exception, err:
                    msg = 'Unable to find the station in lookup due to: %s' % str(err)  # noqa
                    LOGGER.error(msg)
            elif counter == 1:
                ID = properties_list[0]['platform_id']
                Type = properties_list[0]['platform_type']
                Country = properties_list[0]['country']
                GAW = properties_list[0]['gaw_id']
                Lat = str(geometry_list[0][1])
                Long = str(geometry_list[0][0])
            else:
                length = 0
                for item in properties_list:
                    if item['acronym'].lower() == Agency.lower() or item['contributor_name'].lower() == Agency.lower():  # noqa
                        ID = item['platform_id']
                        Type = item['platform_type']
                        Country = item['country']
                        GAW = item['gaw_id']
                        Lat = str(geometry_list[length][1])
                        Long = str(geometry_list[length][0])
                    length = length + 1

            self.station_info['Platform'] = [Type, ID, station_name,
                                             Country, GAW]
        except Exception, err:
            msg = 'Unable to process station metadata due to: %s' % str(err)
            LOGGER.error(msg)

        if 'version' in metadata_dict:
            Version = metadata_dict['version']
        else:
            Version = '1.0'

        self.station_info['Data_Generation'] = [date_generated, Agency,
                                                Version, ScientificAuthority]

        Model = 'na'
        Name = 'ECC'
        Number = 'na'
        if 'inst type' in metadata_dict:
            Name = metadata_dict['inst type']
        if 'inst number' in metadata_dict:
            Model = metadata_dict['inst number'].strip()[0:2]
            Number = metadata_dict['inst number'].strip()
        else:
            LOGGER.info('Collecting instrument information.')
            if inst_raw is not None and ecc_inst_reached:
                Model = inst_raw[:2].strip()
                Number = inst_raw.strip()

        self.station_info['Instrument'] = [Name, Model, Number]

        self.station_info['TimeStamp'] = ['+00:00:00', date, time]

        self.station_info['Location'] = [Lat, Long, 'na']

        self.station_info['Auxillary_Data'] = [' ', ' ', ' ', ' ', ' ', ' ',
                                               ' ']

        self.station_info['Flight_Summary'] = [' ', ' ', ' ', ' ',
                                               ' ', ' ', ' ', ' ', ' ']

        line_num = 7
        LOGGER.info('Collecting payload data.')
        if pote_payload_line_num != 0:
            if type(file_content) is not list:
                file_content.seek(0, 0)
                line_num = 0
            for line in file_content:
                if line == "":
                    continue
                line_num += 1
                temp_data = []
                if line_num >= pote_payload_line_num:
                    line_tok = line.split()
                    if len(line_tok) < 9:
                        continue
                    Pressure = ''
                    Duration = ''
                    GPHeight = ''
                    Temperature = ''
                    RelativeHumidity = ''
                    SampleTemperature = ''
                    O3PartialPressure = ''
                    WindDirection = ''
                    WindSpeed = ''
                    LevelCode = ''
                    if 'Pressure' in element_index_dict.keys():
                        Pressure = line_tok[element_index_dict['Pressure']]
                    if 'Duration' in element_index_dict.keys():
                        Duration = line_tok[element_index_dict['Duration']]
                    if 'GPHeight' in element_index_dict.keys():
                        GPHeight = line_tok[element_index_dict['GPHeight']]
                    if 'Temperature' in element_index_dict.keys():
                        Temperature = line_tok[element_index_dict['Temperature']]  # noqa
                    if 'RelativeHumidity' in element_index_dict.keys():
                        RelativeHumidity = line_tok[element_index_dict['RelativeHumidity']]  # noqa
                    if 'SampleTemperature' in element_index_dict.keys():
                        SampleTemperature = line_tok[element_index_dict['SampleTemperature']]  # noqa
                    if 'O3PartialPressure' in element_index_dict.keys():
                        O3PartialPressure = line_tok[element_index_dict['O3PartialPressure']]  # noqa
                    if 'WindDirection' in element_index_dict.keys():
                        WindDirection = line_tok[element_index_dict['WindDirection']]  # noqa
                    if 'WindSpeed' in element_index_dict.keys():
                        WindSpeed = line_tok[element_index_dict['WindSpeed']]

                    temp_data = [Pressure, O3PartialPressure, Temperature,
                                 WindSpeed, WindDirection, LevelCode,
                                 Duration, GPHeight, RelativeHumidity,
                                 SampleTemperature]

                    self.data_truple.append(temp_data)

    def creater(self):
        """
        :return ecsv: EXT-CSV object that contains all tables that
        are required for WOUDC EXT-CSV.

        Creating WOUDC EXT-CSV tables, adding data collected from
        parser method to EXT-CSV tables by using woudc-extscv lib.
        """

        LOGGER.info('Creating woudc ext-csv table')
        ecsv = woudc_extcsv.Writer(template=True)
        LOGGER.info('Adding header and comments.')
        ecsv.add_comment('The data contained in this file was submitted '
                         'to NDACC: http://www.ndsc.ncep.noaa.gov')
        ecsv.add_comment('This WOUDC extended CSV file was generated '
                         'using Woudc_format lib, AMES_converter class')
        ecsv.add_comment('\'na\' is indicated for fields where the value'
                         ' was not available at the time generation of '
                         'this file.')
        ecsv.add_comment('--- NASA-Ames MNAME ---')
        ecsv.add_comment(self.mname)
        ecsv.add_comment('\n')
        LOGGER.info('Adding Content Table.')
        ecsv.add_data("CONTENT",
                      ",".join(self.station_info["Content"])
                      )
        LOGGER.info('Adding Data_Generation Table.')
        ecsv.add_data("DATA_GENERATION",
                      ",".join(self.station_info["Data_Generation"]))
        LOGGER.info('Adding Platform Table.')
        ecsv.add_data("PLATFORM",
                      ",".join(self.station_info["Platform"]))
        LOGGER.info('Adding Instrument Table.')
        ecsv.add_data("INSTRUMENT",
                      ",".join(self.station_info["Instrument"]))
        LOGGER.info('Adding Location Table.')
        ecsv.add_data("LOCATION",
                      ",".join(self.station_info["Location"]))
        LOGGER.info('Adding Timestamp Table.')
        ecsv.add_data("TIMESTAMP",
                      ",".join(self.station_info["TimeStamp"]))
        LOGGER.info('Adding Flight_Summary Table.')
        ecsv.add_data("FLIGHT_SUMMARY",
                      ",".join(self.station_info["Flight_Summary"]),
                      field="IntegratedO3,CorrectionCode,"
                      "SondeTotalO3,CorrectionFactor,TotalO3,"
                      "WLCode,ObsType,Instrument,Number")
        LOGGER.info('Adding Aixillary_Data Table.')
        ecsv.add_data("AIXILLARY_DATA",
                      ",".join(self.station_info["Auxillary_Data"]),
                      field="MeteoSonde,ib1,ib2,PumpRate,"
                      "BackgroundCorr,SampleTemperatureType,"
                      "MinutesGroundO3")
        LOGGER.info('Adding Profile Table.')
        ecsv.add_data("PROFILE",
                      ",".join(self.data_truple[0]),
                      field="Pressure,O3PartialPressure,Temperature,WindSpeed,"
                            "WindDirection,LevelCode,Duration,GPHeight,"
                            "RelativeHumidity,SampleTemperature")
        x = 1
        LOGGER.info('Inserting payload value.')
        while x < len(self.data_truple):
            ecsv.add_data("PROFILE",
                          ",".join(self.data_truple[x]))
            x = x + 1

        return ecsv


class AMES_2160_Boulder_converter(converter):
    """
    Class that is build to convert AMES-2160 data format from
    Station: Boulder.
    """

    def __init__(self):
        """
        Create instance variables.
        """
        self.data_truple = []
        self.station_info = {}
        self.mname = ''

    def parser(self, file_content, metadata_dict):
        """
        :parm file_content: opened file object for AMES file.
        :parm metadata_dict: dictionary stores user inputed station metadata

        Processing of data, collecting required information for WOUDC EXT-CSV.
        """
        client = WoudcClient()
        counter = 0
        flag = False
        if 'raw_file' in metadata_dict:
            raw_filename = metadata_dict['raw_file']
            self.station_info['raw_file'] = raw_filename
        LOGGER.info('Parsing AMES-2160 file.')
        LOGGER.info('Collecting header inforamtion')
        date_map = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                    'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                    'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}
        date_map_key = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL',
                        'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        for line in file_content:
            format_type = None
            counter += 1
            if counter == 1:
                station_name = 'Boulder ESRL HQ (CO)'
                tok = line.split()
                idex = line.index('   ')
                PI = line[0:idex].strip()
                date = tok[len(tok) - 3]
                for item in date_map_key:
                    if item in date:
                        new_date = date.replace(item, date_map[item])
                        break
                day = new_date[0:2]
                month = (new_date[new_date.index('-') +
                         1:new_date.index('-') + 3])
                year = new_date[len(new_date) - 4:len(new_date)]
                date = '%s-%s-%s' % (year, month, day)
                date_generated = datetime.datetime.utcnow().strftime('%Y-%m-%d')  # noqa
                time = tok[len(tok) - 2][:8]
            if counter == 2:
                tok = line.split()
                format_type = tok[1].strip()
                if format_type == '2160':
                    flag = True
            if counter == 6:
                self.mname = line
                break

        if flag:
            element_mapping = {'Pressure': 'Pressure',
                               'Ozone partial pressure': 'O3PartialPressure',
                               'Temperature': 'Temperature',
                               'Horizontal wind direction': 'WindDirection',
                               'Time after launch': 'Duration',
                               'Geopotential height': 'GPHeight',
                               'Relative humidity': 'RelativeHumidity',
                               'Internal temperature': 'SampleTemperature',
                               'Horizontal wind speed': 'WindSpeed'}
            element_list = element_mapping.keys()
            element_index_dict = {}
            line_num = 0
            prev_tok_count = 0
            pote_payload_line_num = 0
            pote_payload_counter = 0
            element_index = 0
            inst_raw = None
            pressure_reached = False
            payload_element_done = False
            ecc_inst_reached = False
            LOGGER.info('Checking observation condition.')
            for line in file_content:
                counter += 1
                if 'Time after launch' in line:
                    pressure_reached = True
                if pressure_reached and 'zzzzz' in line:
                    payload_element_done = True
                if ('[' in line and pressure_reached and
                        not payload_element_done):
                    # potential payload element
                    test_str = (line[:line.index('[')]).strip()
                    if test_str in element_list:
                        element_index_dict[element_mapping[test_str]] = element_index  # noqa
                        element_mapping.pop(test_str)
                    element_index += 1
                if ecc_inst_reached:
                    inst_raw = line.strip()
                    ecc_inst_reached = False
                if line.strip() == 'ECC':
                    ecc_inst_reached = True
                if len(re.findall('[A-Za-z]+', line)) == 0:
                    # potential payload line
                    line_tok = line.split()
                    if len(line_tok) >= 8:
                        if prev_tok_count == 0:
                            prev_tok_count = len(line_tok)
                        if prev_tok_count != 0:
                            if prev_tok_count != len(line_tok):
                                prev_tok_count = 0
                                pote_payload_line_num = 0
                                pote_payload_counter = 0
                            else:
                                prev_tok_count = len(line_tok)
                        if pote_payload_line_num == 0:
                            pote_payload_line_num = counter
                        pote_payload_counter += 1
                        if pote_payload_counter == 10:
                            break
                    else:
                        prev_tok_count = 0
                        pote_payload_line_num = 0
                        pote_payload_counter = 0
                else:
                    prev_tok_count = 0
                    pote_payload_line_num = 0
                    pote_payload_counter = 0
        try:
            LOGGER.info('Getting content table information from resource.cfg')
            self.station_info["Content"] = [
                util.get_config_value("NDACC", "CONTENT.Class"),
                util.get_config_value("NDACC", "CONTENT.Category"),
                util.get_config_value("NDACC", "CONTENT.Level"),
                util.get_config_value("NDACC", "CONTENT.Form")
            ]

        except Exception, err:
            msg = 'Unable to get content table information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
        Agency = 'na'
        ScientificAuthority = PI
        try:
            LOGGER.info('Looking for Agency in PI list.')
            Agency = util.get_NDACC_agency(ScientificAuthority)
        except Exception, err:
            msg = 'Unable to find agency due to: %s' % str(err)
            LOGGER.error(msg)

        if Agency.strip() == 'ESRL/GMD':
            Agency = 'NOAA-CMDL'

        try:
            LOGGER.info('Getting station metadata from pywoudc.')
            station_metadata = client.get_station_metadata(raw=False)
        except Exception, err:
            msg = 'Unable to get station metadata from pywoudc due to: %s' % str(err)  # noqa
            LOGGER.error(msg)

        try:
            properties_list = []
            geometry_list = []
            counter = 0
            LOGGER.info('Parsing station metadata.')
            for row in station_metadata['features']:
                properties = row['properties']
                geometry = row['geometry']['coordinates']
                if station_name.lower() == properties['platform_name'].lower():
                    properties_list.append(properties)
                    geometry_list.append(geometry)
                    counter = counter + 1
            if counter == 0:
                LOGGER.warning('Unable to find station: %s, start lookup process.') % station_name  # noqa
                try:
                    ID = 'na'
                    Type = 'unknown'
                    Country = 'unknown'
                    GAW = 'unknown'
                    Lat, Long = util.get_NDACC_station(station_name)
                except Exception, err:
                    msg = 'Unable to find the station in lookup due to: %s' % str(err)  # noqa
                    LOGGER.error(msg)
            elif counter == 1:
                ID = properties_list[0]['platform_id']
                Type = properties_list[0]['platform_type']
                Country = properties_list[0]['country']
                GAW = properties_list[0]['gaw_id']
                Lat = str(geometry_list[0][0])
                Long = str(geometry_list[0][1])
            else:
                length = 0
                for item in properties_list:
                    if item['acronym'].strip() == Agency.strip():
                        ID = item['platform_id']
                        Type = item['platform_type']
                        Country = item['country']
                        GAW = item['gaw_id']
                        Lat = str(geometry_list[length][1])
                        Long = str(geometry_list[length][0])
                    length = length + 1

            self.station_info['Platform'] = [Type, ID, station_name,
                                             Country, GAW]
        except Exception, err:
            msg = 'Unable to process station metadata due to: %s' % str(err)
            LOGGER.error(msg)

        if 'version' in metadata_dict:
            Version = metadata_dict['version']
        else:
            Version = '1.0'
        self.station_info['Data_Generation'] = [date_generated, Agency,
                                                Version, ScientificAuthority]

        Model = 'na'
        Name = 'ECC'
        Number = 'na'

        LOGGER.info('Collecting instrument information.')

        if inst_raw is not None:
            Model = inst_raw[:2].strip()
            Number = inst_raw.strip()

        self.station_info['Instrument'] = [Name, Model, Number]

        self.station_info['TimeStamp'] = ['+00:00:00', date, time]

        self.station_info['Location'] = [Lat, Long, 'na']

        self.station_info['Auxillary_Data'] = [' ', ' ', ' ', ' ', ' ', ' ',
                                               ' ']

        self.station_info['Flight_Summary'] = [' ', ' ', ' ', ' ', ' ',
                                               ' ', ' ', ' ', ' ']

        line_num = 6
        LOGGER.info('Collecting payload data.')

        if pote_payload_line_num != 0:
            if type(file_content) is not list:
                file_content.seek(0, 0)
                line_num = 0
            for line in file_content:
                if line == "":
                    continue
                line_num += 1
                temp_data = []
                if line_num >= pote_payload_line_num:
                    line_tok = line.split()
                    Pressure = ''
                    Duration = ''
                    GPHeight = ''
                    Temperature = ''
                    RelativeHumidity = ''
                    SampleTemperature = ''
                    O3PartialPressure = ''
                    WindDirection = ''
                    WindSpeed = ''
                    LevelCode = ''
                    if 'Pressure' in element_index_dict.keys():
                        Pressure = line_tok[element_index_dict['Pressure']]
                    if 'Duration' in element_index_dict.keys():
                        Duration = line_tok[element_index_dict['Duration']]
                    if 'GPHeight' in element_index_dict.keys():
                        GPHeight = line_tok[element_index_dict['GPHeight']]
                    if 'Temperature' in element_index_dict.keys():
                        Temperature = line_tok[element_index_dict['Temperature']]  # noqa
                    if 'RelativeHumidity' in element_index_dict.keys():
                        RelativeHumidity = line_tok[element_index_dict['RelativeHumidity']]  # noqa
                    if 'SampleTemperature' in element_index_dict.keys():
                        SampleTemperature = line_tok[element_index_dict['SampleTemperature']]  # noqa
                    if 'O3PartialPressure' in element_index_dict.keys():
                        O3PartialPressure = line_tok[element_index_dict['O3PartialPressure']]  # noqa
                    if 'WindDirection' in element_index_dict.keys():
                        WindDirection = line_tok[element_index_dict['WindDirection']]  # noqa
                    if 'WindSpeed' in element_index_dict.keys():
                        WindSpeed = line_tok[element_index_dict['WindSpeed']]

                    temp_data = [Pressure, O3PartialPressure, Temperature,
                                 WindSpeed, WindDirection, LevelCode,
                                 Duration, GPHeight, RelativeHumidity,
                                 SampleTemperature]

                    self.data_truple.append(temp_data)

    def creater(self):
        """
        :return ecsv: EXT-CSV object that contains all tables that
        are required for WOUDC EXT-CSV.

        Creating WOUDC EXT-CSV tables, adding data collected from
        parser method to EXT-CSV tables by using woudc-extscv lib.
        """

        LOGGER.info('Creating woudc ext-csv table')
        ecsv = woudc_extcsv.Writer(template=True)
        LOGGER.info('Adding header and comments.')
        if 'raw_file' in self.station_info:
            ecsv.add_comment('The data contained in this file was submitted '
                             'to NDACC: ')
            ecsv.add_comment(self.station_info['raw_file'])
        ecsv.add_comment('This WOUDC extended CSV file was generated '
                         'using Woudc_format lib, AMES_converter class')
        ecsv.add_comment('\'na\' is indicated for fields where the value'
                         ' was not available at the time generation of '
                         'this file.')
        ecsv.add_comment('--- NASA-Ames MNAME ---')
        ecsv.add_comment(self.mname)
        ecsv.add_comment('\n')
        LOGGER.info('Adding Content Table.')
        ecsv.add_data("CONTENT",
                      ",".join(self.station_info["Content"])
                      )
        LOGGER.info('Adding Data_Generation Table.')
        ecsv.add_data("DATA_GENERATION",
                      ",".join(self.station_info["Data_Generation"]))
        LOGGER.info('Adding Platform Table.')
        ecsv.add_data("PLATFORM",
                      ",".join(self.station_info["Platform"]))
        LOGGER.info('Adding Instrument Table.')
        ecsv.add_data("INSTRUMENT",
                      ",".join(self.station_info["Instrument"]))
        LOGGER.info('Adding Location Table.')
        ecsv.add_data("LOCATION",
                      ",".join(self.station_info["Location"]))
        LOGGER.info('Adding Timestamp Table.')
        ecsv.add_data("TIMESTAMP",
                      ",".join(self.station_info["TimeStamp"]))
        LOGGER.info('Adding Flight_Summary Table.')
        ecsv.add_data("FLIGHT_SUMMARY",
                      ",".join(self.station_info["Flight_Summary"]),
                      field="IntegratedO3,CorrectionCode,"
                      "SondeTotalO3,CorrectionFactor,TotalO3,"
                      "WLCode,ObsType,Instrument,Number")
        LOGGER.info('Adding Aixillary_Data Table.')
        ecsv.add_data("AIXILLARY_DATA",
                      ",".join(self.station_info["Auxillary_Data"]),
                      field="MeteoSonde,ib1,ib2,PumpRate,"
                      "BackgroundCorr,SampleTemperatureType,"
                      "MinutesGroundO3")
        LOGGER.info('Adding Profile Table.')
        ecsv.add_data("PROFILE",
                      ",".join(self.data_truple[0]),
                      field="Pressure,O3PartialPressure,Temperature,WindSpeed,"
                            "WindDirection,LevelCode,Duration,GPHeight,"
                            "RelativeHumidity,SampleTemperature")
        x = 1
        LOGGER.info('Inserting payload value.')
        while x < len(self.data_truple):
            ecsv.add_data("PROFILE",
                          ",".join(self.data_truple[x]))
            x = x + 1

        return ecsv


def load(InFormat, inpath, metadata_dict={}):
    """
    :parm inpath: full input file path
    :parm InFormat: Input file format: SHADOZ, AMES-2160, BAS,
                    AMES-2160-Boulder
    :parm metadata_dict: directly inputed station metadata

    :return ecsv: ext-csv object thats is ready to be dump out.

    This method process the incoming file, convert them into
    ext-csv obj (dictionary of dictionary), and returns the
    Ext-CSV object.
    """

    head, tail = ntpath.split(inpath)
    if tail == "":
        filename = ntpath.basename(head)
    else:
        filename = tail
    if not bool(metadata_dict):
        metadata_dict = {}
    if InFormat.lower() == 'shadoz':
        LOGGER.info('Initiatlizing SHADOZ converter...')
        converter = shadoz_converter()
        LOGGER.info('opening file: %s', inpath)
        with open(inpath) as f:
            try:
                LOGGER.info('parsing file.')
                converter.parser(f, metadata_dict)
            except Exception, err:
                if 'referenced before assignment' in str(err):
                    err = 'Unsupported SHADOZ formats.'
                msg = 'Unable to parse the file due to: %s' % str(err)
                LOGGER.error(msg)
                raise WOUDCFormatParserError(msg)
            try:
                LOGGER.info('create ext-csv table.')
                ecsv = converter.creater(filename)
            except Exception, err:
                msg = 'Unable to create ext-csv table due to: %s' % str(err)
                LOGGER.error(msg)
                raise WOUDCFormatCreateExtCsvError(msg)
        return ecsv

    elif InFormat.lower() == 'bas':
        LOGGER.info('Initiatlizing BAS converter...')
        converter = BAS_converter()
        LOGGER.info('opening file: %s', inpath)
        with open(inpath) as f:
            try:
                LOGGER.info('parsing file.')
                converter.parser(f)
            except Exception, err:
                if 'referenced before assignment' in str(err):
                    err = 'Unsupported BAS formats.'
                msg = 'Unable to parse the file due to: %s' % str(err)
                LOGGER.error(msg)
                raise WOUDCFormatParserError(msg)
            try:
                LOGGER.info('create ext-csv table.')
                ecsv = converter.creater()
            except Exception, err:
                msg = 'Unable to create ext-csv table due to: %s' % str(err)
                LOGGER.error(msg)
                raise WOUDCFormatCreateExtCsvError(msg)
        return ecsv

    elif InFormat.lower() == 'ames-2160':
        LOGGER.info('Initiatlizing AMES-2160 converter...')
        converter = AMES_2160_converter()
        LOGGER.info('opening file: %s', inpath)
        with open(inpath) as f:
            try:
                LOGGER.info('parsing file.')
                converter.parser(f, metadata_dict)
            except Exception, err:
                if 'referenced before assignment' in str(err):
                    err = 'Unsupported AMES formats.'
                msg = 'Unable to parse the file due to: %s' % str(err)
                LOGGER.error(msg)
                raise WOUDCFormatParserError(msg)
            try:
                LOGGER.info('create ext-csv table.')
                ecsv = converter.creater()
            except Exception, err:
                msg = 'Unable to create ext-csv table due to: %s' % str(err)
                LOGGER.error(msg)
                raise WOUDCFormatCreateExtCsvError(msg)
        return ecsv

    elif InFormat.lower() == 'ames-2160-boulder':
        LOGGER.info('Initializing AMES-2160-Boulder converter...')
        converter = AMES_2160_Boulder_converter()
        with open(inpath) as f:
            try:
                LOGGER.info('parsing file.')
                converter.parser(f, metadata_dict)
            except Exception, err:
                if 'referenced before assignment' in str(err):
                    err = 'Unsupported AMES-Boulder formats.'
                msg = 'Unable to parse the file due to: %s' % str(err)
                LOGGER.error(msg)
                raise WOUDCFormatParserError(msg)

            try:
                LOGGER.info('create ext-csv table.')
                ecsv = converter.creater()
            except Exception, err:
                msg = 'Unable to create ext-csv table due to: %s' % str(err)
                LOGGER.error(msg)
                raise WOUDCFormatCreateExtCsvError(msg)
        return ecsv

    else:
        LOGGER.error('Unsupported format: %s' % InFormat)
        raise RuntimeError('Unsupported format: %s' % InFormat)
        return None


def loads(InFormat, str_object, metadata_dict={}):
    """
    :parm str_obj: string representation of input file
    :parm InFormat: Input file format: SHADOZ, AMES-2160, BAS,
                    AMES-2160-Boulder
    :parm metadata_dict: directly inputed station metadata

    :return ecsv: ext-csv object thats is ready to be dump out.

    This method process the incoming file, convert them into
    ext-csv obj (dictionary of dictionary), and returns the
    Ext-CSV object.
    """
    if not bool(metadata_dict):
        metadata_dict = {}
    if str_object is not None:
        str_obj = str_object.split('\n')
    if InFormat.lower() == 'shadoz':
        LOGGER.info('Initiatlizing SHADOZ converter...')
        converter = shadoz_converter()
        try:
            LOGGER.info('parsing file.')
            converter.parser(str_obj, metadata_dict)
        except Exception, err:
            if 'referenced before assignment' in str(err):
                err = 'Unsupported SHADOZ formats.'
            msg = 'Unable to parse the file due to: %s' % str(err)
            LOGGER.error(msg)
            raise WOUDCFormatParserError(msg)
        try:
            LOGGER.info('create ext-csv table.')
            ecsv = converter.creater('N/A')
        except Exception, err:
            msg = 'Unable to create ext-csv table due to: %s' % str(err)
            LOGGER.error(msg)
            raise WOUDCFormatCreateExtCsvError(msg)
        return ecsv

    elif InFormat.lower() == 'bas':
        LOGGER.info('Initiatlizing BAS converter...')
        converter = BAS_converter()
        try:
            LOGGER.info('parsing file.')
            converter.parser(str_obj)
        except Exception, err:
            if 'referenced before assignment' in str(err):
                err = 'Unsupported BAS formats.'
            msg = 'Unable to parse the file due to: %s' % str(err)
            LOGGER.error(msg)
            raise WOUDCFormatParserError(msg)
        try:
            LOGGER.info('create ext-csv table.')
            ecsv = converter.creater()
        except Exception, err:
            msg = 'Unable to create ext-csv table due to: %s' % str(err)
            LOGGER.error(msg)
            raise WOUDCFormatCreateExtCsvError(msg)
        return ecsv

    elif InFormat.lower() == 'ames-2160':
        LOGGER.info('Initiatlizing AMES-2160 converter...')
        converter = AMES_2160_converter()
        try:
            LOGGER.info('parsing file.')
            converter.parser(str_obj, metadata_dict)
        except Exception, err:
            if 'referenced before assignment' in str(err):
                err = 'Unsupported AMES formats.'
            msg = 'Unable to parse the file due to: %s' % str(err)
            LOGGER.error(msg)
            raise WOUDCFormatParserError(msg)
        try:
            LOGGER.info('create ext-csv table.')
            ecsv = converter.creater()
        except Exception, err:
            msg = 'Unable to create ext-csv table due to: %s' % str(err)
            LOGGER.error(msg)
            raise WOUDCFormatCreateExtCsvError(msg)
        return ecsv

    elif InFormat.lower() == 'ames-2160-boulder':
        LOGGER.info('Initializing AMES-2160-Boulder converter...')
        converter = AMES_2160_Boulder_converter()
        try:
            LOGGER.info('parsing file.')
            converter.parser(str_obj, metadata_dict)
        except Exception, err:
            if 'referenced before assignment' in str(err):
                err = 'Unsupported AMES-Boulder formats.'
            msg = 'Unable to parse the file due to: %s' % str(err)
            LOGGER.error(msg)
            raise WOUDCFormatParserError(msg)
        try:
            LOGGER.info('create ext-csv table.')
            ecsv = converter.creater()
        except Exception, err:
            msg = 'Unable to create ext-csv table due to: %s' % str(err)
            LOGGER.error(msg)
            raise WOUDCFormatCreateExtCsvError(msg)
        return ecsv

    else:
        LOGGER.error('Unsupported format: %s' % InFormat)
        raise RuntimeError('Unsupported format: %s' % InFormat)
        return None


def dump(ecsv, outpath):
    """
    :parm ecsv: ext-csv object that is ready to be printed to outputfile
    :parm outpath: output file path

     Print ext-csv object and its information to output file.
    """
    try:
        LOGGER.info('Dump ext-csv table to output file.')
        woudc_extcsv.dump(ecsv, outpath)
    except Exception, err:
        msg = 'Unable to dump ext-csv table to output file due to: %s' % str(err)  # noqa
        LOGGER.error(msg)
        raise WOUDCFormatDumpError(msg)


def dumps(ecsv):
    """
    :parm ecsv: ext-csv object that is ready to be printed to outputfile

    Print ext-csv object on to screen.
    """
    try:
        LOGGER.info('Print ext-csv table to screen.')
        return woudc_extcsv.dumps(ecsv)
    except Exception, err:
        msg = 'Unable to print ext-csv table to screen due to: %s' % str(err)
        LOGGER.error(msg)
        raise WOUDCFormatDumpError(msg)


def cli():
    """command line interface to core functions"""
    import json
    import os
    import argparse
    from woudc_formats.totalozone_mf import TotalOzone_MasterFile

    LOGGER = logging.getLogger(__name__)

    # Dfine CLI
    PARSER = argparse.ArgumentParser(
        description='Non-standard to WOUDC extended CSV convertor.'
    )

    PARSER.add_argument(
        '--format',
        help='Non-standard format to be converted to WOUDC extended CSV.',
        required=True,
        choices=(
            'SHADOZ',
            'BAS',
            'AMES-2160',
            'AMES-2160-Boulder',
            'totalozone-masterfile'
        )
    )

    PARSER.add_argument(
        '--inpath',
        help='Path to input non-standard data',
        required=True
    )

    PARSER.add_argument(
        '--outpath',
        help='Path to output file',
        required=True
    )

    PARSER.add_argument(
        '--logfile',
        help='log file path',
        required=True
    )

    PARSER.add_argument(
        '--loglevel',
        help='logging level',
        choices=(
            'DEBUG',
            'CRITICAL',
            'ERROR',
            'WARNING',
            'INFO',
            'DEBUG',
            'NOTSET'
        ),
        required=True
    )

    PARSER.add_argument(
        '--metadata',
        help='dictionary of metadata. Keys: station, agency, SA, inst type, inst number, raw_file',  # noqa
        required=False
    )

    ARGS = PARSER.parse_args()
    if ARGS.metadata:
        metadata_dict = json.loads(ARGS.metadata)
    else:
        metadata_dict = {}
    # setup logging
    if ARGS.loglevel and ARGS.logfile:
        util.setup_logger(ARGS.logfile, ARGS.loglevel)

    if ARGS.format == 'totalozone-masterfile':
        input_path = ARGS.inpath
        output_path = ARGS.outpath
        LOGGER.info('Running totalozone masterfile process...')
        MF = TotalOzone_MasterFile()
        if input_path.startswith('http'):
            LOGGER.info('Input is totalozone snapshot CSV: %s', input_path)
            try:
                LOGGER.info('Downloading totalozone snapshot CSV...')
                output = util.download_zip(input_path)
                print output.getvalue()
                LOGGER.info('Downloading totalozone snapshot CSV...')
            except Exception, err:
                msg = 'Unable to download totalozone snapshot file from: %s,\
                due to: %s' % (input_path, str(err))
                LOGGER.error(msg)
        else:
            try:
                LOGGER.info('Extracting %s', input_path)
                util.extract_data(input_path, output_path)
            except Exception, err:
                msg = 'Unable to extract totalozone snapshot file from :%s,\
                due to: %s' % (input_path, str(err))
                LOGGER.error(msg)
        '''
        try:
            LOGGER.info('Sorting data')
            data, title = MF.sort(output)
        except Exception, err:
            msg = ('Unable to sort data due to :%s', str(err))
            LOGGER.error(msg)
        '''
        try:
            LOGGER.info('Generating masterfile data')
            output2 = MF.execute(os.path.join(output_path, 'totalozone.csv'))
        except Exception, err:
            msg = ('Unable to generate masterfile due to :%s', str(err))
            LOGGER.error(msg)
        try:
            LOGGER.info('Creating zipfile in %s', output_path)
            util.zip_file(output2, output_path, '/o3tot.zip')
        except Exception, err:
            msg = ('Unable to zip file due to :%s', str(err))
            LOGGER.error(msg)
        os.remove(os.path.join(output_path, 'totalozone.csv'))
        LOGGER.info('TotalOzone masterfile process complete.')
    else:
        ecsv = load(ARGS.format, ARGS.inpath, metadata_dict)
        if ecsv is not None:
            dump(ecsv, ARGS.outpath)


class WOUDCFormatCreateExtCsvError(Exception):
    """WOUDC Format Parsing Error"""
    pass


class WOUDCFormatParserError(Exception):
    """WOUDC Format Parsing Error"""
    pass


class WOUDCFormatDumpError(Exception):
    """WOUDC Format Parsing Error"""
    pass


if __name__ == '__main__':
    cli()
