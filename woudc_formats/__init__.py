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

    def parser(self, file_content, station_name, agency_name, metadata_dic):
        # Place left for time and define logging
        """
        :parm file_content: opened file object for SHADOZ file.
        :parm metadata_dic: user specified metadata information
        Processing of data, collecting required information for WOUDC EXT-CSV.
        """
        metadata_dict = {}
        client = WoudcClient()
        counter = 0
        flag = 0
        LOGGER.info('Parsing file, collecting data from file.')
        bad_value = ''

        # Going through SHADOZ file line by line
        for lines in file_content:
            if lines == "":
                continue
            if ":" in lines:
                # Each line will be seperated by :, as header and value
                number = lines.index(":")
                key = lines[0:number].strip()
                metadata_dict[key] = lines[number + 1:].strip()
                self.ori.append(lines.strip('\n'))

                # Variation of the line header
                if ('SHADOZ Principal Investigator' in lines or
                   'Station Principal Investigator' in lines):
                    self.inv.append(lines.strip('\n'))
                elif 'Missing or bad values' in lines:
                    bad_value = lines[number + 1:].strip()

            # Locate payload starting line
            elif "sec     hPa         km       C         %" in lines:
                flag = 1
                continue
            elif flag == 1:
                # Pick and Choose required data from payload
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

        # Getting Information from Config file for CONTENT table
        try:
            LOGGER.info('Getting Content Table information from resource.cfg')
            # station_info dictionary stores parsed data
            self.station_info["Content"] = [
                util.get_config_value("SHADOZ", "CONTENT.Class"),
                util.get_config_value("SHADOZ", "CONTENT.Category"),
                util.get_config_value("SHADOZ", "CONTENT.Level"),
                util.get_config_value("SHADOZ", "CONTENT.Form")
            ]
        except Exception, err:
            msg = 'Unable to get Content Table information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)

        # Change date format to meet WOUDC requirement
        # Parsing data from SHADOZ to WOUDC format
        if "," in metadata_dict["SHADOZ format data created"]:
            re_data = metadata_dict["SHADOZ format data created"].replace(
                ",", ".")
            metadata_dict["SHADOZ format data created"] = re_data

        if "," in metadata_dict["Station Principal Investigator(s)"]:
            re_in = metadata_dict["Station Principal Investigator(s)"].replace(
                ",", ".")
            metadata_dict["Station Principal Investigator(s)"] = re_in

        if station_name is not None:
            station = station_name
        else:
            try:
                number = metadata_dict["STATION"].index(",")
                station = metadata_dict["STATION"][0:number]
            except Exception, err:
                msg = 'Unable to get station name due to: %s' % str(err)
                LOGGER.error(msg)

        if agency_name is not None:
            Agency = agency_name
        else:
            try:
                # Get agency information from foncig
                Agency = util.get_config_value(
                    "AGENCY", station)
            except Exception, err:
                LOGGER.error(str(err))
                Agency = 'N/A'
                pass

        try:
            # Map Name from SHADOZ file to WOUDC databse's Name
            station = util.get_config_value(
                "NAME CONVERTER",
                metadata_dict["STATION"][0:number])
        except Exception, err:
            LOGGER.error(str(err))
            pass

        station = station.decode('UTF-8')

        try:
            # Formating date information
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
            # Formatting Date
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

        # Collecting station metadata by using pywoudc
        # Station name and agency name is required to find
        # Station metadata from pywoudc
        header_list = ['type', 'ID', 'station', 'country', 'gaw_id']
        pywoudc_header_list = ['platform_type', 'platform_id', 'platform_name',
                               'country', 'gaw_id']
        temp_dict = {}
        for item in header_list:
            temp_dict[item] = ''
            # Pre set an empty dictionary, if user passed in
            # this specified information, insert into dictionary
            if item in metadata_dic.keys():
                temp_dict[item] = metadata_dic[item]

        try:
            LOGGER.info('Processing station metadata information.')
            for row in station_metadata['features']:
                properties = row['properties']
                if (station == properties['platform_name'] and
                   Agency == properties['acronym']):
                    # Match station record in WOUDC database
                    LOGGER.info('Station found in Woudc_System, starting processing platform information.')  # noqa
                    for item in header_list:
                        # Insert data into dictionary only when this
                        # field is empty
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
            # Processing Instrument information by using pywoudc
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
                    # Try to parse instrument data collected from
                    # SHADOZ file
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


class Vaisala_converter(converter):
    """
    Genric Vaisala format to WOUDC EXT-CSV format converter.
    """

    def __init__(self):
        """
        Create instance variables.
        """
        self.data_truple = []
        self.station_info = {}

    def parser(self, file_content, station_name, agency_name, metadata_dic):
        """
        :parm file_content: opened file object for SHADOZ file.
        :parm metadata_dic: user specified metadata informatiom
        Agency, SA, ID, Station(Name), Country are required as input
        Processing of data, collecting required information for WOUDC EXT-CSV.
        """

        metadata = {}
        flag = 0
        counter = 0

        for line in file_content:
            # Collecting information from Vaisala file
            if line.strip() == '':
                continue
            elif 'Started at' in line:
                metadata['date'] = line.split('    ')[1].strip()
            elif 'Location' in line:
                metadata['location'] = line[line.index(':') + 1:].strip()
            elif 'Special sensor serial number' in line:
                metadata['instrument'] = line.split(':')[1].strip()
            elif 'Integrated Ozone' in line:
                metadata['IntO3'] = line.split(':')[1].strip()
            elif 'Residual Ozone' in line:
                metadata['ResO3'] = line.split(':')[1].strip()
            elif 'Time Pressure   Height  Temperature  RH    VirtT   DPD  LRate AscRate Ozone [mPa]' in line:  # noqa
                header = [x.strip() for x in line.split(' ')]
                header[len(header) - 2] = '%s%s' % (header[len(header) - 2], header[len(header) - 1])  # noqa
                header.pop()
            elif 'min  s      hPa      gpm     deg C      %       C     C    C/km     m/sOzone [mPa]' in line:  # noqa
                flag = 1
            elif 'min  s      hPa      gpm     deg C      %       C     C    C/km     m/sO3 [mPa] and Tb [C]' in line:  # noqa
                flag = 2
            elif flag == 1:
                min = line[0:4].strip()
                seconds = line[4:7].strip()
                time = str(int(min) * 60 + int(seconds))
                cur_line = []
                counter = counter + 1
                # Pick and choose required information for payload
                cur_line = [line[11:18].strip(), line[76:80].strip(),
                            line[31:37].strip(), '', '', '', time,
                            line[20:27].strip(), line[40:44].strip(), '']
                self.data_truple.insert(counter, cur_line)
            elif flag == 2:
                min = line[0:4].strip()
                seconds = line[4:7].strip()
                time = str(int(min) * 60 + int(seconds))
                cur_line = []
                counter = counter + 1
                # Pick and choose required information for payload
                cur_line = [line[11:18].strip(), line[74:80].strip(),
                            line[31:37].strip(), '', '', '', time,
                            line[20:27].strip(), line[40:44].strip(), '']
                self.data_truple.insert(counter, cur_line)

        LOGGER.info('Parsing metadata information from file, resource.cfg, and pywoudc.')  # noqa
        try:
            LOGGER.info('Getting Content Table information from resource.cfg')
            # station_info dictionary stores processed data
            self.station_info["Content"] = [
                util.get_config_value("VAISALA", "CONTENT.Class"),
                util.get_config_value("VAISALA", "CONTENT.Category"),
                util.get_config_value("VAISALA", "CONTENT.Level"),
                util.get_config_value("VAISALA", "CONTENT.Form")
            ]
        except Exception, err:
            msg = 'Unable to get Content Table information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)

        try:
            self.station_info["Data_Generation"] = [
                datetime.datetime.utcnow().strftime('%Y-%m-%d'),
                agency_name,
                '1',
                metadata_dic['SA']
            ]
        except Exception, err:
            msg = 'Unable to get Data_Generation infomation due to: %s' % str(err)  # noqa
            LOGGER.error(msg)

        LOGGER.info('Processing platform information')
        try:
            Type = 'STN'
            ID = metadata_dic['ID']
            Name = station_name
            Country = metadata_dic['country']
            GAW_ID = ''
            if "GAW_ID" in metadata_dic:
                GAW_ID = metadata_dic['GAW_ID']
            self.station_info['Platform'] = [
                Type, ID, Name, Country, GAW_ID
            ]
        except Exception, err:
            msg = 'Unable to process platform infomation due to: %s' % str(err)
            LOGGER.error(msg)

        LOGGER.info('Processing Instrument information')
        try:
            # Parsing instrument information collected from
            # Vaisala file
            inst_model = ''
            inst_number = ''
            if 'inst model' in metadata_dic:
                inst_model = metadata_dic['inst model']
            if 'inst number' in metadata_dic:
                inst_number = metadata_dic['inst number']

            if inst_model == '' and inst_number == '':
                if 'z' == metadata["instrument"][0:1].lower() or 'c' == metadata["instrument"][0:1].lower():  # noqa
                    inst_model = metadata["instrument"][0:1]  # noqa
                    inst_number = metadata["instrument"][1:]  # noqa
                elif re.search('[a-zA-Z]', metadata["instrument"][0:2]):  # noqa:
                    inst_model = metadata["instrument"][0:2]
                    inst_number = metadata["instrument"][2:]
                else:
                    inst_model = 'N/A'
                    inst_number = 'N/A'
            self.station_info['Instrument'] = [
                'ECC',
                inst_model,
                inst_number
            ]
        except Exception, err:
            msg = 'Unable to get Instrument information due to: %s' % str(err)
            LOGGER.error(msg)

        LOGGER.info('Processing Location Information')
        try:
            Lat = 'N/A'
            Lon = 'N/A'
            Evl = 'N/A'
            # Parsing location information Collected
            # from vaisala file
            location_info_list_tmp = metadata['location'].split(' ')
            loc_info_list = []
            for item in location_info_list_tmp:
                if item != '':
                    loc_info_list.append(item)
            if 'N' == loc_info_list[1]:
                Lat = loc_info_list[0]
            elif 'S' == loc_info_list[1]:
                Lat = '-%s' % loc_info_list[0]
            if 'E' == loc_info_list[3]:
                Lon = loc_info_list[2]
            elif 'W' == loc_info_list[3]:
                Lon = '-%s' % loc_info_list[2]
            Evl = loc_info_list[4]
            self.station_info['Location'] = [
                Lat,
                Lon,
                Evl
            ]
        except Exception, err:
            msg = 'Unable to get Location information due to: %s' % str(err)
            LOGGER.error(msg)

        LOGGER.info('Processing Timestamp information')
        try:
            UTCOffset = '+00:00:00'
            date_tok = metadata['date'].split(' ')
            day = date_tok[0]
            month = date_tok[1]
            # Formatting date
            date_map = {'January': '01', 'February': '02', 'March': '03',
                        'April': '04', 'May': '05', 'June': '06', 'July': '07',
                        'August': '08', 'September': '09', 'October': '10',
                        'November': '11', 'December': '12'}
            if month in date_map:
                month = date_map[month]
            year = date_tok[2]
            time = date_tok[3]
            if len(time.split(':')) == 2:
                time = '%s:00' % time
            self.station_info['Timestamp'] = [
                UTCOffset,
                '%s/%s/%s' % (day, month, year),
                time
            ]
        except Exception, err:
            msg = 'Unable to get Timestamp information due to: %s' % str(err)
            LOGGER.error(msg)

        LOGGER.info('Processing Flight_Summary information')
        try:
            # Calculating TotalOzone Value
            # TotalOzone = IntegratedO3 + ResidualO3
            IntO3 = ''
            ResO3 = ''
            TotO3 = ''
            if 'IntO3' in metadata:
                IntO3 = metadata['IntO3']
            if 'ResO3' in metadata:
                ResO3 = metadata['ResO3']
            if IntO3 != '' and ResO3 != '':
                TotO3 = str(float(IntO3) + float(ResO3))
            self.station_info['Flight_Summary'] = [
                IntO3, '0', TotO3, '', '', '', '', '', ''
            ]
        except Exception, err:
            msg = 'Unable to get Flight_Summary information due to: %s' % str(err)  # noqa
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
            ecsv.add_comment('These data were originally received by the WOUDC in Vaisala file format and')  # noqa
            ecsv.add_comment('have been translated into extCSV file format for WOUDC archiving.')  # noqa
            ecsv.add_comment('This translation process re-formats these data into comply with WOUDC standards.')  # noqa
            ecsv.add_comment('')
            ecsv.add_comment('Source File: %s' % filename)
            ecsv.add_comment('')

        except Exception, err:
            msg = 'Unable to add header due to: %s' % str(err)
            LOGGER.error(msg)

        LOGGER.info('Adding Content Table.')
        ecsv.add_data("CONTENT",
                      ",".join(self.station_info["Content"]))

        LOGGER.info('Adding Data_generation Table.')
        ecsv.add_data("DATA_GENERATION",
                      ",".join(self.station_info["Data_Generation"]))

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
                      ",".join(self.station_info["Timestamp"]))

        LOGGER.info('Adding Flight_Summary Table.')
        ecsv.add_data("FLIGHT_SUMMARY",
                      ",".join(self.station_info["Flight_Summary"]),
                      field="IntegratedO3,CorrectionCode,"
                      "SondeTotalO3,CorrectionFactor,TotalO3,"
                      "WLCode,ObsType,Instrument,Number")

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
        # Collecting data from BAS file
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

            # Collecting payload data
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
        # Get all these value from config
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

    def parser(self, file_content, station_name, agency_name, metadata_dict):
        """
        :parm file_content: opened file object for AMES file.
        :parm metadata_dict: dictionary stores user inputed station metadata
        Station name and Agency name is required in order to process AMES file
        Processing of data, collecting required information for WOUDC EXT-CSV.
        """
        client = WoudcClient()
        counter = 0
        flag = False
        LOGGER.info('Parsing AMES-2160 file.')
        LOGGER.info('Collecting header inforamtion')
        flag_first = 0
        time = 'UNKNOWN'
        flag = False
        date_tok = []
        for line in file_content:
            counter += 1
            # Collecting information line by line
            if counter == 1:
                # Two situation:
                #      1. first line contains 2160
                #      2. ndacc AMES-2160, contains a header line for the file
                # Only AMES from ndacc with header contains time
                # information(hh:mm:ss)
                if '2160' in line:
                    station = station_name
                    station = station.decode('UTF-8')
                    flag = True
                    continue
                else:
                    if flag_first == 1:
                        raise Exception('Unsupported AMES file')
                    counter = 0
                    flag_first = 1
                    tok = line.split('   ')
                    time = tok[-1].strip().split(' ')[1]
                    time = time[0:8]
                    continue
            elif counter == 2:
                # Second line of AMES is SA, need to reformat it to WOUDC
                # Standard
                if 'SA' in metadata_dict:
                    PI = metadata_dict['SA'].upper().strip()
                else:
                    if ',' in line:
                        First_Name = line.split(',')[0].strip()
                        Last_Name = line.split(',')[1].strip()[0]
                        PI = '%s %s.' % (First_Name, Last_Name)
                    else:
                        PI = line.strip()
            elif counter == 3:
                # Third line is the agency name, usually is different
                # from the name in WOUDC database, it is perfered
                # for user to pass in agency name
                if agency_name is not None:
                    Agency = agency_name
                else:
                    Agency = 'UNKNOWN'
            if counter == 5:
                # line 5 is program name
                self.mname = line
            if counter == 7:
                # Parsing date information
                # First date information is data collection date
                # Second date is data generation date
                date_tok_temp = line.split(' ')
                for item in date_tok_temp:
                    item = item.strip()
                    if item != '' and item != ' ':
                        date_tok.append(item)
                RDATE = '%s-%s-%s' % (date_tok[3], date_tok[4], date_tok[5])
                RDATE = RDATE.strip()
                DATE = '%s-%s-%s' % (date_tok[0], date_tok[1], date_tok[2])
                DATE = DATE.strip()
                break

        if flag_first == 1:
            counter += 1
        if flag:
            element_mapping = {'Pressure': 'Pressure',
                               'Pressure at observation': 'Pressure',
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
            prev_tok_count = 0
            pote_payload_line_num = 0
            pote_payload_counter = 0
            element_index = 0
            level_counter = 0
            ib1_index = None  # flag: find ib1 or not
            ib2_index = None  # flag: find ib2 or not
            inst_index = None  # flag: find instrument information or not
            height_reached = False  # flag: find station elvation or not
            level_data = []
            inst_raw = None
            level_reached = False  # flag: metadata header reached or not
            level_data_reached = False  # flag: metadata value readed or not
            pressure_reached = False  # flag: payload header reached or not
            payload_element_done = False  # flag: reached end payload header
            ecc_inst_reached = False
            # flag: Does file in specific format that
            # instrument info followed by a ECC line
            LOGGER.info('Checking observation condition.')
            for line in file_content:
                # Some AMES file use () rather than [], change it
                line = line.replace('(', '[')
                line = line.replace(')', ']')
                counter += 1
                if ('Time after launch' in line or
                   'Pressure at observation' in line):
                    # Flagging payload header reached
                    pressure_reached = True
                if pressure_reached and 'zzzzz' in line:
                    # flagging payload header ended
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
                    # ecc_inst_reached reached, next line is
                    # instrument information
                    inst_raw = line.strip()
                    ecc_inst_reached = False
                if line.strip() == 'ECC':
                    # specific format where 'ECC' followed by instrument info
                    ecc_inst_reached = True
                if 'Number of levels' in line:
                    # metadata header section reached
                    level_reached = True
                if level_reached:
                    level_counter += 1
                    # Collecting metadata header, and index for specific header
                    if 'Ozone background after exposure to ozone in laboratory Ib1' in line:  # noqa
                        ib1_index = level_counter - 1
                    if 'Ozone background on filter just prior to launch Ib2' in line:  # noqa
                        ib2_index = level_counter - 1
                    if ('longitude' in line) or ('Longitude' in line):
                        long_index = level_counter - 1
                    if ('latitude' in line) or ('Latitude' in line):
                        lat_index = level_counter - 1
                    if 'Station height' in line or 'Elevation' in line:
                        height_index = level_counter - 1
                        height_reached = True
                    if 'Serial number of ECC' in line:
                        inst_index = level_counter - 1
                    if len(re.findall('[A-Za-z]+', line)) == 0:
                        # Metadata value line only contains number
                        # Potential metadata value
                        if not level_data_reached:
                            if line == '':
                                level_counter -= 1
                                continue
                            line_tok = line.split(' ')
                            if len(line_tok) > 8:
                                # Metadata value found
                                level_data_reached = True
                                data_block_size = level_counter - 1
                    if level_data_reached:
                        # Collecting metadata value into list
                        if (len(level_data) < data_block_size and
                           line[0] != ' '):
                            if len(re.findall('[A-Za-z]+', line)) > 0:
                                # metadata value line with letter is
                                # corresponde to one header
                                level_data.append(line.strip())
                                continue
                            line_tok = line.split(' ')
                            # metadata value line with only number cooresponde
                            # to multiple header, each value seperated by space
                            for item in line_tok:
                                item = item.strip()
                                if item != ' ' and item != '':
                                    level_data.append(item)
                        else:
                            # only payload information starts with space,
                            # if space found, payload line reached
                            level_data_reached = False
                            level_reached = False
                            # Parse collected metadata, if found
                            if ib1_index is not None:
                                ib1 = level_data[ib1_index]
                            else:
                                ib1 = ''
                            if ib2_index is not None:
                                ib2 = level_data[ib2_index]
                            else:
                                ib2 = ''
                            if inst_index is not None:
                                inst_raw = level_data[inst_index]
                            Long = level_data[long_index]
                            Lat = level_data[lat_index]
                            if not height_reached:
                                Height = 'UNKNOWN'
                            else:
                                Height = level_data[height_index]

                if (not level_reached and
                   len(re.findall('[A-Za-z]+', line)) == 0):
                    # potential payload line
                    # Walk through this block of statement to find out
                    # what logic is used to identify payload line
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
                            # found starting payload line
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

        ScientificAuthority = PI

        if Agency == 'UNKNOWN':
            # if Agency is not passed in by user, and not found in AMES,
            # looks for agency based on SA name, might return None
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
            # processing station metadata from pywoudc, there might be
            # multiple record found for one station under different agency.
            # Therefore, Agency is required to process station metadata
            # Any variable or code relate to geometry is used to access
            # Geometry information returned by pywoudc, not used anymore,
            # but can be de-commentted to make it work
            properties_list = []
            # geometry_list = []
            counter = 0
            LOGGER.info('Parsing station metadata.')
            for row in station_metadata['features']:
                properties = row['properties']
                # geometry = row['geometry']['coordinates']
                if station.lower() == properties['platform_name'].lower():
                    properties_list.append(properties)
                    # geometry_list.append(geometry)
                    counter = counter + 1
            if counter == 0:
                LOGGER.warning('Unable to find stationi: %s, start lookup process.') % station  # noqa
                try:
                    ID = 'na'
                    Type = 'unknown'
                    Country = 'unknown'
                    GAW = 'unknown'
                    # Lat, Long = util.get_NDACC_station(station)
                except Exception, err:
                    msg = 'Unable to find the station in lookup due to: %s' % str(err)  # noqa
                    LOGGER.error(msg)
            elif counter == 1:
                ID = properties_list[0]['platform_id']
                Type = properties_list[0]['platform_type']
                Country = properties_list[0]['country']
                GAW = properties_list[0]['gaw_id']
                # Lat = str(geometry_list[0][1])
                # Long = str(geometry_list[0][0])
            else:
                length = 0
                for item in properties_list:
                    if item['acronym'].lower() == Agency.lower() or item['contributor_name'].lower() == Agency.lower():  # noqa
                        ID = item['platform_id']
                        Type = item['platform_type']
                        Country = item['country']
                        GAW = item['gaw_id']
                        # Lat = str(geometry_list[length][1])
                        # Long = str(geometry_list[length][0])
                    length = length + 1

            self.station_info['Platform'] = [Type, ID, station,
                                             Country, GAW]
        except Exception, err:
            msg = 'Unable to process station metadata due to: %s' % str(err)
            LOGGER.error(msg)

        if 'version' in metadata_dict:
            Version = metadata_dict['version']
        else:
            Version = '1.0'

        self.station_info['Data_Generation'] = [RDATE, Agency,
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
            if inst_raw is not None:
                Model = inst_raw[:2].strip()
                Number = inst_raw.strip()

        self.station_info['Instrument'] = [Name, Model, Number]

        self.station_info['TimeStamp'] = ['+00:00:00', DATE, time]

        self.station_info['Location'] = [Lat, Long, Height]

        self.station_info['Auxillary_Data'] = ['', ib1, ib2, '', '', '', '']

        self.station_info['Flight_Summary'] = ['', '', '', '', '', '',
                                               '', '', '']

        # Pick and choose payload data
        # if user using loads method, file_content is a list,
        # there is a special logic to threat it if it is a list
        # (define line_num to 7)
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


def load(InFormat, inpath, station_name=None, agency_name=None,  metadata_dict=None):  # noqa
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
    if InFormat.lower() == 'vaisala':
        LOGGER.info('Initiatlizing Vaisala converter...')
        converter = Vaisala_converter()
        LOGGER.info('opening file: %s', inpath)
        with open(inpath) as f:
            try:
                LOGGER.info('parsing file.')
                converter.parser(f, station_name, agency_name, metadata_dict)
            except Exception, err:
                if 'referenced before assignment' in str(err):
                    err = 'Unsupported Vaisala formats.'
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

    elif InFormat.lower() == 'shadoz':
        LOGGER.info('Initiatlizing SHADOZ converter...')
        converter = shadoz_converter()
        LOGGER.info('opening file: %s', inpath)
        with open(inpath) as f:
            try:
                LOGGER.info('parsing file.')
                converter.parser(f, station_name, agency_name, metadata_dict)
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
                converter.parser(f, station_name, agency_name, metadata_dict)
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

    else:
        LOGGER.error('Unsupported format: %s' % InFormat)
        raise RuntimeError('Unsupported format: %s' % InFormat)
        return None


def loads(InFormat, str_object, station_name=None, agency_name=None, metadata_dict=None):  # noqa
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
    if InFormat.lower() == 'vaisala':
        LOGGER.info('Initiatlizing Vaisala converter...')
        converter = Vaisala_converter()
        try:
            LOGGER.info('parsing file.')
            converter.parser(str_obj, station_name, agency_name, metadata_dict)
        except Exception, err:
            if 'referenced before assignment' in str(err):
                err = 'Unsupported Vaisala formats.'
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

    elif InFormat.lower() == 'shadoz':
        LOGGER.info('Initiatlizing SHADOZ converter...')
        converter = shadoz_converter()
        try:
            LOGGER.info('parsing file.')
            converter.parser(str_obj, station_name, agency_name, metadata_dict)
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
            converter.parser(str_obj, station_name, agency_name, metadata_dict)
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
            'VAISALA',
            'totalozone-masterfile'
        )
    )

    PARSER.add_argument(
        '--station',
        help='WOUDC station name',
        required=False
    )

    PARSER.add_argument(
        '--agency',
        help='WOUDC database\'s agency name',
        required=False
    )

    PARSER.add_argument(
        '--inpath',
        help='Path to input non-standard data',
        required=True
    )

    PARSER.add_argument(
        '--outpath',
        help='Path to output file',
        required=False
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
        help='dictionary of metadata. Keys: SA, inst type, inst number, raw_file',  # noqa
        required=False
    )

    ARGS = PARSER.parse_args()
    if ARGS.station:
        station_name = ARGS.station
    else:
        station_name = None
    if ARGS.agency:
        agency_name = ARGS.agency
    else:
        agency_name = None
    if ARGS.metadata:
        metadata_dict = json.loads(ARGS.metadata)
    else:
        metadata_dict = {}
    if ARGS.outpath:
        output_path = ARGS.outpath
    else:
        output_path = '%s.csv' % ARGS.inpath
    # setup logging
    if ARGS.loglevel and ARGS.logfile:
        util.setup_logger(ARGS.logfile, ARGS.loglevel)

    if ARGS.format == 'totalozone-masterfile':
        input_path = ARGS.inpath
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
        ecsv = load(ARGS.format, ARGS.inpath, station_name,
                    agency_name, metadata_dict)
        if ecsv is not None:
            dump(ecsv, output_path)


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
