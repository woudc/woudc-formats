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
from pyshadoz import SHADOZ
import nappy

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
        :param file_content: opened file object for SHADOZ file.
        :param metadata_dic: user specified metadata information
        Processing of data, collecting required information for WOUDC EXT-CSV.
        """

        client = WoudcClient()
        LOGGER.info('Parsing file, collecting data from file.')
        bad_value = ''
        s = SHADOZ(file_content)

        # copy original header to be used as comment
        for key, value in s.metadata.items():
            self.ori.append(key + ' : ' + str(value))

        self.inv.append('SHADOZ Principal Investigator    : ' + s.metadata['SHADOZ Principal Investigator']) # noqa
        self.inv.append('Station Principal Investigator(s): ' + s.metadata['Station Principal Investigator(s)']) # noqa

        bad_value = str(s.metadata['Missing or bad values'])

        # get payload data
        counter = 0
        for row in s.get_data():
            star_flag = False
            if any(['*' in str(x) for x in row]):
                star_flag = True
            Press = row[s.get_data_index('Press')]
            if (type(Press) is str and '*' in Press) or str(int(round(float(Press)))) == bad_value: # noqa
                Press = ''
            else:
                Press = format(Press, '.3f')
            O3PP = row[s.get_data_index('O3', 'mPa')]
            if (type(O3PP) is str and '*' in O3PP) or str(int(round(float(O3PP)))) == bad_value: # noqa
                O3PP = ''
            else:
                O3PP = format(O3PP, '.3f')
            Temp = row[s.get_data_index('Temp')]
            if (type(Temp) is str and '*' in Temp) or str(int(round(float(Temp)))) == bad_value: # noqa
                Temp = ''
            else:
                Temp = format(Temp, '.3f')
            WSPD = row[s.get_data_index('W Spd', 'm/s')]
            if (type(WSPD) is str and '*' in WSPD) or str(int(round(float(WSPD)))) == bad_value: # noqa
                WSPD = ''
            else:
                WSPD = format(WSPD, '.3f')
            WDIR = row[s.get_data_index('W Dir')]
            if (type(WDIR) is str and '*' in WDIR) or str(int(round(float(WDIR)))) == bad_value: # noqa
                WDIR = ''
            else:
                WDIR = format(WDIR, '.3f')
            Duration = str(row[s.get_data_index('Time', 'sec')])
            if '*' in Duration or str(int(round(float(Duration)))) == bad_value: # noqa
                Duration = ''
            GPHeight = row[s.get_data_index('Alt', 'km')]
            if (type(GPHeight) is str and '*' in GPHeight) or str(int(round(float(GPHeight)))) == bad_value: # noqa
                GPHeight = ''
            else:
                GPHeight = str(float(format(GPHeight, '.3f')) * 1000)
            RelativeHumidity = row[s.get_data_index('RH', '%')]
            if (type(RelativeHumidity) is str and '*' in RelativeHumidity) or str(int(round(float(RelativeHumidity)))) == bad_value: # noqa
                RelativeHumidity = ''
            else:
                RelativeHumidity = format(RelativeHumidity, '.3f')
            SampleTemperature = row[s.get_data_index('T Pump')]
            if (type(SampleTemperature) is str and '*' in SampleTemperature) or str(int(round(float(SampleTemperature)))) == bad_value: # noqa
                SampleTemperature = ''
            else:
                SampleTemperature = format(SampleTemperature, '.3f')

            if star_flag:
                self.data_truple.insert(counter, [Press, O3PP, Temp, WSPD,
                                                  WDIR, '',
                                                  Duration, GPHeight,
                                                  RelativeHumidity,
                                                  SampleTemperature])
            else:
                self.data_truple.insert(counter, [Press, O3PP, Temp, WSPD,
                                                  WDIR, '',
                                                  Duration, GPHeight,
                                                  RelativeHumidity,
                                                  SampleTemperature])
                counter += 1

        LOGGER.info('Parsing metadata information from file, resource.cfg, and pywoudc.')  # noqa
        # Getting Information from Config file for CONTENT table
        try:
            LOGGER.info('Getting Content Table information from resource.cfg')
            # station_info dictionary stores parsed data
            self.station_info['Content'] = [
                util.get_config_value('SHADOZ', 'CONTENT.Class'),
                util.get_config_value('SHADOZ', 'CONTENT.Category'),
                util.get_config_value('SHADOZ', 'CONTENT.Level'),
                util.get_config_value('SHADOZ', 'CONTENT.Form')
            ]
        except Exception as err:
            msg = 'Unable to get Content Table information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

        scientific_authority = s.metadata['Station Principal Investigator(s)'].replace(',', '.') # noqa

        if station_name is not None:
            station = station_name
        else:
            try:
                number = s.metadata['STATION'].index(',')
                station = s.metadata['STATION'][0:number]
            except Exception as err:
                msg = 'Unable to get station name from file due to: %s' % str(err)  # noqa
                LOGGER.error(msg)
                station = 'UNKNOWN'
        if agency_name is not None:
            Agency = agency_name
        else:
            try:
                # Get agency information from config
                Agency = util.get_config_value(
                    'AGENCY', station)
            except Exception as err:
                msg = 'Unable to get agency info from config file due to : %s' % str(err)  # noqa
                LOGGER.error(msg)
                Agency = 'N/A'
        try:
            # Map Name from SHADOZ file to WOUDC database's Name
            LOGGER.info('Try to map station name to WOUDC databaset station name.')  # noqa
            station = util.get_config_value(
                'NAME CONVERTER',
                s.metadata['STATION'][0:number])
        except Exception as err:
            msg = 'Unable to find a station name mapping in woudc db due to: %s' % str(err)  # noqa
            LOGGER.error(msg)

        try:
            data_generation_date = str(s.metadata['SHADOZ format data created']) # noqa

            version = ''
            if 'Reprocessed' in s.metadata['SHADOZ Version']:
                version = s.metadata['SHADOZ Version'].strip().split(' ')[0]
            else:
                version = s.metadata['SHADOZ Version']

            self.station_info["Data_Generation"] = [
                data_generation_date,
                Agency,
                version,
                scientific_authority
            ]
        except Exception as err:
            msg = 'Unable to get Data Generation information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

        try:
            self.station_info['Location'] = [str(s.metadata['Latitude (deg)']), # noqa
                                             str(s.metadata['Longitude (deg)']), # noqa
                                             str(s.metadata['Elevation (m)'])]

        except Exception as err:
            msg = 'Unable to get Location information due to: %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        launch_date = ''
        if 'Launch Date' not in s.metadata:
            msg = 'No Launch Date'
            LOGGER.error(msg)
        else:
            launch_date = str(s.metadata['Launch Date'])

        launch_time = ''
        if 'Launch Time (UT)' not in s.metadata:
            msg = 'Launch Time not found.'
            LOGGER.error(msg)
        else:
            launch_time = str(s.metadata['Launch Time (UT)'])

        self.station_info['Timestamp'] = ['+00:00:00',
                                          launch_date,
                                          launch_time]

        if 'Integrated O3 until EOF (DU)' in s.metadata:
            self.station_info['Flight_Summary'] = [
                str(s.metadata['Integrated O3 until EOF (DU)']),
                '', '', '', '', '', '', '', '']

        elif 'Final Integrated O3 (DU)' in s.metadata:
            self.station_info['Flight_Summary'] = [
                str(s.metadata['Final Integrated O3 (DU)']),
                '', '', '', '', '', '', '', '']

        radiosonde = ''
        try:
            idx = str(s.metadata['Radiosonde, SN']).index(',')
            radiosonde = str(s.metadata['Radiosonde, SN'])[0:idx]
        except Exception:
            msg = 'Radiosonde invalid value or not found in file'
            LOGGER.error(msg)

        background_current = ''
        if 'Background current (uA)' in s.metadata:
            background_current = str(s.metadata['Background current (uA)'])

        if 'Sonde/Sage Climatology(1988-2002)' in s.metadata:
            self.station_info['Auxiliary_Data'] = [
                radiosonde.replace(',', ''),
                str(s.metadata['Sonde/Sage Climatology(1988-2002)']).replace(',', ''), # noqa
                background_current.replace(',', ''),
                str(s.metadata['Pump flow rate (sec/100ml)']).replace(',', ''),
                str(s.metadata['Applied pump corrections']).replace(',', ''),
                str(s.metadata['KI Solution']).replace(',', '')]
        elif 'Sonde/MLS Climatology(1988-2010)' in s.metadata:
            self.station_info['Auxiliary_Data'] = [
                radiosonde.replace(',', ''),
                str(s.metadata['Sonde/MLS Climatology(1988-2010)']).replace(',', ''), # noqa
                background_current.replace(',', ''),
                str(s.metadata['Pump flow rate (sec/100ml)']).replace(',', ''),
                str(s.metadata['Applied pump corrections']).replace(',', ''),
                str(s.metadata['KI Solution']).replace(',', '')]
        else:
            self.station_info['Auxiliary_Data'] = [
                radiosonde.replace(',', ''),
                '',
                background_current.replace(',', ''),
                str(s.metadata['Pump flow rate (sec/100ml)']).replace(',', ''),
                str(s.metadata['Applied pump corrections']).replace(',', ''),
                str(s.metadata['KI Solution']).replace(',', '')]

        try:
            LOGGER.info('Getting station metadata by pywoudc.')
            station_metadata = client.get_station_metadata()
        except Exception as err:
            msg = 'Unable to get metadata from pywoudc due to: %S' % str(err)
            LOGGER.error(msg)
            return False, msg

        # Collecting station metadata by using pywoudc
        # Station name and agency name is required to find
        # Station metadata from pywoudc
        header_list = ['type', 'ID', 'station', 'country', 'gaw_id']
        pywoudc_header_list = ['platform_type', 'platform_id', 'platform_name',
                               'country_code', 'gaw_id']
        temp_dict = {}
        for item in header_list:
            temp_dict[item] = ''
            # Pre set an empty dictionary, if user passed in
            # this specified information, insert into dictionary
            if item in metadata_dic.keys():
                temp_dict[item] = metadata_dic[item]

        try:
            LOGGER.info('Processing station metadata information.')
            LOGGER.info('Searching for %s station %s' % (station, Agency))
            for row in station_metadata['features']:
                properties = row['properties']
                LOGGER.info('Data received from Woudc_System: station = [%s]' % properties['platform_name'].encode('utf-8'))  # noqa
                if all([station == properties['platform_name'],
                        Agency == properties['acronym']]):
                    print('\n[%s]\n' % properties)
                    # Match station record in WOUDC database
                    LOGGER.info('Station found in Woudc_System, starting processing platform information.')  # noqa
                    for ind in range(len(header_list)):
                        item = header_list[ind]
                        # Insert data into dictionary only when this
                        # field is empty
                        if temp_dict[item] == '':
                            LOGGER.info('Received %s value %s from Woudc_System.' % (item, properties[pywoudc_header_list[ind]])) # noqa
                            temp_dict[item] = properties[pywoudc_header_list[ind]]  # noqa
                    break
            self.station_info['Platform'] = []

            for item in header_list:
                print('\n%s\n' % temp_dict[item])
                self.station_info['Platform'].append(temp_dict[item])

        except Exception as err:
            msg = 'Unable to process station metadata from pywoudc due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

        try:
            # Processing Instrument information by using pywoudc
            LOGGER.info('Processing instrument metadata information.')
            inst_model = 'UNKNOWN'
            inst_number = 'UNKNOWN'
            if 'inst model' in metadata_dic:
                inst_model = metadata_dic['inst model']
            if 'inst number' in metadata_dic:
                inst_number = metadata_dic['inst number']

            key = ''
            if inst_model == 'UNKNOWN' and inst_number == 'UNKNOWN':
                if ',' in str(s.metadata['Sonde Instrument, SN']) or ' ' in str(s.metadata['Sonde Instrument, SN']).strip():  # noqa
                    key = re.split(',| ', str(s.metadata['Sonde Instrument, SN']).strip())  # noqa
                    key = key[len(key) - 1]
                else:
                    key = str(s.metadata['Sonde Instrument, SN']).strip()
                if str(s.metadata['Sonde Instrument, SN']) == bad_value:
                    inst_model = 'UNKNOWN'
                    inst_number = 'UNKNOWN'
                else:
                    # Try to parse instrument data collected from
                    # SHADOZ file
                    if '-' in key:
                        inst_model = 'UNKNOWN'
                        inst_number = key
                    elif 'z' == key[0:1].lower():  # noqa
                        inst_model = key[0:1]  # noqa
                        inst_number = key[1:]  # noqa
                    elif re.search('[a-zA-Z]', key[0:2]):  # noqa:
                        inst_model = key[0:2]
                        inst_number = key[2:]
                    else:
                        inst_model = 'UNKNOWN'
                        inst_number = key
            if inst_number.strip() == '':
                inst_number = 'UNKNOWN'
            if inst_model.strip() == '':
                inst_model = 'UNKNOWN'
            self.station_info["Instrument"] = [
                "ECC", inst_model, inst_number]
        except Exception as err:
            msg = 'Unable to process instrument metadata due to: %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        return True, 'Parsing Done.'

    def creater(self, filename):
        """
        :return ecsv: ext-csv object that is ready to be dumped out

        Creating ext-csv tables and insert table values
        """
        try:
            LOGGER.info('Creating woudc extcsv template.')
            ecsv = woudc_extcsv.Writer(template=True)
        except Exception as err:
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
        except Exception as err:
            msg = 'Unable to add header due to: %s' % str(err)
            LOGGER.error(msg)

        try:
            LOGGER.info('Adding Content Table.')
            ecsv.add_data("CONTENT",
                          ",".join(self.station_info["Content"]))
        except Exception as err:
            msg = 'Unable to add content table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Data_generation Table.')
            ecsv.add_data("DATA_GENERATION",
                          ",".join(self.station_info["Data_Generation"]))
            x = len(self.inv)
            c = 0
            while c < x:
                ecsv.add_table_comment('DATA_GENERATION', self.inv[c])
                c = c + 1
        except Exception as err:
            msg = 'Unable to add Data Generation table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Platform Table.')
            print('\n\n\n')
            print(self.station_info['Platform'])
            print('\n\n\n')
            ecsv.add_data("PLATFORM",
                          ",".join(self.station_info["Platform"]))
        except Exception as err:
            msg = 'Unable to add Platform Table due to: %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Instrument Table.')
            ecsv.add_data("INSTRUMENT",
                          ",".join(self.station_info["Instrument"]))
        except Exception as err:
            msg = 'Unable to add Instrument table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Location Table.')
            ecsv.add_data("LOCATION",
                          ",".join(self.station_info["Location"]))
        except Exception as err:
            msg = 'Unable to add Location table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Timestamp Table.')
            ecsv.add_data("TIMESTAMP",
                          ",".join(self.station_info["Timestamp"]))
        except Exception as err:
            msg = 'Unable to add Timestamp table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Flight_Summary Table.')
            ecsv.add_data("FLIGHT_SUMMARY",
                          ",".join(self.station_info["Flight_Summary"]),
                          field="IntegratedO3,CorrectionCode,"
                          "SondeTotalO3,CorrectionFactor,TotalO3,"
                          "WLCode,ObsType,Instrument,Number")
        except Exception as err:
            msg = 'Unable to add Flight_Summary table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Auxiliary_Data Table.')
            ecsv.add_data("AUXILIARY_DATA",
                          ",".join(self.station_info["Auxiliary_Data"]),
                          field="RadioSonde,Sonde Climatology,Background Current,PumpRate," # noqa
                          "BackgroundCorr,"
                          "KI Solution")
        except Exception as err:
            msg = 'Unable to add Auxiliary table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        LOGGER.info('Adding Profile Table(Payload).')
        try:
            ecsv.add_data("PROFILE",
                          ",".join(self.data_truple[0]),
                          field="Pressure,O3PartialPressure,Temperature,"
                          "WindSpeed,WindDirection,LevelCode,Duration,"
                          "GPHeight,RelativeHumidity,SampleTemperature")
        except Exception as err:
            msg = 'Cannot add PROFILE table due to: %s ' % str(err)
            LOGGER.error(msg)
            return False, msg

        first_flag = True
        LOGGER.info('Insert payload value to Profile Table.')
        for val in self.data_truple:
            if first_flag:
                first_flag = False
            else:
                ecsv.add_data("PROFILE",
                              ",".join(val))
        return ecsv, 'Create EXT-CSV object Done.'


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
        :param file_content: opened file object for SHADOZ file.
        :param metadata_dic: user specified metadata informatiom
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
                minutes = line[0:4].strip()
                seconds = line[4:7].strip()
                try:
                    time = str(int(minutes) * 60 + int(seconds))
                except Exception as err:
                    msg = '''
                    Cannot convert minutes + seconds to duration due to : %s,
                    minutes: %s, second: %s
                    ''' % (str(err), minutes, seconds)
                    LOGGER.error(msg)
                    return False, msg
                cur_line = []
                counter = counter + 1
                # Pick and choose required information for payload
                cur_line = [line[11:18].strip(), line[76:80].strip(),
                            line[31:37].strip(), '', '', '', time,
                            line[20:27].strip(), line[40:44].strip(), '']
                self.data_truple.insert(counter, cur_line)
            elif flag == 2:
                minutes = line[0:4].strip()
                seconds = line[4:7].strip()
                try:
                    time = str(int(minutes) * 60 + int(seconds))
                except Exception as err:
                    msg = '''
                    Cannot convert minutes + seconds to duration due to : %s,
                    minutes: %s, second: %s
                    ''' % (str(err), minutes, seconds)
                    LOGGER.error(msg)
                    return False, msg
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
        except Exception as err:
            msg = 'Unable to get Content Table information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

        try:
            self.station_info["Data_Generation"] = [
                datetime.datetime.utcnow().strftime('%Y-%m-%d'),
                agency_name,
                '1',
                metadata_dic['SA']
            ]
        except Exception as err:
            msg = 'Unable to get Data_Generation infomation due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

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
        except Exception as err:
            msg = 'Unable to process platform infomation due to: %s' % str(err)
            LOGGER.error(msg)
            return False, msg

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
                if 'instrument' in metadata:
                    if 'z' == metadata["instrument"][0:1].lower() or 'c' == metadata["instrument"][0:1].lower():  # noqa
                        inst_model = metadata["instrument"][0:1]  # noqa
                        inst_number = metadata["instrument"][1:]  # noqa
                    elif re.search('[a-zA-Z]', metadata["instrument"][0:2]):  # noqa:
                        inst_model = metadata["instrument"][0:2]
                        inst_number = metadata["instrument"][2:]
                    else:
                        inst_model = 'UNKNOWN'
                        inst_number = 'UNKNOWN'
                else:
                    inst_model = 'UNKNOWN'
                    inst_number = 'UNKNOWN'
            if inst_model.strip() == '':
                inst_model = 'UNKNOWN'
            if inst_number.strip() == '':
                inst_number = 'UNKNOWN'
            self.station_info['Instrument'] = [
                'ECC',
                inst_model,
                inst_number
            ]
        except Exception as err:
            msg = 'Unable to get Instrument information due to: %s' % str(err)
            LOGGER.error(msg)
            return False, msg

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
        except Exception as err:
            msg = 'Unable to get Location information due to: %s' % str(err)
            LOGGER.error(msg)
            return False, msg

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
                '%s-%s-%s' % (year, month, day),
                time
            ]
        except Exception as err:
            msg = 'Unable to get Timestamp information due to: %s' % str(err)
            LOGGER.error(msg)
            return False, msg

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
        except Exception as err:
            msg = 'Unable to get Flight_Summary information due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

        return True, 'Parsing Done'

    def creater(self, filename):
        """
        :return ecsv: ext-csv object that is ready to be dumped out

        Creating ext-csv tables and insert table values
        """
        try:
            LOGGER.info('Creating woudc extcsv template.')
            ecsv = woudc_extcsv.Writer(template=True)
        except Exception as err:
            msg = 'Unable to create woudc extcsv template due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding header/Comments.')
            ecsv.add_comment('These data were originally received by the WOUDC in Vaisala file format and')  # noqa
            ecsv.add_comment('have been translated into extCSV file format for WOUDC archiving.')  # noqa
            ecsv.add_comment('This translation process re-formats these data into comply with WOUDC standards.')  # noqa
            ecsv.add_comment('')
            ecsv.add_comment('Source File: %s' % filename)
            ecsv.add_comment('')

        except Exception as err:
            msg = 'Unable to add header due to: %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Content Table.')
            ecsv.add_data("CONTENT",
                          ",".join(self.station_info["Content"]))
        except Exception as err:
            msg = 'Unable to add Content table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Data_generation Table.')
            ecsv.add_data("DATA_GENERATION",
                          ",".join(self.station_info["Data_Generation"]))
        except Exception as err:
            msg = 'Unable to add Data_Generation table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Platform Table')
            ecsv.add_data("PLATFORM",
                          ",".join(self.station_info["Platform"]))
        except Exception as err:
            msg = 'Unable to add Platform table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Instrument Table.')
            ecsv.add_data("INSTRUMENT",
                          ",".join(self.station_info["Instrument"]))
        except Exception as err:
            msg = 'Unable to add Instrument table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Location Table.')
            ecsv.add_data("LOCATION",
                          ",".join(self.station_info["Location"]))
        except Exception as err:
            msg = 'Unable to add Location table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Timestamp Table.')
            ecsv.add_data("TIMESTAMP",
                          ",".join(self.station_info["Timestamp"]))
        except Exception as err:
            msg = 'Unable to add Timestamp table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Flight_Summary Table.')
            ecsv.add_data("FLIGHT_SUMMARY",
                          ",".join(self.station_info["Flight_Summary"]),
                          field="IntegratedO3,CorrectionCode,"
                          "SondeTotalO3,CorrectionFactor,TotalO3,"
                          "WLCode,ObsType,Instrument,Number")
        except Exception as err:
            msg = 'Unable to add Flight_Summary table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Adding Profile Table(Payload).')
            ecsv.add_data("PROFILE",
                          ",".join(self.data_truple[0]),
                          field="Pressure,O3PartialPressure,Temperature,"
                          "WindSpeed,WindDirection,LevelCode,Duration,"
                          "GPHeight,RelativeHumidity,SampleTemperature")
        except Exception as err:
            msg = 'Unable to add Profile table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        x = 1
        LOGGER.info('Insert payload value to Profile Table.')
        while x < len(self.data_truple) - 1:

            ecsv.add_data("PROFILE",
                          ",".join(self.data_truple[x]))
            x = x + 1
        return ecsv, 'Create EXT-CSV object Done.'


class BAS_converter(converter):

    def __init__(self):
        """
        Create instance variables.
        """
        self.data_truple = []
        self.station_info = {}

    def parser(self, file_content):
        """
        :param file_content: opened file object for BAS file.

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

        try:
            self.station_info["Data_Generation"] = [
                time,
                util.get_config_value(station, 'agency_name'), "1.0",
                util.get_config_value(station, 'sci_auth')
            ]
        except Exception as err:
            msg = 'Unable to get Data_Generation info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
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
        except Exception as err:
            msg = 'Unable to get platform information due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            self.station_info["Instrument"] = [util.get_config_value(station,
                                               'instrument_name'),
                                               util.get_config_value(station,
                                               'instrument_model'),
                                               util.get_config_value(station,
                                               'instrument_number')]
        except Exception as err:
            msg = 'Unable to get Instrument info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            self.station_info["Location"] = [util.get_config_value(station,
                                             'latitude'),
                                             util.get_config_value(station,
                                             'longitude'),
                                             util.get_config_value(station,
                                             'height')]
        except Exception as err:
            msg = 'Unable to get Location info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            self.station_info["Timestamp"] = ["+00:00:00", "", ""]
        except Exception as err:
            msg = 'Unable to get Timestamp info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        return True, 'Parsing Done'

    def creater(self):
        """
        :return ecsv: ext-csv object that is ready to be dumped out

        Creating ext-csv tables and insert table values
        """
        counter = 0
        dataoutput = []

        try:
            for item in self.data_truple:

                if item == ['', '', '', '', '', '', '', '', '']:
                    break
                hour = float(item[7])
                span = float(item[8])

                dataoutput.insert(counter, [item[1] + "/" + item[0] + "/" + str(round(float(item[2]) / 365.25 + 1900)), "", "", item[3], item[4], str(round(hour + 12, 2)), str(round(round(hour + 12, 2) + span / 60, 2)),  "", item[5], item[6], ""])  # noqa
                counter = counter + 1
        except Exception as err:
            msg = 'Unable to process data payload due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        ecsv = woudc_extcsv.Writer(template=True)

        try:
            ecsv.add_data("CONTENT", ",".join(self.station_info["Content"]))
        except Exception as err:
            msg = 'Unable to add Content table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            ecsv.add_data("DATA_GENERATION",
                          ",".join(self.station_info["Data_Generation"]))
        except Exception as err:
            msg = 'Unable to add Data_Generation table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            ecsv.add_data("PLATFORM", ",".join(self.station_info["Platform"]))
        except Exception as err:
            msg = 'Unable to add Platform table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            ecsv.add_data("INSTRUMENT",
                          ",".join(self.station_info["Instrument"]))
        except Exception as err:
            msg = 'Unable to add Instrument table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            ecsv.add_data("LOCATION", ",".join(self.station_info["Location"]))
        except Exception as err:
            msg = 'Unable to add Location table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            ecsv.add_data("TIMESTAMP",
                          ",".join(self.station_info["Timestamp"]))
        except Exception as err:
            msg = 'Unable to add TimeStamp table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            ecsv.add_data("PROFILE", ",".join(dataoutput[0]),
                          field="Date,WLCode,ObsCode,ColumnO3,StdDevO3,UTC_Begin,UTC_End,UTC_Mean,nOBs,mMu,ColumnSO2")  # noqa
            x = 1
        except Exception as err:
            msg = 'Unable to add Profile table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        while x < len(dataoutput):

            ecsv.add_data("PROFILE", ",".join(dataoutput[x]))
            x = x + 1

        return ecsv, 'Create EXT-CSV object Done.'


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

    def parser(self, file_content, agency_name, metadata_dict):
        """
        :param file_content: path to AMES file.
        :param metadata_dict: dictionary stores user inputed station metadata
        Station name and Agency name is required in order to process AMES file
        Processing of data, collecting required information for WOUDC EXT-CSV.
        """
        if agency_name is None:
            msg = 'Agency name required for AMES conversion'
            LOGGER.error(msg)
            return False, msg

        client = WoudcClient()

        LOGGER.info('Parsing AMES-2160 file.')

        LOGGER.info('Determining AMES format.')
        # Check if file is AMES using try-except
        NDACC = False
        try:
            f = nappy.openNAFile(file_content.name)
        except Exception:
            try:
                f = nappy.openNAFile(file_content.name, ignore_header_lines=1)
                NDACC = True
            except Exception as err:
                msg = 'Unable to parse file due to: %s' % str(err)
                LOGGER.error(msg)
                return False, msg

        LOGGER.info('Reading file data.')
        f.readData()

        ScientificAuthority = f.getOriginator()
        # If Scientific Authority is in form: last, first,
        # change to first last.
        if ',' in ScientificAuthority:
            ScientificAuthority = '%s %s' % (ScientificAuthority.split(',')[1].split()[0].strip(), ScientificAuthority.split(',')[0].strip()) # noqa

        platform_name = f.X[0][0]
        try:
            platform_name = util.get_config_value('AMES',
                                                  platform_name)
        except Exception as err:
            LOGGER.info('Could not get platform name from config due to %s' % str(err)) # noqa
            return False, str(err)

        if NDACC:
            time = f.ignored_header_lines[0].split()[-1].strip().split()[0][0:8] # noqa
        else:
            # Time in Lerwick files is in decimal hours and needs to be
            # converted to HH:mm:ss.
            timeval = f.A[f.ANAME.index('Launch time (Decimal UT hours from 0 hours on day given by DATE)')][0] # noqa
            time = '%02d:%02d:%02d' % (int(timeval), (timeval*60)%60, (timeval*3600)%60) # noqa

        try:
            LOGGER.info('Gathering data values.')
            # If this passes, files are like Boulder.
            # Otherwise, files are like Lerwick/Neumayer.
            headers = [x.strip() for x in f.A[-2][0].split()]
            units = [x.strip() for x in f.A[-1][0].split()]
            temp_index = headers.index('Temp')

            try:
                inst_type = f.A[-7][0]

                # Order of metadata fields differs from file to file, but
                # names are always consistent
                Lat = str(f.A[f.ANAME.index('Station latitude [decimal degrees N]')][0]) # noqa
                Long = str(f.A[f.ANAME.index('Station longitude [decimal degrees E] (range: 0.00 - 359.99)')][0]) # noqa
                Height = str(f.A[f.ANAME.index('Station height [m]')][0])
                ib1 = str(f.A[f.ANAME.index('Ozone background after exposure to ozone in laboratory Ib1 [microA]')][0]) # noqa
                ib2 = str(f.A[f.ANAME.index('Ozone background on filter just prior to launch Ib2 [microA]')][0]) # noqa
                pump_rate = str(f.A[f.ANAME.index('Inverse pump flow rate (s/100 ml)')][0]) # noqa
                correction_factor = str(f.A[f.ANAME.index('Correction factor (COL2/COL1) (negative: not applied; positive: applied)')][0]) # noqa

                Pressure_list = [str(x) for x in f.V[0][0]]
                Duration_list = [str(x) for x in f.X[0][1]]
                SampleTemperature_list = f.V[10][0]
                O3PP_list = [str(x) for x in f.V[4][0]]
                WindDir_list = [str(x) for x in f.V[5][0]]
                WindSpd_list = [str(x) for x in f.V[6][0]]

                Temperature_list = f.V[2][0]

                # Check if temperature is in Kelvin
                LOGGER.info('Checking temperature units.')
                if units[temp_index] == 'K':
                    Temperature_list = [str(x - 273.15) for x in Temperature_list] # noqa
                else:
                    Temperature_list = [str(x) for x in Temperature_list]

                temp_index = headers.index('IntT')
                if units[temp_index] == 'K':
                    SampleTemperature_list = [str(x - 273.15) for x in SampleTemperature_list] # noqa
                else:
                    SampleTemperature_list = [str(x) for x in SampleTemperature_list] # noqa
            except Exception as err:
                msg = 'Unable to gather data values due to : %s' % str(err)
                LOGGER.error(msg)
                return False, msg

        except Exception:
            LOGGER.info('Gathering data values.')
            try:
                # Separate instrument type and model by index of
                # first non-aplha char.
                try:
                    inst_type = f.A[-1][0]
                    for char in inst_type:
                        if not char.isalpha():
                            break
                    inst_model = inst_type[inst_type.index(char):]
                    inst_type = inst_type[:inst_type.index(char)]
                except Exception:
                    inst_model = inst_type = 'UNKNOWN'

                # Order of metadata fields differs from file to file, but
                # names are always consistent
                Lat = str(f.A[f.ANAME.index('Latitude of station (decimal degrees)')][0]) # noqa
                Long = str(f.A[f.ANAME.index('East Longitude of station (decimal degrees)')][0]) # noqa
                Height = ''
                ib1 = str(f.A[f.ANAME.index('Background sensor current before cell is exposed to ozone (microamperes)')][0]) # noqa
                try:
                    ib2 = str(f.A[f.ANAME.index('Background sensor current in the end of the pre-flight calibration (microamperes')][0]) # noqa
                except Exception:
                    ib2 = str(f.A[f.ANAME.index('Background sensor current in the end of the pre-flight calibration (microamperes)')][0]) # noqa
                pump_rate = ''
                correction_factor = str(f.A[f.ANAME.index('Correction factor (COL2A/COL1 or COL2B/COL1) (NOT APPLIED TO DATA)')][0]) # noqa

                Pressure_list = [str(x) for x in f.X[0][1]]
                Duration_list = [str(x) for x in f.V[0][0]]
                SampleTemperature_list = f.V[4][0]
                O3PP_list = [str(x) for x in f.V[5][0]]
                WindDir_list = [str(x) for x in f.V[6][0]]
                WindSpd_list = [str(x) for x in f.V[7][0]]

                Temperature_list = f.V[2][0]

                # Check if temperature is in Kelvin
                LOGGER.info('Checking temperature units.')
                for header in f.VNAME:
                    vals = header.split()
                    if vals[0] == 'Temperature':
                        if len(vals) == 2:
                            if vals[-1].strip('()') == 'K':
                                Temperature_list = [str(x - 273.15) for x in Temperature_list] # noqa
                            else:
                                Temperature_list = [str(x) for x in Temperature_list] # noqa
                        elif len(vals) == 5:
                            if vals[-1].strip('()') == 'K':
                                SampleTemperature_list = [str(x - 273.15) for x in SampleTemperature_list] # noqa
                            else:
                                SampleTemperature_list = [str(x) for x in SampleTemperature_list] # noqa
            except Exception as err:
                msg = 'Unable to gather data values due to : %s' % str(err)
                LOGGER.error(msg)
                return False, msg

        # Date components are in a list, convert to date string.
        LOGGER.info('Converting dates to string.')
        data_gen_date = '%s-%s-%s' % (f.getFileDates()[1][0],
                                      '%02d' % f.getFileDates()[1][1],
                                      '%02d' % f.getFileDates()[1][2])
        instance_date = '%s-%s-%s' % (f.getFileDates()[0][0],
                                      '%02d' % f.getFileDates()[0][1],
                                      '%02d' % f.getFileDates()[0][2])

        GPHeight_list = [str(x) for x in f.V[1][0]]
        RelativeHumidity_list = [str(x) for x in f.V[3][0]]

        LOGGER.info('Retrieving instrument information.')
        try:
            # Separate instrument model and number by index of
            # first alpha char.
            inst_model = f.A[-6][0]
            for char in inst_model:
                if char.isalpha():
                    break
            if inst_model.index(char) == len(inst_model) - 1:
                inst_number = inst_model
                inst_model = 'UNKNOWN'
            else:
                inst_number = inst_model[inst_model.index(char) + 1:]
                inst_model = inst_model[:inst_model.index(char) + 1]
            # Sometimes files have SPC which is too specific.
            if inst_type == 'SPC':
                inst_type = 'ECC'
        except Exception:
            inst_model = inst_number = 'UNKNOWN'

        # Zip all data lists together
        master_list = zip(Pressure_list, O3PP_list, Temperature_list,
                          WindSpd_list, WindDir_list,
                          [''] * len(Pressure_list), Duration_list,
                          GPHeight_list, RelativeHumidity_list,
                          SampleTemperature_list)

        try:
            LOGGER.info('Getting content table information from resource.cfg')
            self.station_info["Content"] = [util.get_config_value("NDACC", "CONTENT.Class"),  # noqa
                                       util.get_config_value("NDACC", "CONTENT.Category"),  # noqa
                                       util.get_config_value("NDACC", "CONTENT.Level"),  # noqa
                                       util.get_config_value("NDACC", "CONTENT.Form")]  # noqa
        except Exception as err:
            msg = 'Unable to get Content Info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            LOGGER.info('Getting station metadata from pywoudc.')
            station_metadata = client.get_station_metadata(raw=False)
        except Exception as err:
            msg = 'Unable to get station metadata from pywoudc due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

        try:
            # processing station metadata from pywoudc, there might be
            # multiple record found for one station under different agency.
            # Therefore, Agency is required to process station metadata
            # Any variable or code relate to geometry is used to access
            # Geometry information returned by pywoudc, not used anymore,
            # but can be un-commentted to make it work
            properties_list = []
            # geometry_list = []
            counter = 0
            LOGGER.info('Parsing station metadata.')
            for row in station_metadata['features']:
                properties = row['properties']
                # geometry = row['geometry']['coordinates']
                if platform_name.lower() == properties['platform_name'].lower(): # noqa
                    properties_list.append(properties)
                    # geometry_list.append(geometry)
                    counter = counter + 1
            if counter == 0:
                LOGGER.warning('Unable to find station: %s, start lookup process.' % platform_name)  # noqa
                try:
                    ID = 'na'
                    Type = 'unknown'
                    Country = 'unknown'
                    GAW = 'unknown'
                    # Lat, Long = util.get_NDACC_station(station)
                except Exception as err:
                    msg = 'Unable to find the station in lookup due to: %s' % str(err)  # noqa
                    LOGGER.error(msg)
            elif counter == 1:
                ID = properties_list[0]['platform_id']
                Type = properties_list[0]['platform_type']
                Country = properties_list[0]['country_code']
                GAW = properties_list[0]['gaw_id']
                # Lat = str(geometry_list[0][1])
                # Long = str(geometry_list[0][0])
            else:
                length = 0
                for item in properties_list:
                    if item['acronym'].lower() == agency_name.lower() or item['contributor_name'].lower() == agency_name.lower():  # noqa
                        ID = item['platform_id']
                        Type = item['platform_type']
                        Country = item['country_code']
                        GAW = item['gaw_id']
                        # Lat = str(geometry_list[length][1])
                        # Long = str(geometry_list[length][0])
                    length = length + 1

            self.station_info['Platform'] = [Type, ID, platform_name,
                                             Country, GAW]
        except Exception as err:
            msg = 'Unable to process station/platform metadata due to: %s' % str(err)  # noqa
            LOGGER.error(msg)
            return False, msg

        if 'version' in metadata_dict:
            Version = metadata_dict['version']
        else:
            Version = '1.0'

        try:
            self.station_info['Data_Generation'] = [data_gen_date, agency_name,
                                                    Version,
                                                    ScientificAuthority]
        except Exception as err:
            msg = 'Unable to add Data_Generation table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            if 'inst type' in metadata_dict:
                inst_type = metadata_dict['inst type']
            if 'inst number' in metadata_dict:
                inst_model = metadata_dict['inst number'].strip()[0:2]
                if re.search('[a-zA-Z]', inst_model) is None:
                    inst_model = 'UNKNOWN'
                inst_number = metadata_dict['inst number'].strip()
            self.station_info['Instrument'] = [inst_type, inst_model,
                                               inst_number]
        except Exception as err:
            msg = 'Unable to get Instrument info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            self.station_info['TimeStamp'] = ['+00:00:00', instance_date, time]
        except Exception as err:
            msg = 'Unable to get Timestamp info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            self.station_info['Location'] = [Lat, Long, Height]
        except Exception as err:
            msg = 'Unable to get Location info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        try:
            self.station_info['Auxiliary_Data'] = ['', ib1, ib2, pump_rate,
                                                   correction_factor, '', '']
        except Exception as err:
            msg = 'Unable to get Auxiliary info due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg

        for sub_list in master_list:
            self.data_truple.append(sub_list)

        return True, 'Parsing Done'

    def creater(self, filename):
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
        ecsv.add_comment('')
        ecsv.add_comment('Source File: %s' % filename)
        ecsv.add_comment('')
        ecsv.add_comment('--- NASA-Ames MNAME ---')
        ecsv.add_comment(self.mname)
        ecsv.add_comment('\n')
        try:
            LOGGER.info('Adding Content Table.')
            ecsv.add_data("CONTENT",
                          ",".join(self.station_info["Content"])
                          )
        except Exception as err:
            msg = 'Unable to add Content Table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg
        try:
            LOGGER.info('Adding Data_Generation Table.')
            ecsv.add_data("DATA_GENERATION",
                          ",".join(self.station_info["Data_Generation"]))
        except Exception as err:
            msg = 'Unable to add Data_Generation table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg
        try:
            LOGGER.info('Adding Platform Table.')
            ecsv.add_data("PLATFORM",
                          ",".join(self.station_info["Platform"]))
        except Exception as err:
            msg = 'Unable to get att platform table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg
        try:
            LOGGER.info('Adding Instrument Table.')
            ecsv.add_data("INSTRUMENT",
                          ",".join(self.station_info["Instrument"]))
        except Exception as err:
            msg = 'Unable to add Instrument table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg
        try:
            LOGGER.info('Adding Location Table.')
            ecsv.add_data("LOCATION",
                          ",".join(self.station_info["Location"]))
        except Exception as err:
            msg = 'Unable to add Location table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg
        try:
            LOGGER.info('Adding Timestamp Table.')
            ecsv.add_data("TIMESTAMP",
                          ",".join(self.station_info["TimeStamp"]))
        except Exception as err:
            msg = 'Unable to add Timestamp table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg
        try:
            LOGGER.info('Adding Auxiliary_Data Table.')
            ecsv.add_data("AUXILIARY_DATA",
                          ",".join(self.station_info["Auxiliary_Data"]),
                          field="MeteoSonde,ib1,ib2,PumpRate,"
                          "BackgroundCorr,SampleTemperatureType,"
                          "MinutesGroundO3")
        except Exception as err:
            msg = 'Unable to add Auxiliary table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg
        try:
            LOGGER.info('Adding Profile Table.')
            ecsv.add_data("PROFILE",
                          ",".join(self.data_truple[0]),
                          field="Pressure,O3PartialPressure,Temperature,"
                                "WindSpeed,WindDirection,LevelCode,Duration,"
                                "GPHeight,RelativeHumidity,SampleTemperature")
        except Exception as err:
            msg = 'Unable to add Profile table due to : %s' % str(err)
            LOGGER.error(msg)
            return False, msg
        x = 1
        LOGGER.info('Inserting payload value.')
        while x < len(self.data_truple):
            ecsv.add_data("PROFILE",
                          ",".join(self.data_truple[x]))
            x = x + 1

        return ecsv, 'Create EXT-CSV object Done.'


def load(InFormat, inpath, station_name=None, agency_name=None,  metadata_dict=None):  # noqa
    """
    :param inpath: full input file path
    :param InFormat: Input file format: SHADOZ, AMES-2160, BAS,
                    AMES-2160-Boulder
    :param metadata_dict: directly inputed station metadata

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
            LOGGER.info('parsing file.')
            status, msg = converter.parser(f, station_name, agency_name, metadata_dict)  # noqa
            if status is False:
                LOGGER.error(msg)
                raise WOUDCFormatParserError(msg)
            ecsv, msg2 = converter.creater(filename)
            if ecsv is False:
                LOGGER.error(msg2)
                raise WOUDCFormatCreateExtCsvError(msg2)
        return ecsv

    elif InFormat.lower() == 'shadoz':
        LOGGER.info('Initiatlizing SHADOZ converter...')
        converter = shadoz_converter()
        LOGGER.info('opening file: %s', inpath)
        with open(inpath) as f:
            LOGGER.info('parsing file.')
            status, msg = converter.parser(f, station_name, agency_name, metadata_dict)  # noqa
            if status is False:
                LOGGER.error(msg)
                raise WOUDCFormatParserError(msg)
            ecsv, msg2 = converter.creater(filename)
            if ecsv is False:
                LOGGER.error(msg2)
                raise WOUDCFormatCreateExtCsvError(msg2)
        return ecsv

    elif InFormat.lower() == 'bas':
        LOGGER.info('Initiatlizing BAS converter...')
        converter = BAS_converter()
        LOGGER.info('opening file: %s', inpath)
        with open(inpath) as f:
            LOGGER.info('parsing file.')
            status, msg = converter.parser(f)
            if status is False:
                LOGGER.error(msg)
                raise WOUDCFormatParserError(msg)
            ecsv, msg2 = converter.creater()
            if ecsv is False:
                LOGGER.error(msg2)
                raise WOUDCFormatCreateExtCsvError(msg2)
        return ecsv

    elif InFormat.lower() == 'ames-2160':
        LOGGER.info('Initiatlizing AMES-2160 converter...')
        converter = AMES_2160_converter()
        LOGGER.info('opening file: %s', inpath)
        with open(inpath) as f:
            LOGGER.info('parsing file.')
            status, msg = converter.parser(f, agency_name, metadata_dict)  # noqa
            if status is False:
                LOGGER.error(msg)
                raise WOUDCFormatParserError(msg)
            ecsv, msg2 = converter.creater(filename)
            if ecsv is False:
                LOGGER.error(msg2)
                raise WOUDCFormatCreateExtCsvError(msg2)
        return ecsv

    else:
        LOGGER.error('Unsupported format: %s' % InFormat)
        raise RuntimeError('Unsupported format: %s' % InFormat)
        return None


def loads(InFormat, str_object, station_name=None, agency_name=None, metadata_dict=None):  # noqa
    """
    :param str_obj: string representation of input file
    :param InFormat: Input file format: SHADOZ, AMES-2160, BAS,
                    AMES-2160-Boulder
    :param metadata_dict: directly inputed station metadata

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
        LOGGER.info('parsing file.')
        status, msg = converter.parser(str_obj, station_name, agency_name, metadata_dict)  # noqa
        if status is False:
            LOGGER.error(msg)
            raise WOUDCFormatParserError(msg)
        ecsv, msg2 = converter.creater('N/A')
        if ecsv is False:
            LOGGER.error(msg2)
            raise WOUDCFormatCreateExtCsvError(msg2)
        return ecsv

    elif InFormat.lower() == 'shadoz':
        LOGGER.info('Initiatlizing SHADOZ converter...')
        converter = shadoz_converter()
        LOGGER.info('parsing file.')
        status, msg = converter.parser(str_obj, station_name, agency_name, metadata_dict)  # noqa
        if status is False:
            LOGGER.error(msg)
            raise WOUDCFormatParserError(msg)
        ecsv, msg2 = converter.creater('N/A')
        if ecsv is False:
            LOGGER.error(msg2)
            raise WOUDCFormatCreateExtCsvError(msg2)
        return ecsv

    elif InFormat.lower() == 'bas':
        LOGGER.info('Initiatlizing BAS converter...')
        converter = BAS_converter()
        LOGGER.info('parsing file.')
        status, msg = converter.parser(str_obj)
        if status is False:
            LOGGER.error(msg)
            raise WOUDCFormatParserError(msg)
        ecsv, msg2 = converter.creater()
        if ecsv is False:
            LOGGER.error(msg2)
            raise WOUDCFormatCreateExtCsvError(msg2)
        return ecsv

    elif InFormat.lower() == 'ames-2160':
        LOGGER.error('AMES requires the use of load, not loads')
        return None

    else:
        LOGGER.error('Unsupported format: %s' % InFormat)
        raise RuntimeError('Unsupported format: %s' % InFormat)
        return None


def dump(ecsv, outpath):
    """
    :param ecsv: ext-csv object that is ready to be printed to outputfile
    :param outpath: output file path

     Print ext-csv object and its information to output file.
    """
    try:
        LOGGER.info('Dump ext-csv table to output file.')
        woudc_extcsv.dump(ecsv, outpath)
    except Exception as err:
        msg = 'Unable to dump ext-csv table to output file due to: %s' % str(err)  # noqa
        LOGGER.error(msg)
        raise WOUDCFormatDumpError(msg)


def dumps(ecsv):
    """
    :param ecsv: ext-csv object that is ready to be printed to outputfile

    Print ext-csv object on to screen.
    """
    try:
        LOGGER.info('Print ext-csv table to screen.')
        return woudc_extcsv.dumps(ecsv)
    except Exception as err:
        msg = 'Unable to print ext-csv table to screen due to: %s' % str(err)
        LOGGER.error(msg)
        raise WOUDCFormatDumpError(msg)


def cli():
    """command line interface to core functions"""
    import json
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

    """
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
            except Exception as err:
                msg = 'Unable to download totalozone snapshot file from: %s,\
                due to: %s' % (input_path, str(err))
                LOGGER.error(msg)
        else:
            try:
                LOGGER.info('Extracting %s', input_path)
                util.extract_data(input_path, output_path)
            except Exception as err:
                msg = 'Unable to extract totalozone snapshot file from :%s,\
                due to: %s' % (input_path, str(err))
                LOGGER.error(msg)
        '''
        try:
            LOGGER.info('Sorting data')
            data, title = MF.sort(output)
        except Exception as err:
            msg = ('Unable to sort data due to :%s', str(err))
            LOGGER.error(msg)
        '''
        try:
            LOGGER.info('Generating masterfile data')
            output2 = MF.execute(os.path.join(output_path, 'totalozone.csv'))
        except Exception as err:
            msg = ('Unable to generate masterfile due to :%s', str(err))
            LOGGER.error(msg)
        try:
            LOGGER.info('Creating zipfile in %s', output_path)
            util.zip_file(output2, output_path, '/o3tot.zip')
        except Exception as err:
            msg = ('Unable to zip file due to :%s', str(err))
            LOGGER.error(msg)
        os.remove(os.path.join(output_path, 'totalozone.csv'))
        LOGGER.info('TotalOzone masterfile process complete.')
    """
    if ARGS.format == 'totalozone-masterfile':
        input = ARGS.inpath
        LOGGER.info('Running totalozone masterfile process...')
        MF = TotalOzone_MasterFile()
        output = ARGS.outpath
        MF.update_totalOzone_master_file(input, output, None, 'overwrite', 'off')  # noqa
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
