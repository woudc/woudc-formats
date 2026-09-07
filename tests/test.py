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
# more information, see # http://www.tbs-sct.gc.ca/fip-pcim/index-eng.asp
#
# Copyright title to all 3rd party software distributed with this
# software is held by the respective copyright holders as noted in
# those files. Users are asked to read the 3rd Party Licenses
# referenced with those assets.
#
# Copyright (c) 2026 Government of Canada
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the 'Software'), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
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
import re
import unittest

from woudc_formats import load, WOUDCFormatParserError
from woudc_formats.util import setup_logger


class Test(unittest.TestCase):
    """Test suite for Writer"""

    def setUp(self):
        """setup test fixtures, etc."""
        loglevel = 'ERROR'
        setup_logger(loglevel)
        LOGGER = logging.getLogger(__name__)  # noqa

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_dump_file(self):
        """notification test"""

    def test_shadoz(self):
        """
        Tests for SHADOZ
        """
        shadoz_filename = "tests/reunion_20141210_V05.dat"
        s = load('SHADOZ', shadoz_filename)

        self.assertTrue("CONTENT" in s.extcsv)
        self.assertTrue("DATA_GENERATION" in s.extcsv)
        self.assertTrue("PLATFORM" in s.extcsv)
        self.assertTrue("INSTRUMENT" in s.extcsv)
        self.assertTrue("LOCATION" in s.extcsv)
        self.assertTrue("TIMESTAMP" in s.extcsv)
        self.assertTrue("FLIGHT_SUMMARY" in s.extcsv)
        self.assertTrue("AUXILIARY_DATA" in s.extcsv)
        self.assertTrue("PROFILE" in s.extcsv)
        self.assertTrue("Class" in s.extcsv["CONTENT"])
        self.assertTrue("Category" in s.extcsv["CONTENT"])
        self.assertTrue("Level" in s.extcsv["CONTENT"])
        self.assertTrue("Form" in s.extcsv["CONTENT"])
        self.assertTrue("Date" in s.extcsv["DATA_GENERATION"])
        self.assertTrue("Agency" in s.extcsv["DATA_GENERATION"])
        self.assertTrue("Version" in s.extcsv["DATA_GENERATION"])
        self.assertTrue("ScientificAuthority" in s.extcsv["DATA_GENERATION"])
        self.assertTrue("Type" in s.extcsv["PLATFORM"])
        self.assertTrue("ID" in s.extcsv["PLATFORM"])
        self.assertTrue("Name" in s.extcsv["PLATFORM"])
        self.assertTrue("Country" in s.extcsv["PLATFORM"])
        self.assertTrue("GAW_ID" in s.extcsv["PLATFORM"])
        self.assertTrue("Name" in s.extcsv["INSTRUMENT"])
        self.assertTrue("Model" in s.extcsv["INSTRUMENT"])
        self.assertTrue("Number" in s.extcsv["INSTRUMENT"])
        self.assertTrue("Latitude" in s.extcsv["LOCATION"])
        self.assertTrue("Longitude" in s.extcsv["LOCATION"])
        self.assertTrue("Height" in s.extcsv["LOCATION"])
        self.assertTrue("UTCOffset" in s.extcsv["TIMESTAMP"])
        self.assertTrue("Date" in s.extcsv["TIMESTAMP"])
        self.assertTrue("Time" in s.extcsv["TIMESTAMP"])
        self.assertTrue("IntegratedO3" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("CorrectionCode" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("SondeTotalO3" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("CorrectionFactor" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("TotalO3" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("WLCode" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("ObsType" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("Instrument" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("Number" in s.extcsv["FLIGHT_SUMMARY"])
        self.assertTrue("RadioSonde" in s.extcsv["AUXILIARY_DATA"])
        self.assertTrue("Sonde Climatology" in s.extcsv["AUXILIARY_DATA"])
        self.assertTrue("Background Current" in s.extcsv["AUXILIARY_DATA"])
        self.assertTrue("PumpRate" in s.extcsv["AUXILIARY_DATA"])
        self.assertTrue("BackgroundCorr" in s.extcsv["AUXILIARY_DATA"])
        self.assertTrue("KI Solution" in s.extcsv["AUXILIARY_DATA"])
        self.assertTrue("Pressure" in s.extcsv["PROFILE"])
        self.assertTrue("O3PartialPressure" in s.extcsv["PROFILE"])
        self.assertTrue("Temperature" in s.extcsv["PROFILE"])
        self.assertTrue("WindSpeed" in s.extcsv["PROFILE"])
        self.assertTrue("WindDirection" in s.extcsv["PROFILE"])
        self.assertTrue("LevelCode" in s.extcsv["PROFILE"])
        self.assertTrue("Duration" in s.extcsv["PROFILE"])
        self.assertTrue("GPHeight" in s.extcsv["PROFILE"])
        self.assertTrue("RelativeHumidity" in s.extcsv["PROFILE"])
        self.assertTrue("SampleTemperature" in s.extcsv["PROFILE"])

        with open(shadoz_filename) as f:
            counter = 0
            line_counter = 0
            payload_val = 0
            for line in f:
                if line_counter == 0:
                    payload_val = int(line)
                if line_counter >= payload_val and line.strip() != '':
                    payload_list = [v.strip() for v in re.split(r'\s{2,}', line.strip())] # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["Pressure"][counter], payload_list[1]) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["O3PartialPressure"][counter], payload_list[5]) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["Temperature"][counter], payload_list[3]) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["WindSpeed"][counter], payload_list[9]) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["WindDirection"][counter], payload_list[8]) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["Duration"][counter], payload_list[0]) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["GPHeight"][counter], str(float(payload_list[2])*1000)) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["RelativeHumidity"][counter], payload_list[4]) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["SampleTemperature"][counter], payload_list[10]) # noqa
                    self.assertEqual(s.extcsv["PROFILE"]["LevelCode"][counter], '') # noqa
                    counter += 1
                line_counter += 1
        for val in ['Pressure', 'O3PartialPressure', 'Temperature',
                    'WindSpeed', 'WindDirection', 'Duration',
                    'GPHeight', 'RelativeHumidity', 'SampleTemperature',
                    'LevelCode']:
            self.assertEqual(len(s.extcsv["PROFILE"][val]), counter)
        self.assertEqual(s.extcsv["PLATFORM"]["Type"], ["STN"])
        self.assertEqual(s.extcsv["PLATFORM"]["Country"],
                         ["France"])
        self.assertEqual(s.extcsv["DATA_GENERATION"]["Agency"],
                         ["U_LaReunion"])

    def test_bas(self):
        """
        BAS Tests
        """
        bas_filename = "tests/7_Vernadsky_2013-05-16.txt"
        b = load('BAS', bas_filename)

        self.assertTrue("CONTENT" in b.extcsv)
        self.assertTrue("DATA_GENERATION" in b.extcsv)
        self.assertTrue("PLATFORM" in b.extcsv)
        self.assertTrue("INSTRUMENT" in b.extcsv)
        self.assertTrue("LOCATION" in b.extcsv)
        self.assertTrue("TIMESTAMP" in b.extcsv)
        self.assertTrue("PROFILE" in b.extcsv)
        self.assertTrue("Class" in b.extcsv["CONTENT"])
        self.assertTrue("Category" in b.extcsv["CONTENT"])
        self.assertTrue("Level" in b.extcsv["CONTENT"])
        self.assertTrue("Form" in b.extcsv["CONTENT"])
        self.assertTrue("Date" in b.extcsv["DATA_GENERATION"])
        self.assertTrue("Agency" in b.extcsv["DATA_GENERATION"])
        self.assertTrue("Version" in b.extcsv["DATA_GENERATION"])
        self.assertTrue("ScientificAuthority" in b.extcsv["DATA_GENERATION"])
        self.assertTrue("Type" in b.extcsv["PLATFORM"])
        self.assertTrue("ID" in b.extcsv["PLATFORM"])
        self.assertTrue("Name" in b.extcsv["PLATFORM"])
        self.assertTrue("Country" in b.extcsv["PLATFORM"])
        self.assertTrue("GAW_ID" in b.extcsv["PLATFORM"])
        self.assertTrue("Name" in b.extcsv["INSTRUMENT"])
        self.assertTrue("Model" in b.extcsv["INSTRUMENT"])
        self.assertTrue("Number" in b.extcsv["INSTRUMENT"])
        self.assertTrue("Latitude" in b.extcsv["LOCATION"])
        self.assertTrue("Longitude" in b.extcsv["LOCATION"])
        self.assertTrue("Height" in b.extcsv["LOCATION"])
        self.assertTrue("UTCOffset" in b.extcsv["TIMESTAMP"])
        self.assertTrue("Date" in b.extcsv["TIMESTAMP"])
        self.assertTrue("Time" in b.extcsv["TIMESTAMP"])
        self.assertTrue("Date" in b.extcsv["PROFILE"])
        self.assertTrue("WLCode" in b.extcsv["PROFILE"])
        self.assertTrue("ObsCode" in b.extcsv["PROFILE"])
        self.assertTrue("ColumnO3" in b.extcsv["PROFILE"])
        self.assertTrue("StdDevO3" in b.extcsv["PROFILE"])
        self.assertTrue("UTC_Begin" in b.extcsv["PROFILE"])
        self.assertTrue("UTC_End" in b.extcsv["PROFILE"])
        self.assertTrue("UTC_Mean" in b.extcsv["PROFILE"])
        self.assertTrue("nOBs" in b.extcsv["PROFILE"])
        self.assertTrue("mMu" in b.extcsv["PROFILE"])
        self.assertTrue("ColumnSO2" in b.extcsv["PROFILE"])

        self.assertEqual(b.extcsv["PROFILE"]["ColumnO3"][0], "264")
        self.assertEqual(b.extcsv["PROFILE"]["ColumnO3"][1], "264")
        self.assertEqual(b.extcsv["PROFILE"]["ColumnO3"][10], "286")
        self.assertEqual(b.extcsv["PROFILE"]["StdDevO3"][10], "4")
        self.assertEqual(b.extcsv["PLATFORM"]["Type"], ["STN"])
        self.assertEqual(b.extcsv["PLATFORM"]["Country"], ["ATA"])
        self.assertEqual(b.extcsv["DATA_GENERATION"]["Agency"], ["BAS"])

    def test_AMES(self):
        """
        Tests for AMES-2160
        """
        AMES_filename = "tests/le140101.b11"
        AMES_filename2 = 'tests/bu20170609.b18'

        # test for error when agency name is None
        # (not passed in CLI)
        with self.assertRaises(WOUDCFormatParserError):
            a = load('AMES-2160', AMES_filename)

        a = load('AMES-2160', AMES_filename, agency_name="UKMO")
        b = load('AMES-2160', AMES_filename2, agency_name="NOAA-CMDL")

        self.assertTrue("CONTENT" in a.extcsv)
        self.assertTrue("DATA_GENERATION" in a.extcsv)
        self.assertTrue("PLATFORM" in a.extcsv)
        self.assertTrue("INSTRUMENT" in a.extcsv)
        self.assertTrue("LOCATION" in a.extcsv)
        self.assertTrue("TIMESTAMP" in a.extcsv)
        self.assertTrue("AUXILIARY_DATA" in a.extcsv)
        self.assertTrue("PROFILE" in a.extcsv)
        self.assertTrue("Class" in a.extcsv["CONTENT"])
        self.assertTrue("Category" in a.extcsv["CONTENT"])
        self.assertTrue("Level" in a.extcsv["CONTENT"])
        self.assertTrue("Form" in a.extcsv["CONTENT"])
        self.assertTrue("Date" in a.extcsv["DATA_GENERATION"])
        self.assertTrue("Agency" in a.extcsv["DATA_GENERATION"])
        self.assertTrue("Version" in a.extcsv["DATA_GENERATION"])
        self.assertTrue("ScientificAuthority" in
                        a.extcsv["DATA_GENERATION"])
        self.assertTrue("Type" in a.extcsv["PLATFORM"])
        self.assertTrue("ID" in a.extcsv["PLATFORM"])
        self.assertTrue("Name" in a.extcsv["PLATFORM"])
        self.assertTrue("Country" in a.extcsv["PLATFORM"])
        self.assertTrue("GAW_ID" in a.extcsv["PLATFORM"])
        self.assertTrue("Name" in a.extcsv["INSTRUMENT"])
        self.assertTrue("Model" in a.extcsv["INSTRUMENT"])
        self.assertTrue("Number" in a.extcsv["INSTRUMENT"])
        self.assertTrue("Latitude" in a.extcsv["LOCATION"])
        self.assertTrue("Longitude" in a.extcsv["LOCATION"])
        self.assertTrue("Height" in a.extcsv["LOCATION"])
        self.assertTrue("UTCOffset" in a.extcsv["TIMESTAMP"])
        self.assertTrue("Date" in a.extcsv["TIMESTAMP"])
        self.assertTrue("Time" in a.extcsv["TIMESTAMP"])
        self.assertTrue("MeteoSonde" in a.extcsv["AUXILIARY_DATA"])
        self.assertTrue("ib1" in a.extcsv["AUXILIARY_DATA"])
        self.assertTrue("ib2" in a.extcsv["AUXILIARY_DATA"])
        self.assertTrue("PumpRate" in a.extcsv["AUXILIARY_DATA"])
        self.assertTrue("BackgroundCorr" in
                        a.extcsv["AUXILIARY_DATA"])
        self.assertTrue("SampleTemperatureType" in
                        a.extcsv["AUXILIARY_DATA"])
        self.assertTrue("MinutesGroundO3" in
                        a.extcsv["AUXILIARY_DATA"])
        self.assertTrue("Pressure" in a.extcsv["PROFILE"])
        self.assertTrue("O3PartialPressure" in a.extcsv["PROFILE"])
        self.assertTrue("Temperature" in a.extcsv["PROFILE"])
        self.assertTrue("WindSpeed" in a.extcsv["PROFILE"])
        self.assertTrue("WindDirection" in a.extcsv["PROFILE"])
        self.assertTrue("LevelCode" in a.extcsv["PROFILE"])
        self.assertTrue("Duration" in a.extcsv["PROFILE"])
        self.assertTrue("GPHeight" in a.extcsv["PROFILE"])
        self.assertTrue("RelativeHumidity" in a.extcsv["PROFILE"])
        self.assertTrue("SampleTemperature" in a.extcsv["PROFILE"])

        with open(AMES_filename) as f:
            payload = False
            counter = 0
            for line in f:
                if payload:
                    payload_list = [str(float(x)) for x in line.split()]
                    self.assertEqual(a.extcsv["PROFILE"]["Pressure"][counter], payload_list[0]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["O3PartialPressure"][counter], payload_list[6]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["Temperature"][counter], payload_list[3]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["WindSpeed"][counter], payload_list[8]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["WindDirection"][counter], payload_list[7]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["Duration"][counter], payload_list[1]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["GPHeight"][counter], payload_list[2]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["RelativeHumidity"][counter], payload_list[4]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["SampleTemperature"][counter], payload_list[5]) # noqa
                    self.assertEqual(a.extcsv["PROFILE"]["LevelCode"][counter], '') # noqa
                    counter += 1
                if 'ECC6A' in line:
                    payload = True

        self.assertEqual(a.extcsv["PLATFORM"]["Type"], ["STN"])
        self.assertEqual(a.extcsv["PLATFORM"]["Country"],
                         ["United Kingdom of Great Britain and Northern Ireland"])  # noqa
        self.assertEqual(a.extcsv["DATA_GENERATION"]["Agency"],
                         ["UKMO"])

        self.assertTrue("CONTENT" in b.extcsv)
        self.assertTrue("DATA_GENERATION" in b.extcsv)
        self.assertTrue("PLATFORM" in b.extcsv)
        self.assertTrue("INSTRUMENT" in b.extcsv)
        self.assertTrue("LOCATION" in b.extcsv)
        self.assertTrue("TIMESTAMP" in b.extcsv)
        self.assertTrue("AUXILIARY_DATA" in b.extcsv)
        self.assertTrue("PROFILE" in b.extcsv)
        self.assertTrue("Class" in b.extcsv["CONTENT"])
        self.assertTrue("Category" in b.extcsv["CONTENT"])
        self.assertTrue("Level" in b.extcsv["CONTENT"])
        self.assertTrue("Form" in b.extcsv["CONTENT"])
        self.assertTrue("Date" in b.extcsv["DATA_GENERATION"])
        self.assertTrue("Agency" in b.extcsv["DATA_GENERATION"])
        self.assertTrue("Version" in b.extcsv["DATA_GENERATION"])
        self.assertTrue("ScientificAuthority" in
                        b.extcsv["DATA_GENERATION"])
        self.assertTrue("Type" in b.extcsv["PLATFORM"])
        self.assertTrue("ID" in b.extcsv["PLATFORM"])
        self.assertTrue("Name" in b.extcsv["PLATFORM"])
        self.assertTrue("Country" in b.extcsv["PLATFORM"])
        self.assertTrue("GAW_ID" in b.extcsv["PLATFORM"])
        self.assertTrue("Name" in b.extcsv["INSTRUMENT"])
        self.assertTrue("Model" in b.extcsv["INSTRUMENT"])
        self.assertTrue("Number" in b.extcsv["INSTRUMENT"])
        self.assertTrue("Latitude" in b.extcsv["LOCATION"])
        self.assertTrue("Longitude" in b.extcsv["LOCATION"])
        self.assertTrue("Height" in b.extcsv["LOCATION"])
        self.assertTrue("UTCOffset" in b.extcsv["TIMESTAMP"])
        self.assertTrue("Date" in b.extcsv["TIMESTAMP"])
        self.assertTrue("Time" in b.extcsv["TIMESTAMP"])
        self.assertTrue("MeteoSonde" in b.extcsv["AUXILIARY_DATA"])
        self.assertTrue("ib1" in b.extcsv["AUXILIARY_DATA"])
        self.assertTrue("ib2" in b.extcsv["AUXILIARY_DATA"])
        self.assertTrue("PumpRate" in b.extcsv["AUXILIARY_DATA"])
        self.assertTrue("BackgroundCorr" in
                        b.extcsv["AUXILIARY_DATA"])
        self.assertTrue("SampleTemperatureType" in
                        b.extcsv["AUXILIARY_DATA"])
        self.assertTrue("MinutesGroundO3" in
                        b.extcsv["AUXILIARY_DATA"])
        self.assertTrue("Pressure" in b.extcsv["PROFILE"])
        self.assertTrue("O3PartialPressure" in b.extcsv["PROFILE"])
        self.assertTrue("Temperature" in b.extcsv["PROFILE"])
        self.assertTrue("WindSpeed" in b.extcsv["PROFILE"])
        self.assertTrue("WindDirection" in b.extcsv["PROFILE"])
        self.assertTrue("LevelCode" in b.extcsv["PROFILE"])
        self.assertTrue("Duration" in b.extcsv["PROFILE"])
        self.assertTrue("GPHeight" in b.extcsv["PROFILE"])
        self.assertTrue("RelativeHumidity" in b.extcsv["PROFILE"])
        self.assertTrue("SampleTemperature" in b.extcsv["PROFILE"])

        with open(AMES_filename2) as f:
            payload = False
            counter = 0
            for line in f:
                if payload:
                    payload_list = [str(float(x)) for x in line.split()]
                    self.assertEqual(b.extcsv["PROFILE"]["Pressure"][counter], payload_list[1]) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["O3PartialPressure"][counter], payload_list[5]) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["Temperature"][counter], str(float(payload_list[3]) - 273.15)) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["WindSpeed"][counter], payload_list[7]) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["WindDirection"][counter], payload_list[6]) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["Duration"][counter], payload_list[0]) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["GPHeight"][counter], payload_list[2]) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["RelativeHumidity"][counter], payload_list[4]) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["SampleTemperature"][counter], str(float(payload_list[11]) - 273.15)) # noqa
                    self.assertEqual(b.extcsv["PROFILE"]["LevelCode"][counter], '') # noqa
                    counter += 1
                if '      s     hPa' in line:
                    payload = True

        self.assertEqual(b.extcsv["PLATFORM"]["Type"], ["STN"])
        self.assertEqual(b.extcsv["PLATFORM"]["Country"],
                         ["United States of America"])
        self.assertEqual(b.extcsv["DATA_GENERATION"]["Agency"],
                         ["NOAA-CMDL"])

    def test_vaisala(self):
        """
        Vaisala Tests
        """
        Vaisala_filename = "tests/Ozono000121_14SEG_SKBO.txt"
        Vai = load('Vaisala', Vaisala_filename, "Vaisala", "Vaisala_Agency", {"ID": "666", "SA": "Vaisala_SA", "country": "Vaisala_Country"})  # noqa

        self.assertTrue("CONTENT" in Vai.extcsv)
        self.assertTrue("DATA_GENERATION" in Vai.extcsv)
        self.assertTrue("PLATFORM" in Vai.extcsv)
        self.assertTrue("INSTRUMENT" in Vai.extcsv)
        self.assertTrue("LOCATION" in Vai.extcsv)
        self.assertTrue("TIMESTAMP" in Vai.extcsv)
        self.assertTrue("PROFILE" in Vai.extcsv)
        self.assertTrue("Class" in Vai.extcsv["CONTENT"])
        self.assertTrue("Category" in Vai.extcsv["CONTENT"])
        self.assertTrue("Level" in Vai.extcsv["CONTENT"])
        self.assertTrue("Form" in Vai.extcsv["CONTENT"])
        self.assertTrue("Date" in Vai.extcsv["DATA_GENERATION"])
        self.assertTrue("Agency" in Vai.extcsv["DATA_GENERATION"])
        self.assertTrue("Version" in Vai.extcsv["DATA_GENERATION"])
        self.assertTrue("ScientificAuthority" in
                        Vai.extcsv["DATA_GENERATION"])
        self.assertTrue("Type" in Vai.extcsv["PLATFORM"])
        self.assertTrue("ID" in Vai.extcsv["PLATFORM"])
        self.assertTrue("Name" in Vai.extcsv["PLATFORM"])
        self.assertTrue("Country" in Vai.extcsv["PLATFORM"])
        self.assertTrue("GAW_ID" in Vai.extcsv["PLATFORM"])
        self.assertTrue("Name" in Vai.extcsv["INSTRUMENT"])
        self.assertTrue("Model" in Vai.extcsv["INSTRUMENT"])
        self.assertTrue("Number" in Vai.extcsv["INSTRUMENT"])
        self.assertTrue("Latitude" in Vai.extcsv["LOCATION"])
        self.assertTrue("Longitude" in Vai.extcsv["LOCATION"])
        self.assertTrue("Height" in Vai.extcsv["LOCATION"])
        self.assertTrue("UTCOffset" in Vai.extcsv["TIMESTAMP"])
        self.assertTrue("Date" in Vai.extcsv["TIMESTAMP"])
        self.assertTrue("Time" in Vai.extcsv["TIMESTAMP"])
        self.assertTrue("Pressure" in Vai.extcsv["PROFILE"])
        self.assertTrue("O3PartialPressure" in Vai.extcsv["PROFILE"])  # noqa
        self.assertTrue("Temperature" in Vai.extcsv["PROFILE"])
        self.assertTrue("WindSpeed" in Vai.extcsv["PROFILE"])
        self.assertTrue("WindDirection" in Vai.extcsv["PROFILE"])
        self.assertTrue("LevelCode" in Vai.extcsv["PROFILE"])
        self.assertTrue("Duration" in Vai.extcsv["PROFILE"])
        self.assertTrue("GPHeight" in Vai.extcsv["PROFILE"])
        self.assertTrue("RelativeHumidity" in Vai.extcsv["PROFILE"])  # noqa
        self.assertTrue("SampleTemperature" in Vai.extcsv["PROFILE"])  # noqa

        self.assertEqual(Vai.extcsv["PROFILE"]["Pressure"][0], "753.2")
        self.assertEqual(Vai.extcsv["PROFILE"]["Pressure"][1], "747.3")
        self.assertEqual(Vai.extcsv["PROFILE"]["Pressure"][10], "692.1")
        self.assertEqual(Vai.extcsv["PROFILE"]["O3PartialPressure"][10], "2.12")  # noqa
        self.assertEqual(Vai.extcsv["PLATFORM"]["Type"], ["STN"])
        self.assertEqual(Vai.extcsv["PLATFORM"]["Country"], ["Vaisala_Country"])  # noqa
        self.assertEqual(Vai.extcsv["DATA_GENERATION"]["Agency"], ["Vaisala_Agency"])  # noqa


# main
if __name__ == '__main__':
    unittest.main()
