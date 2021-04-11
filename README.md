[![Build Status](https://github.com/woudc/woudc-formats/workflows/build%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/woudc/woudc-formats/actions)
[![Downloads this month on PyPI](https://img.shields.io/pypi/dm/woudc-formats.svg)](http://pypi.python.org/pypi/woudc-formats)
[![Latest release](https://img.shields.io/pypi/v/woudc-formats.svg)](http://pypi.python.org/pypi/woudc-formats)
[![License](https://img.shields.io/pypi/l/woudc-formats)](https://github.com/woudc/woudc-formats)

# WOUDC Format Converter 

woudc-formats is a Python package used to perform various transformations
from/to WOUDC supported formats.

Currently supported features include:

- Readers: SHADOZ, BAS, NASA AMES files, Vaisala.
- Writers: WOUDC totalozone [daily summary](https://woudc.org/archive/Summaries/TotalOzone/Daily_Summary/FileFormat_DV.txt) (master file).

### Installation Instructions

## Requirements
woudc-formats requires Python 3.6 and above.

## Dependencies
See `requirements.txt`
- [pywoudc](https://github.com/woudc/pywoudc)
- [woudc-extcsv](https://github.com/woudc/woudc-extcsv)
- [pyshadoz](https://github.com/wmo-cop/pyshadoz)

## Setup
```bash
git clone https://github.com/woudc/woudc-formats.git && cd woudc-formats
python setup.py install
```

## Usage

### Command Line Interface
```bash
usage: woudc-formats.py --format {SHADOZ, BAS, AMES-2160, Vaisala, totalozone-masterfile} --inpath PATH/FILENAME --logfile PATH/LOGFILE --loglevel {DEBUG, CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET}

Required Arguments:
    --format: indicate input file format
    --inpath: import filename and path, for SHADOZ, BAS, Vaisala, and AMES-2160
    --logfile: path to log file, including file name
    --loglevel: define log level for logging

Optional Arguments:
    --outpath: indicate the output file path, by default is 'INPATH.csv'
    --station: station name in WOUDC
    --agency: agency name in WOUDC
    --metadata: a dictionary formatted string containing some specified station metadation information
            ex: {"inst type": "ECC", "inst number": "XXXXX", "SA": "XX" , "ID" : "XXX", "country": "XXX", "GAW_ID": "XXX"}

Importance:
    For AMES-2160 format, --agency argument is required in order to process the file.
    For Vaisala format, --station and --agency in arguments and 'ID', 'GAW_ID', 'country', and 'SA' arguments in --metadata are required in order to process the file.
```

### API
```bash
usage: 
import woudc_formats
ecsv = woudc_formats.load(In_Format, InPut_File_Path, station, agency)
if ecsv is not None:
    woudc_formats.dump(ecsv, Output_file_path)

OR

import woudc_formats
with open(input_file_path) as ff
    ff.read()
ecsv = woudc_formats.loads(In_Format, s)
if ecsv is not None:
    woudc_formats.dump(ecsv, Output_file_path)

Optional Method:
woudc_formats.load(In_Format, InPut_File_Path, station, agency, metadata) : Take input file path and return ext-csv object, agency is required for AMES file and metadata is required for Vaisala, see optional arguments for Command Line Interface for more detail.
woudc_formats.loads(In_Format,String_of_InPut_file, station, agency, metadata) : Take string represenataion of input file and return ext-csv object. Station and agency are required for AMES file and metadata is required for Vaisala, see optional arguments for Command Line Interface for more detail.
woudc_formats.dump(ecsv, Output_file_path) : Take ext-csv object and produce output file.
woudc_formats.dumps(ecsv) : Take ext-csv object and prints to screen.
```
### Example
```bash
woudc-formats.py --format SHADOZ --inpath ./bin/SAMPLE.dat --outpath ./bin/SAMPLE.csv --logfile ./bin/LOG.log --loglevel DEBUG
woudc-formats.py --format totalozone-masterfile --inpath <full local or web path to totalozone snapshot> --outpath <output path> --loglevel <log level> --logfile <log file>
woudc-formats.py --format AMES-2160 --inpath <full local path to AMES file> --outpath <output path> --loglevel <log level> --logfile <log file> --agency XXX --metadata '{"SA": "XXX", "inst type": "ECC", "inst number": "6A3412"}'

For Agency 'AWI-NM':
woudc-formats.py --format AMES-2160 --inpath INPATH --logfile log.log --loglevel DEBUG --agency AWI-NM
```
