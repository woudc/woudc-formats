[![Build Status](https://github.com/woudc/woudc-formats/workflows/build%20%E2%9A%99%EF%B8%8F/badge.svg)](https:    //github.com/woudc/woudc-formats/actions)

# WOUDC Format Converter 

woudc-formats is a Python package used to perform various transformations
from/to WOUDC supported formats.

Currently supported features include:

- Readers: SHADOZ, BAS, NASA AMES files, Vaisala
- Writers: WOUDC totalozone [daily summary](https://woudc.org/archive/Summaries/TotalOzone/Daily_Summary/FileFormat_DV.txt) (master file)

## Installation

### pip

Install latest stable version from [PyPI](https://pypi.org/project/woudc-formats).

```bash
pip3 install woudc-formats
```

### From source
Install latest development version.

```bash
python3 -m venv woudc-formats
cd woudc-formats
. bin/activate
git clone https://github.com/woudc/woudc-formats.git
cd woudc-formats
pip3 install .
```

## Usage

### Command Line Interface
```bash
usage: woudc-formats --format {SHADOZ, BAS, AMES-2160, Vaisala, totalozone-masterfile} --inpath PATH/FILENAME --logfile PATH/LOGFILE --loglevel {DEBUG, CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET}

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
```

Note:
- For AMES-2160 format, --agency argument is required in order to process the file.
- For Vaisala format, --station and --agency in arguments and 'ID', 'GAW_ID', 'country', and 'SA' arguments in --metadata are required in order to process the file.
```

### API
```bash
import woudc_formats

# load from file
ecsv = woudc_formats.load(In_Format, InPut_File_Path, station, agency)

if ecsv is not None:
    woudc_formats.dump(ecsv, Output_file_path)

# load from string
with open(input_file_path) as ff
    ff.read()
ecsv = woudc_formats.loads(In_Format, s)
if ecsv is not None:
    woudc_formats.dump(ecsv, Output_file_path)
```

### Running
```bash
woudc-formats --format SHADOZ --inpath ./bin/SAMPLE.dat --outpath ./bin/SAMPLE.csv --logfile ./bin/LOG.log --loglevel DEBUG
woudc-formats --format totalozone-masterfile --inpath <full local or web path to totalozone snapshot> --outpath <output path> --loglevel <log level> --logfile <log file>
woudc-formats --format AMES-2160 --inpath <full local path to AMES file> --outpath <output path> --loglevel <log level> --logfile <log file> --agency XXX --metadata '{"SA": "XXX", "inst type": "ECC", "inst number": "6A3412"}'

For Agency 'AWI-NM':
woudc-formats --format AMES-2160 --inpath INPATH --logfile log.log --loglevel DEBUG --agency AWI-NM
```

## Development

```bash
python3 -m venv woudc-formats
cd woudc-formats
source bin/activate
git clone https://github.com/woudc/woudc-formats.git
cd woudc-formats
pip3 install .
pip3 install ".[dev]"
```

### Running Tests

```bash
python3 tests/test.py
```

## Releasing

```bash
# create release (x.y.z is the release version)
vi pyproject.toml  # update [project]/version
git commit -am 'update release version x.y.z'
git push origin master
git tag -a x.y.z -m 'tagging release version x.y.z'
git push --tags

# upload to PyPI
rm -fr build dist *.egg-info
python3 -m build
twine upload dist/*

# publish release on GitHub (https://github.com/woudc/woudc-formats/releases/new)

# bump version back to dev
vi pyproject.toml  # update [project]/version
git commit -am 'back to dev'
git push origin master
```

### Code Conventions

woudc-formats code conventions are as per
[PEP8](https://www.python.org/dev/peps/pep-0008).

```bash
# code should always pass the following
find -type f -name "*.py" | xargs flake8
```

## Issues

All bugs, enhancements and issues are managed on
[GitHub](https://github.com/woudc/woudc-formats/issues).

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)
* [Thinesh Sornalingam](https://github.com/thineshsornalingam)
