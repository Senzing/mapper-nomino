# mapper-nomino

## Overview

This mapper converts Nomino data CSV files into JSONL files ready to load into Senzing. You can purchase Nomino data at [https://www.nominodata.com](https://www.nominodata.com/)

## Prerequisites

- Python 3.9 or higher
- [Senzing/mapper-base](https://github.com/Senzing/mapper-base)

## Installation

Place the mapper files in a common directory structure with mapper-base:

```console
/senzing/mappers/mapper-base
/senzing/mappers/mapper-nomino
```

Set the PYTHONPATH to include mapper-base:

```console
export PYTHONPATH=$PYTHONPATH:/senzing/mappers/mapper-base
```

## Usage

```console
python3 nomino-mapper.py --help
usage: nomino-mapper.py [-h] [-i INPUT_FILE] [-o OUTPUT_FILE] [-l LOG_FILE] [-d DATA_SOURCE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
                        the name of the input file
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        the name of the output file
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file
  -d DATA_SOURCE, --data_source DATA_SOURCE
                        the data source code to use, default="NOMINO"
```

### Example

```console
python3 nomino-mapper.py -i input/nomino_data.csv -o output/nomino_data.jsonl
```

- Add the `-l` parameter to get stats and examples of the mapped file.
- Add the `-d` parameter to change the data source code from the default. You might want to do this if you want to have a different data source code for each Nomino risk code.

## Configuring Senzing

Run the [nomino_config_updates.g2c](nomino_config_updates.g2c) file with the Senzing configuration tool to add the data source.

If you override the default data source code with the `-d` parameter, update the `.g2c` file accordingly.
