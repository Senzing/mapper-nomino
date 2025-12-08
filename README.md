# mapper-nomino

## Overview

This mapper converts Nomino data CSV files into JSONL files ready to load into Senzing. You can purchase Nomino data at [https://www.nominodata.com](https://www.nominodata.com/)

## Prerequisites

- Python 3.9 or higher

## Usage

```console
python3 src/nomino_mapper.py --help
usage: nomino_mapper.py [-h] [-i INPUT_FILE] [-o OUTPUT_FILE] [-d DATA_SOURCE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
                        the name of the input file
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        the name of the output file
  -d DATA_SOURCE, --data_source DATA_SOURCE
                        the data source code to use, default="NOMINO"
```

### Example

```console
python3 src/nomino_mapper.py -i input/nomino_data.csv -o output/nomino_data.jsonl
```

Add the `-d` parameter to change the data source code from the default. You might want to do this if you want to have a different data source code for each Nomino risk code.

## Mapping Details

Nomino data often contains multiple CSV rows for the same entity, each with different attributes (e.g., separate rows for different addresses or aliases). This mapper groups rows into a single Senzing record by computing a hash of key fields: `Source`, `OriginalID`, `namefull`, `Title`, and `page_URL`. Rows with the same hash are combined, with their attributes merged into the output record.

The mapper also determines the record type (PERSON, ORGANIZATION, VESSEL, or AIRCRAFT) based on the `type` field and other indicators in the data.

See [src/nomino_mapper.py](src/nomino_mapper.py) for field mapping logic. The code is designed to be readable and self-documenting.

## Configuring Senzing

Run the [src/nomino_config.g2c](src/nomino_config.g2c) file with the Senzing configuration tool to add the data source.

If you override the default data source code with the `-d` parameter, update the `.g2c` file accordingly.

## Testing

```console
pytest tests/
```
