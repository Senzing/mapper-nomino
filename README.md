# mapper-nomino

## Overview

This mapper converts Nomino data csv files into json files ready to load into Senzing. You can purchase Nomino data at [https://www.nominodata.com]

Full Usage:

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

Typical Use:

```console
python3 nomino-mapper.py -i input/riskcodeWL.csv -o output/riskcode-WL.json
```

- You can add the -l parameter to get stats and examples of the mapped file.
- You can add the -d parameter to change the data source code from the default. You might want to do this if you want to have a different data source code for each Nomino risk code.

Configuring Senzing:

Go into the G2ConfigTool.py and add the data source code(s) you decide to use.

```console
root@995af99a4c9e:/opt/senzing/g2/python# G2ConfigTool.py

Welcome to the Senzing configuration tool! Type help or ? to list commands

(g2cfg) addDataSource NOMINO

Data source successfully added!

(g2cfg) save

Are you certain you wish to proceed and save changes? (y/n) y

Configuration changes saved!


Initializing Senzing engines ...

(g2cfg) quit

```

You are now ready to load the json output file into Senzing using your desired method!

[https://www.nominodata.com]: https://www.nominodata.com/
