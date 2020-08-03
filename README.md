# mapper-nomino

## Overview

The mapping files in this project are for use with [Senzing/mapper-csv](https://github.com/Senzing/mapper-csv). Complete instructions on using these mapping files can be found under the mapper-csv repository. 

The mappers are located in the mappings folder. Those currently available are:

- [Human Trafficking (HT)](mappings/Nomino_HT-map.json)
- [Marijuana Related Businesses (MRB)](mappings/Nomino_MRB-map.json)
- [Pharma Risk (PR)](mappings/Nomino_PR-map.json)

To use these mappers with the [Senzing/mapper-csv](https://github.com/Senzing/mapper-csv) utility apply the included [Senzing configuration](config/nomino_config.g2c). This is applied using the G2ConfigTool.py utility, located in the /python/ path of your Senzing API deployment. 

    ```console
    cd <senzing_root>/python/
    ./G2ConfigTool.py <nomino_repository>/config/nomino_config.g2c
    ```

Basic example command using mapper-csv:

    ```console
    cd <git_repos>/mapper-csv
    ./csv_mapper.py -m ../mapper-nomino/mappings/Nomino_HT-map.json -i <data_path>/ht_active_072420.csv -o <data_path>ht_active_072420-mapped.json
    ```

This is a community site, we welcome additional mapping files as users map additional Nomino feeds.

