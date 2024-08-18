# Oceanic Water Intrusion Identification in Bedford Basin (BB), NS, using the BB Monitoring Program dataset

This project represent the reproduction of my MSc Thesis code orignally developed in MATLAB. The tools you will
find in this repository allow you to download the profile data from the BBMP dataset, manipulate and transform the 
data, and analyze the profiles to identify oceanic water intrusion events into BB, NS. 

Data Source: https://www.bio.gc.ca/science/monitoring-monitorage/bbmp-pobb/bbmp-pobb-en.php

For more information check my paper here: http://hdl.handle.net/10222/83180

## Folders and Tables

```bash
Legend
- Connected tables (@)
- Processed data Schema (*)
- Source Code (SRC)
- Aggregated BBMP CSV files (RAW)
- Normlaized and Conformed profile data (PROCESSED)
- Connection Labels (→)

Intrusion_Redo/
├── SRC/
│   ├── get_BBMP_csv.sh
│   ├── ETL_processes.py
│   ├── Intrusion_analysis.py
├── DATA/
│   ├── RAW/
|   │   ├── @metadata_csv (-> Update)
│   ├── PROCESSED/
|   │   ├── TABLES/
|   |   │   ├── @coefficients_error (-> Error)
|   |   │   ├── @coefficients (-> ID/Error)
|   |   │   ├── @intrusionID+effect (-> ID)
|   |   │   ├── @metadata_intrusions (-> ID/Normal)
|   │   ├── @metadata_processing (-> Normal/Update)
|   │   ├── *processed_data
│   ├── Data Documentation.md
├── requirements.txt
├── Documentation.md
├── .gitignore
└── .pylintrc
```

## Usage

### Data Ingestion from BBMP
- Description: Download data from the BBMP fts server. With profiles of oceanographic 
variables measured at the Compass Bouy

```bash
./get_BBMP_csv.sh
```

- It will ask for a `password`. Just `Press Enter`.
- It wil download the file to `RAW/` as `bbmp_aggregated_profiles.csv`

### ETL data manipulation
- Description: Extraction of variables of interest. Normalizes profiles’ depths and 
fills with NaNs. Integrate profile arrays into a matrix where the dimensions correspond to depth 
and date of measurements. Horizontal and vertical interpolation of profiles. And calculation 
of depth range averages for identification of deep and mid-depth intrusions.

```bash
usage: python ETL_processess.py [-h] [--file_name FILE_NAME] [--deep_depth DEEP_DEPTH] 
                                [--mid_depths MID_DEPTHS][--date_format DATE_FORMAT] 

optional arguments:
  -h, --help                Show this help message and exit
  --file_name FILE_NAME     Default is 'bbmp_aggregated_profiles.csv'. Make sure
                            the file name exists in RAW/
  --deep_depth DEEP_DEPTH   Default is 60. This will calculate depth averages
                            from 60m to the bottom of BB (~70m) 
  --mid_depths MID_DEPTHS   Default is list(20,35). This will calculate depth averages
                            from 20m to 35m
  --date_format DATE_FORMAT Default is %Y-%m-%d %H:%M:%S. This is mainly used to read
                            dates from raw data
```

- I wil create the output named `BBMP_salected_data0.pkl` at '/data/PROCESSED/'
- It will record metadata in `metadata_processing.csv` at '/data/PROCESSED/' 

### Intrusion Identification

- Manual Identification: Allows you to identify these events using data from
the Bedford Basin Monitoring Program (BBMP) station, which collects oceanographic
variables at different depths of the water column. For identification only
temperture and salinity are used, however, future updates will include oxygen at least.
Using the intrusions you identified, the script will determine the best possible
coefficients for automated intrusion identification using the variables provided
(i.e., temperature and salinity), and it will also provide the performance of these
coefficients

- Automated Idetification: Allows you to skip the manual identification and focus on
testing different coefficients and their performance against a set of intrusions
that were manually identified and saved in file fromat(.pkl)

```bash
usage: python Intrusion_analysis.py [-h] [--intrusion_type INTRUSION_TYPE] [--ID_type ID_TYPE] 
                                    [--manual_type MANUAL_TYPE] [--coefficients COEFFICIENTS] 
                                    [--save_manual SAVE_MANUAL] [--manual_input MANUAL_INPUT] 
                                    file_name

positional arguments:
  file_name             Processed file to be used. Make sure file is available
                        at '/data/PROCESSED/'
                        Example: 'BBMP_salected_data_test.pkl'

optional arguments:
  -h, --help                        Show this help message and exit
  --intrusion_type INTRUSION_TYPE   Default is 'Normal'. The prupose of this is so that the
                                    script known what depth average to use. There are 3 types
                                    of intrusion events: Normal (causing increases in both
                                    salinity and temoperature), MID (seen at
                                    mid_depth), and Reverse (causing decreases in temperature
                                    at depth). Both normal and reverse use deep depths,
                                    and MID uses mid depths. 
  --ID_type ID_TYPE                 Default is 'MANUAL'. This allows you to select between
                                    manual and automated intrusion identification
  --manual_type MANUAL_TYPE         Default is 'MANUAL'. This allows you to select between
                                    manually selecting intrusions from plots, or importing
                                    a file of intrusion datetimes (OTHER)
  --coefficients COEFFICIENTS       Default is list(0.5,0.5). These numbers represent the
                                    changes in temperature and salinity that will be used
                                    to flag an intrusion events. Only required for Automatic
                                    identification
  --save_manual SAVE_MANUAL         Default is 'OFF'. This allows you to save (ON) or not
                                    the intrusion you manually identified using the plots
  --manual_input MANUAL_INPUT       Default is 'manual_intrusions_all_noO2.pkl'. This is
                                    the file of intrusion datetimes if MANUAL_TYPE = OTHER.
                                    Make sure this file is available at '/data/PROCESSED/' 
```

### Examples
- TBD

## Notes
- TBD