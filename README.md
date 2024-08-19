# Oceanic Water Intrusion Identification in Bedford Basin (BB), NS, using the BB Monitoring Program dataset

This project represent the reproduction of my MSc Thesis code orignally developed 
in MATLAB. The tools you will find in this repository allow you to download the 
profile data from the BBMP dataset, manipulate and transform the data, and analyze 
the profiles to identify oceanic water intrusion events into BB, NS. 

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
- Description: Extraction of variables of interest. Normalizes profiles’ depths 
and fills with NaNs. Integrate profile arrays into a matrix where the dimensions 
correspond to depth and date of measurements. Horizontal and vertical interpolation 
of profiles. And calculation of depth range averages for identification of deep 
and mid-depth intrusions.

```bash
usage: python -m ETL_processess [-h] [--file_name FILE_NAME] [--deep_depth DEEP_DEPTH] 
	                              [--mid_depths_top MID_DEPTHS_TOP][--mid_depths_bottom MID_DEPTHS_BOTTOM]
	                              [--date_format DATE_FORMAT] 

optional arguments:
  -h, --help                	            Show this help message and exit
  --file_name FILE_NAME     	            Default is 'bbmp_aggregated_profiles.csv'. Make sure
                            	            the file name exists in RAW/
  --deep_depth DEEP_DEPTH   	            Default is 60. This will calculate depth averages
                            	            from 60m to the bottom of BB (~70m) 
  --mid_depths_top MID_DEPTHS_TOP         Default is 20. This will calculate depth averages
                            	            from 20m to MID_DEPTH_BOTTOM
  --mid_depths_bottom  MID_DEPTHS_BOTTOM  Default is 35. This will calculate depth averages
                                          from MID_DEPTH_TOP to 35m
  --date_format DATE_FORMAT 	            Default is %Y-%m-%d %H:%M:%S. This is mainly used to read
                                          dates from raw data
```

- It wil create the output named `BBMP_salected_data0.pkl` at '/data/PROCESSED/'
- It will record metadata in `metadata_processing.csv` at '/data/PROCESSED/' 

### Intrusion Identification

- Manual Identification: Allows you to identify these events using data from
the Bedford Basin Monitoring Program (BBMP) station, which collects oceanographic
variables at different depths of the water column. For identification only
temperture and salinity are used, however, future updates will include oxygen at 
least. Using the intrusions you identified, the script will determine the best 
possible coefficients for automated intrusion identification using the variables 
provided (i.e., temperature and salinity), and it will also provide the performance 
of these coefficients

- Imported Idetification: Allows you to focus on testing different coefficients and 
their performance against a set of intrusions that were manually identified and saved 
in file fromat(.pkl)

```bash
usage: python -m Intrusion_analysis [-h] [--intrusion_type INTRUSION_TYPE] [--ID_type ID_TYPE] 
                                    [--analysis_type ANALYSIS_TYPE] [--coefficient_temp COEFFICIENT_TEMP] 
                                    [--coefficient_salt COEFFICIENT_SALT] [--save_manual SAVE_MANUAL] 
                                    [--manual_input MANUAL_INPUT] file_name

positional arguments:
  file_name             Processed file to be used. Make sure file is available
                        at '/data/PROCESSED/'. Default is 'BBMP_salected_data_test.pkl'

optional arguments:
  -h, --help                                 Show this help message and exit
  --intrusion_type INTRUSION_TYPE            Default is 'Normal'. The prupose of this is so that the
                                             script known what depth average to use. There are 3 types
                                             of intrusion events: Normal (causing increases in both
                                             salinity and temoperature), MID (seen at
                                             mid_depth), and Reverse (causing decreases in temperature
                                             at depth).OPTIONS: MID, NORMAL, REVERSE. 
  --ID_type ID_TYPE                          Default is 'MANUAL'. This allows you to select between
                                             manual and imported intrusion identification. 
                                             OPTIONS: MANUAL and IMPORTED
  --analysis_type ANALYSIS_TYPE              Default is 'GET_COEFFICIENTS'. OPTIONS: GET_COEFFICIENTS
                                             (coefficients estimation) and USE_COEFFICIENTS (coeffients
                                             are provided by the user)
  --coefficient_temp COEFFICIENTS_TEMP       Default is 0.5. This number represents the change in 
					                                   temperature that will be used to flag an intrusion events.
  --coefficient_salt COEFFICIENTS_SALT       Default is 0.5. This number represents the change in 
					                                   salinity that will be used to flag an intrusion events.  
  --save_manual SAVE_MANUAL                  Default is 'OFF'. This allows you to save (ON) or not
                                             the intrusion you manually identified using the plots
  --manual_input MANUAL_INPUT                Default is 'manual_intrusions_all_noO2.pkl'. This is
                                             the file of intrusion datetimes if MANUAL_TYPE = OTHER.
                                             Make sure this file is available at '/data/PROCESSED/' 
```

### Examples
- This example allows you to import manual_intrusions_all_noO2.pkl (DEFAULT) and analyse the performance 
of the script, using the coefficients [0.7, 0.02], at identifing NORMAL intrusion events. 
```bash
python -m Intrusion_analysis --ID_type IMPORTED --analysis_type USE_COEFFICIENTS --coefficient_temp 0.7 --coefficient_salt 0.02 
```

- This example allows you to manually identify (DEFAULT) MID intrusion events, save these intrusions, and 
estimate the best coefficients for automatic intrusion identification (DEFAULT).
```bash
python -m Intrusion_analysis --intrusion_type MID --save_manual ON 
```
## Notes
- TBD