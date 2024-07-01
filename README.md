# Intrusion_Redo

This project represent the reproduction of my MSc Thesis code orignally developed in Matlab

## Folders and Tables

## Legend
- Connected tables (@)
- Processed data Schema (*)
- Source Code (SRC)
- Aggregated BBMP CSV files (RAW)
- Normlaized and Conformed profile data (PROCESSED)
- Connection Labels (→)

- Intrusion_Redo/
- ├── SRC/
- │   ├── get_BBMP_csv.sh
- │   ├── ETL_processes.py
- │   ├── Intrusion_analysis.py
- ├── DATA/
- │   ├── RAW/
- |   │   ├── @metadata_csv (-> Update)
- │   ├── PROCESSED/
- |   │   ├── TABLES/
- |   |   │   ├── @coefficients_error (-> Error)
- |   |   │   ├── @coefficients (-> ID/Error)
- |   |   │   ├── @intrusionID+effect (-> ID)
- |   |   │   ├── @metadata_intrusions (-> ID/Normal)
- |   │   ├── @metadata_processing (-> Normal/Update)
- |   │   ├── *processed_data
- │   ├── Data Documentation.md
- ├── requirements.txt
- ├── Documentation.md
- ├── .gitignore
- └── .pylintrc

## Scripts

### get_BBMP_csv.sh
- Download data from the BBMP fts server. With profiles of oceanographic variables measured at the Compass Bouy

- How to use?

./get_BBMP_csv.sh

- Notes:
It will ask for a password. Just Press Enter.

### ETL_processes.py
- Extraction of variables on interest

- Normalize profiles’ depths and fill with NaNs. Integrate profile arrays into a matrix the dimensions correspond to depth and date of measurements.

- Horizontal and vertical interpolation of profiles. And calculation of depth range averages for identification of deep and mid-depth intrusions.

- How to use?

* ETL_processes {CSV file name} {Depth} {Depth Range}  {Output dataset name} *****TBD
           or
* python ETL_processes.py

│Where:
- Depth: Corresponds to the minimum depth at which deep depth averages will be calcualted. aka
- Depth averages = avg effects calculated from Depth to Maximum Depth (~70m).
- Depth Range: Corresponds to the minimum depth at which mid depth averages will be calcualted. aka. Depth averages = avg effects calculated from depth1 to depth2.

### Intrusion_analysis.py
- Manual identification of intrusion from the PROCESSED data. Recording both the time and the effects of these events. After identification, the coeficients that best match the manually selected intrusions are estimated through an optimization function that minimizes the number of extra and the number of missed intrusions based on your selection. A “true” intrusion data file, selected by the master user, is available and can be contested by the other users other than the master user. 

- Automated identification involves the identification of intrusion events only based on coefficients for chnages in temperature and salinity. They can be calculated using the using manual identificastion and chosing to import a data file rather than selecting the intrusion events yourself.

- How to use?

* Manual Identification
* Intrusion_analysis {Input dataset name} {Intrusion Type} {Indetification type} {Manual Type} *****TBD
          or
* python Intrusion_analysis.py

* Automated identification
* Intrusion_analysis {Input dataset name} {Intrusion Type} {Indetification type} {Coefficients}
*****TBD

│Where:
- Intrusion Type: Choose between 'deep', 'mid', and 'inverse"
- Indentification Type: Choose between 'manual' and 'auto'
- Manual type: Choose between 'selected' and 'imported'

## Notes
- Other functions should be added to account for extracting specific data from tables and converting this Python dataset into and SQL dataset. However this will come once there are many users and long tables. 