# **Oceanic Water Intrusion Identification in Bedford Basin (BB), NS**  
This repository contains the code for reproducing the analysis from my MSc thesis, originally developed in MATLAB. It enables downloading, transforming, and analyzing oceanographic profile data from the **Bedford Basin Monitoring Program (BBMP)** dataset to identify oceanic water intrusion events.  

**Data Source:**  
[BBMP Dataset](https://www.bio.gc.ca/science/monitoring-monitorage/bbmp-pobb/bbmp-pobb-en.php)  
**Thesis Paper:**  
[Access my paper here](http://hdl.handle.net/10222/83180)  

---

## **Repository Structure**
——-NOT UP-TO-DATE
```bash
Legend:
- Connected tables (@)
- Connection Labels (→) *Not implemented

SRC/
├── data/
│   ├── RAW/
│   │   ├── get_ftp_BBMP.sh        # Download raw BBMP data
│   ├── PROCESSED/
│   │   ├── TABLES/
│   │   │   ├── @coefficients_error.csv  (→ Error)
│   │   │   ├── @coefficients.csv        (→ ID/Error)
│   │   │   ├── @intrusionID+effect.csv  (→ ID)
│   │   │   ├── @metadata_intrusions.csv (→ ID/Normal)
│   │   ├── @metadata_processing.csv     (→ Normal)
├── ETL_processes/
├── Factory_method/     # Under Construction
├── Intrusion_analysis/
├── Intrusion_identification/
├── config.py
├── main_analysis.py
├── main_etl.py
└── plot_intrusions.py
```

---

## **Installation and Setup**  

Ensure Python is installed on your machine. Use the following command to install required dependencies:

```bash
pip install -r requirements.txt
```

---

## **Usage Guide**  

### **1. Data Ingestion from BBMP**

Download the latest data from the BBMP FTP server:

```bash
./get_BBMP_csv.sh
```
- Press `Enter` if asked for a password.
- The downloaded file will be saved as `bbmp_aggregated_profiles.csv` in `data/RAW/`.

---

### **2. ETL Data Manipulation**  

Extract variables of interest, normalize depths, perform interpolation, and compute depth averages for identifying intrusions.

```bash
usage: python -m main_etl [-h] [--file_name FILE_NAME] 
                          [--deep_depth DEEP_DEPTH] [--mid_depths_top MID_DEPTHS_TOP]
                          [--mid_depths_bottom MID_DEPTHS_BOTTOM] [--date_format DATE_FORMAT]
```

- Example Command:
```bash
python -m main_etl --mid_depths_top 15
```
This will generate:
- **Processed Data:** `BBMP_selected_data0.pkl` in `/data/PROCESSED/`
- **Metadata:** `metadata_processing.csv` in `/data/PROCESSED/`

---

### **3. Intrusion Identification**  

**Manual Identification:**  
Identify intrusion events using the collected temperature and salinity data. The program determines coefficients for automated intrusion detection and evaluates their performance.

**Imported Identification:**  
Test predefined coefficients using previously identified intrusions.

```bash
usage: python -m main_analysis [-h] [--file_name FILE_NAME] [--intrusion_type INTRUSION_TYPE]
                               [--ID_type ID_TYPE] [--analysis_type ANALYSIS_TYPE]
                               [--coefficient_temp COEFFICIENT_TEMP] 
                               [--coefficient_salt COEFFICIENT_SALT] 
                               [--save_manual SAVE_MANUAL] [--manual_input MANUAL_INPUT]
```

#### **Examples**  

1. **Analyze Imported Intrusions:**  
```bash
python -m main_analysis --ID_type IMPORTED --analysis_type USE_COEFFICIENTS 
                        --coefficient_temp 0.7 --coefficient_salt 0.02
```

2. **Manual Identification of MID Intrusions:**  
```bash
python -m main_analysis --intrusion_type MID --save_manual ON
```

---

### **4. Intrusion Plotting**  

Generate time-depth plots to visualize intrusion events based on temperature and salinity data.

```bash
usage: python -m plot_intrusions [-h] [--file_name FILE_NAME] 
                                 [--initial_yr I_YR] [--final_yr F_YR]
                                 [--datetimes DTM] 
```

- Example Command:
```bash
python -m plot_intrusions --initial_yr 2010 --final_yr 2015
```

This will generate a figure with:
1. Time-Depth Temperature Plot
2. Time-Depth Salinity Plot
3. Depth-Averaged Changes in Temperature, Salinity, and Oxygen  

---

## **Examples**

### **ETL Data Manipulation Example**  
This example processes raw BBMP data and computes depth averages.  
```bash
python -m main_etl --mid_depths_top 15
```

### **Intrusion Identification Example**  
Analyze imported intrusions using specific coefficients for temperature and salinity.  
```bash
python -m main_analysis --ID_type IMPORTED --coefficient_temp 0.7 --coefficient_salt 0.02
```

### **Intrusion Plotting Example**  
Plot intrusion events between 2010 and 2015.  
```bash
python -m plot_intrusions --initial_yr 2010 --final_yr 2015
```

---

## **Notes**  
- **Under Construction:**
  - `Factory_method/`
  - `--datetimes` parameter in `plot_intrusions.py`
  - step_cache and Database

---

## **License**  
This project is licensed under the MIT License - see the `LICENSE` file for details.

---

## **Acknowledgements**  
- Special thanks to the BBMP team for providing the dataset.  
- [Link to BBMP Dataset](https://www.bio.gc.ca/science/monitoring-monitorage/bbmp-pobb/bbmp-pobb-en.php)  