'''This code represents the translation of my master thesis work
from MATLAB to Python. The code allows you to identify ocean water
intrusion events in Bedford Basin (BB), Halifax, NS.

Halifax Habour is composed of 3 main sections, the Outer Harbour,
the Narrows and Bedford Basin. The Outer Harbour is dirrectly connected
to the North Atlantic Ocean and it's connected to BB through a shallow
and narrow channel called the Narrows. For comparison, The Narrows
is about 20m deep in average, and BB is bowl shaped and reaches depths
of about 70m

Intrusion events are water parcels from the Outer Harbour that climb
the slope from the open ocean to the Narrows and then spill into BB
The reason why we care is because these events affect BB stratification,
nutrient concentrations, oxygen concentrations at depths, and more.

This code allows you to identify intrusion events both MANUALLY and
and AUTOMATICALLY.

MANUAL Identification: Allows you to identify these events using data from 
the Bedford Basin Monitoring Program (BBMP) station, which collects oceanographic
variables at different depths of the water column. For identification only 
temperture and salinity are used, however, future updates will include oxygen at least.
Using the intrusions you identified, the script will determine the best possible
coefficients for automated intrusion identification using the variables provided
(i.e., temperature and salinity), and it will also provide the performance of these
coefficients

AUTOMATED Indetification: Allows you to skip the manual identification and focus on
testing different coefficients and their performance against a set of intrusions
that were manually identified and saved in file fromat(.pkl)

All metadata data lineage documentation colleted from automated and manual intrusion events are 
recorded in .csv files located in the DATA/ directory.

Future Funtinalities: MySQL database, Apache Airflow, and Distribution to compile
data from multiple Users

Data Source: https://www.bio.gc.ca/science/monitoring-monitorage/bbmp-pobb/bbmp-pobb-en.php

For more information check my paper here: http://hdl.handle.net/10222/83180

 '''


# Imports
import joblib
import os
import time
import csv
import argparse
import sys
from datetime import datetime,timedelta
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from scipy.optimize import minimize



def get_command_line_args() -> str:
    file_name = 'BBMP_salected_data_test.pkl'
    intrusion_type = 'Normal'
    ID_type = 'MANUAL'
    manual_type='MANUAL'
    coefficients=[0.5, 0.5]
    save_manual='OFF'
    manual_input='manual_intrusions_all_noO2.pkl'
    # Command line arguments
    parser = argparse.ArgumentParser(description='Arguments')
    parser.add_argument('file_name', type=str, help="""TBD""", default=file_name)
    parser.add_argument('--intrusion_type', type=str, help='TBD')
    parser.add_argument('--ID_type', type=str, help='TBD')
    parser.add_argument('--manual_type', type=str, help='TBD')
    parser.add_argument('--coefficients', type=list[int], help='TBD')
    parser.add_argument('--save_manual', type=str, help='TBD')
    parser.add_argument('--manual_input', type=str, help='TBD')

    # Parse and read arguments and assign them to variables if exists
    args, _ = parser.parse_known_args()

    # Check if no arguments were provided and print help if so
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    data_name = file_name
    if args.file_name:
        data_name = args.file_name

    int_type = intrusion_type
    if args.intrusion_type:
        int_type = args.intrusion_type

    idtype = ID_type
    if args.ID_type:
        idtype = args.ID_type

    man_type = manual_type
    if args.manual_type:
        man_type=args.manual_type

    coeff = coefficients
    if args.coefficients:
        coeff=args.coefficients

    s_manual = save_manual
    if args.save_manual:
        s_manual=args.save_manual

    i_manual = manual_input
    if args.manual_input:
        i_manual=args.manual_input

    return data_name, int_type, idtype, man_type, coeff, s_manual, i_manual



def save_joblib(data:any,file_name: str) -> any:
    file_path = '../DATA/PROCESSED/' + file_name
    joblib.dump(data, file_path)
    


def import_joblib(file_path: str) -> any:
    data: any = joblib.load(file_path)
    return data



def timestamp2datetime_lists(lst:list[int]) -> list[datetime]:
    '''Takes a list of timestapms and converst it to a list of datetimes'''
    datetime_list:list[datetime] = [datetime.fromtimestamp(ts) for ts in lst]
    return datetime_list



def separate_yearly_dates(datetime_list:list[datetime]) -> dict[list]:
    '''Separates a list of datetimes and separates them by year by separating indices'''
    years_extracted:list = np.unique([dt.year for dt in datetime_list])

    grouped_years:dict[list] = {year: [] for year in years_extracted}
    for i in datetime_list:
        grouped_years[i.year].append(i)

    return grouped_years, years_extracted



def create_yearly_matrices(selected_data:dict, year_indices:dict[list]) -> dict[dict]:
    '''Use separated indices by year and create matrices with the actual data for plotting'''
    Temp_dataframe = selected_data['sample_matrix_temp']
    Salt_dataframe = selected_data['sample_matrix_salt']
    
    selected_data_temp = Temp_dataframe.to_numpy()
    selected_data_salt = Salt_dataframe.to_numpy()

    yearly_profiles_temp: dict = {}
    yearly_profiles_salt: dict = {}

    for year, indices in year_indices.items():
        yearly_profile_temp = selected_data_temp[:, indices]
        yearly_profiles_temp[year] = yearly_profile_temp

        yearly_profile_salt = selected_data_salt[:, indices]
        yearly_profiles_salt[year] = yearly_profile_salt

    return yearly_profiles_temp, yearly_profiles_salt



points = []   # Creates list to save the points selected
def onclick(event):
    '''Allows you to select point from plots'''
    if event.button == 1:
        if event.inaxes is not None:
            x, y = event.xdata, event.ydata

            points.append((x, y))

            print(f"Point selected: ({x}, {y})")
            event.inaxes.plot(x, y, 'ro')
            event.canvas.draw()



def onkey(event):
    '''Terminates point selection stage'''
    if event.key == ' ':
        plt.close(event.canvas.figure)



def get_points():
    return points



class intrusions:
    '''Creates an Object representing a specific intrusion identification strategy'''

    # Internal Variables
    lin = "-"*6+' '
    dates_error = 10
    OF_range = [-1, 1]
    dates_name = 'sample_timestamps'
    depth_name = 'sample_depth'
    temp_range = [0,10]
    salt_range = [30.5,31.5]
    oxy_range = [0,12]
    bottom_avg_names = ['sample_diff_row_temp', 'sample_diff_row_salt']
    mid_avg_names = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt']
    meta_path = '../DATA/PROCESSED/TABLES/'
    coeff_error_table = 'coefficients_error.csv'
    coeff_table = 'coefficients.csv'
    intrusions_table = 'intrusionID+effect.csv'
    meta_table = 'metadata_intrusions.csv'

    def __init__(self, PATH) -> None:
        self.metadata_intrusions = {}
        self.table_IDeffects = {}
        self.table_coefficients = {}
        self.table_coefficients_error = {}
        self.table_coefficients_error_comb = {}

        self.manualID_dates = []

        print(self.lin+'Importing Data')
        self.metadata_intrusions['Input_dataset'] = PATH
        stat_info = os.stat(PATH)
        self.metadata_intrusions['Date_created'] = time.ctime(stat_info.st_birthtime)

        self.data = import_joblib(PATH)


    def get_command_line_args(self):
        file_name = 'BBMP_salected_data_test.pkl'
        intrusion_type = 'Normal'
        ID_type = 'MANUAL'
        manual_type='MANUAL'
        coefficients=[0.5, 0.5]
        save_manual='OFF'
        manual_input='manual_intrusions_all_noO2.pkl'
        # Command line arguments
        parser = argparse.ArgumentParser(description='Arguments')
        parser.add_argument('file_name', type=str, help="""TBD""", default=file_name)
        parser.add_argument('--intrusion_type', type=str, help='TBD')
        parser.add_argument('--ID_type', type=str, help='TBD')
        parser.add_argument('--manual_type', type=str, help='TBD')
        parser.add_argument('--coefficients', type=list[int], help='TBD')
        parser.add_argument('--save_manual', type=str, help='TBD')
        parser.add_argument('--manual_input', type=str, help='TBD')

        # Parse and read arguments and assign them to variables if exists
        args, _ = parser.parse_known_args()

        # Check if no arguments were provided and print help if so
        if len(sys.argv) == 1:
            parser.print_help()
            sys.exit(0)

        data_name = file_name
        if args.file_name:
            data_name = args.file_name

        int_type = intrusion_type
        if args.intrusion_type:
            int_type = args.intrusion_type

        idtype = ID_type
        if args.ID_type:
            idtype = args.ID_type

        man_type = manual_type
        if args.manual_type:
            man_type=args.manual_type

        coeff = coefficients
        if args.coefficients:
            coeff=args.coefficients

        s_manual = save_manual
        if args.save_manual:
            s_manual=args.save_manual

        i_manual = manual_input
        if args.manual_input:
            i_manual=args.manual_input

        return data_name, int_type, idtype, man_type, coeff, s_manual, i_manual

    
    def separate_yearly_profiles(self) -> dict[dict]:
        self.dates_stamp = self.data[self.dates_name]
        self.dates = timestamp2datetime_lists(self.dates_stamp)

        grouped_years, self.uyears  = separate_yearly_dates(self.dates)

        self.metadata_intrusions['Init_year'] = [self.uyears[0]]
        self.metadata_intrusions['End_year'] = [self.uyears[-1]]

        by_year_indices = {year: [self.dates.index(dt) for dt in grouped_years[year]] 
                        for year in self.uyears}

        yearly_profiles_temp, yearly_profiles_salt = create_yearly_matrices(self.data, by_year_indices)
            
        return {'Yearly Temp Profile': yearly_profiles_temp, 
                'Yearly Salt Profile': yearly_profiles_salt, 
                'Indices by Year':by_year_indices}


    def plot_year_profiles(self, year_data: dict[dict], yr: int):
    
        init_date_index = year_data['Indices by Year'][yr][0]
        last_date_index = year_data['Indices by Year'][yr][-1]
        datetime_list = self.dates[init_date_index:last_date_index]

        fig, axs = plt.subplots(2)
        year_temp_data = year_data['Yearly Temp Profile'][yr]
        year_salt_data = year_data['Yearly Salt Profile'][yr]

        X,Y = np.meshgrid(datetime_list, self.data[self.depth_name])
        mesh0 = axs[0].pcolormesh(X,Y,year_temp_data[:,:len(Y[0,:])], cmap='seismic')
        fig.colorbar(mesh0, ax=axs[0])
        axs[0].invert_yaxis()
        mesh0.set_clim(self.temp_range)
        axs[0].set_xticks([])

        mesh1 = axs[1].pcolormesh(X,Y,year_salt_data[:,:len(Y[0,:])], cmap='seismic')
        fig.colorbar(mesh1, ax=axs[1])
        axs[1].invert_yaxis()
        mesh1.set_clim(self.salt_range)
        axs[1].xaxis.set_major_formatter(mdates.DateFormatter("%m"))

        fig.tight_layout()
        axs[0].text(0.02,0.85,str(yr), transform=axs[0].transAxes,fontsize=14,
                    verticalalignment='bottom',horizontalalignment='left',
                    bbox=dict(facecolor='white',alpha=0.5))

        return {
            'Figure':fig,
            'Axes':axs,
            'Mesh':[mesh0,mesh1]
        }


    def from_1970(self,date: int) -> datetime:
        reference_date = datetime(1970, 1, 1)

        delta = timedelta(days=date)
        datetime_obj = reference_date + delta

        return datetime_obj


    def user_intrusion_selection(self, yearly_profiles: dict[any]) -> None:
        print(self.lin+'Intrusion identification in progress')

        for yr in self.uyears:
            fig = self.plot_year_profiles(yearly_profiles, 
                                yr)

            cid_click = fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

            cid_key = fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

            plt.show()

        intrusion_dates = list(np.array(get_points())[:,0])
        self.manualID_dates = [self.from_1970(dt) for dt in intrusion_dates]\
        
        self.table_IDeffects['Dates'] = self.manualID_dates
        print(self.lin+'Intrusion identification completed')


    def intrusion_date_comparison(self, dates1, dates2) -> dict[list]:
        def within_days(dt1, dt2):
            calc = abs((dt2 - dt1).days)
            return calc

        matching = []
        unmatched_md = []

        for dt1 in dates1:
            found_match = False
            single_match = []
            for dt2 in dates2:
                diff = within_days(dt1, dt2)
                if diff <= self.dates_error:
                    single_match.append([diff, dt1, dt2])
                    found_match = True
                    break

            if not found_match:
                unmatched_md.append(dt1)
            else:
                if len(single_match) > 1:
                    diff_list = [match[0] for match in single_match]
                    min_index = [idx for idx, value in enumerate(diff_list) if value == min(diff_list)]
                    matching.append([single_match[min_index]])
                else:
                    matching.append(single_match) 
        
        matching = [item for sublist in matching for item in sublist]
        matching_estimated = [sublist[2] for sublist in matching]

        set1 = set(dates2)
        set2 = set(matching_estimated)
        unmatched_ed = set1-set2

        return {
            'Matched':matching,
            'Only Manual':unmatched_md,
            'Only Estimated':unmatched_ed,
        }


    def get_original_indices(self) -> None:

        comparison_results = self.intrusion_date_comparison(self.manualID_dates, self.dates)
        compared_dates = comparison_results['Matched']
        intrusion_dates = [match[2] for match in compared_dates]
        self.manualID_indices = [i for i, dt1 in enumerate(self.dates) for j, dt2 in enumerate(intrusion_dates) if dt1 == dt2]
        self.table_IDeffects['Index'] = self.manualID_indices


    def get_intrusion_effects(self) -> None:

        self.manualID_temp_effects = self.data[self.bottom_avg_names[0]][self.manualID_indices]
        self.manualID_salt_effects = self.data[self.bottom_avg_names[1]][self.manualID_indices]

        if self.manualID_type.upper() == 'MID':
            self.manualID_temp_effects = self.data[self.mid_avg_names[0]][self.manualID_indices]
            self.manualID_salt_effects = self.data[self.mid_avg_names[1]][self.manualID_indices]
        
        self.table_IDeffects['Temp_effects'] = self.manualID_temp_effects
        self.table_IDeffects['Salt_effects'] = self.manualID_salt_effects

        self.metadata_intrusions['Variables_used'] = str(['salinity', 'temperature'])

    
    def intrusion_identification(self, lst: list[int]) -> list[datetime]:
        temp_intrusion_coeff, salt_intrusion_coeff = lst

        column_avgs_temp = self.data[self.bottom_avg_names[0]]
        column_avgs_salt = self.data[self.bottom_avg_names[1]]

        if self.manualID_type.upper() == 'MID':
            column_avgs_temp = self.data[self.mid_avg_names[0]]
            column_avgs_salt = self.data[self.mid_avg_names[1]]

        intrusion_temp_indices = list(np.where(column_avgs_temp > temp_intrusion_coeff)[0]+1)
        intrusion_salt_indices = list(np.where(column_avgs_salt > salt_intrusion_coeff)[0]+1)

        all_timestamps = pd.DataFrame(self.dates_stamp)

        temp_intrusion_dates = all_timestamps.iloc[intrusion_temp_indices]
        salt_intrusion_dates = all_timestamps.iloc[intrusion_salt_indices]

        estimated_intrusion_dates = [value for value in temp_intrusion_dates.values.tolist() 
                                    if value in salt_intrusion_dates.values.tolist()]
        estimated_intrusion_dates = [item for sublist in estimated_intrusion_dates for item in sublist]
        estimated_intrusion_dates = timestamp2datetime_lists(estimated_intrusion_dates)

        return estimated_intrusion_dates


    def intrusion_ID_performance(self, lst: list[int]):
        
        estimated_intrusion_dates = self.intrusion_identification(lst)

        real_intrusion_dates = self.manualID_dates
        comparison_dates = self.intrusion_date_comparison(real_intrusion_dates, estimated_intrusion_dates)
            
        missed_id = comparison_dates['Only Manual']
        extra_id = comparison_dates['Only Estimated']
        caught_id = comparison_dates['Matched']

        if len(estimated_intrusion_dates) != 0:
            missed_id_parameter = len(missed_id)/len(real_intrusion_dates)
            extra_id_parameter = len(extra_id)/len(estimated_intrusion_dates)

            performance_parameter = ((len(real_intrusion_dates)*(missed_id_parameter) + 
                                    len(estimated_intrusion_dates)*extra_id_parameter)/
                                    (len(real_intrusion_dates)+len(estimated_intrusion_dates)))
        else:
            performance_parameter = 1

        return performance_parameter


    def estimate_coefficients(self) -> None:
        print(self.lin+'Estimating coefficients for optimized intrusion identification')

        real_intrusion_dates = self.manualID_dates
        range = self.OF_range
        
        temp_range = np.arange(range[0],range[1],0.025)
        salt_range = np.arange(0,range[1],0.02)
        result_final = []

        for temp_guess in temp_range:
            for salt_guess in salt_range:
                initial_guess = [temp_guess, salt_guess]
                result = minimize(self.intrusion_ID_performance, initial_guess)
                result_final.append((result.x, result.fun))

        best_coefficients = min(result_final, key= lambda x: x[1])

        self.OP_performance = best_coefficients[1]
        self.OP_temp_coeff = list(best_coefficients[0])[0]
        self.OP_salt_coeff = list(best_coefficients[0])[1]

        self.table_coefficients['Temp_coefficient'] = [self.OP_temp_coeff]
        self.table_coefficients['Salt_coefficient'] = [self.OP_salt_coeff]
        self.table_coefficients['Performance'] = [self.OP_performance]

        result_comp = self.intrusion_date_comparison(real_intrusion_dates, 
                                                self.intrusion_identification([self.OP_temp_coeff, self.OP_salt_coeff]))
        
        self.OP_Missed = result_comp['Only Manual']
        self.OP_Extra = result_comp['Only Estimated']
        self.OP_Found = result_comp['Matched']

        self.table_coefficients_error['Missed'] = self.OP_Missed
        self.table_coefficients_error['Extra'] = self.OP_Extra
        self.table_coefficients_error['Found'] = self.OP_Found

    
    def count_csv_rows(self,PATH) -> int:
        with open(PATH,'r') as file:
            read = csv.reader(file)
            row_count = sum(1 for row in read)

        return row_count
    

    def record_single(self, TABLE, DICT) -> None:
        table_path = self.meta_path+TABLE
        row_num1 = self.count_csv_rows(table_path)

        if row_num1 == 0:
            DICT['ID'] = 1
            DATAF= pd.DataFrame(DICT)
            DATAF.to_csv(table_path,mode='a', header=True, index=False)
        else:
            DICT['ID'] = row_num1
            DATAF= pd.DataFrame(DICT)
            DATAF.to_csv(table_path,mode='a', header=False, index=False)

    
    def automatedID(self, coefficients, manual_input):
        self.metadata_intrusions['manual_input_type'] = 'N/A'

        self.metadata_intrusions['manual_input_path'] = 'N/A'

        self.metadata_intrusions['manual_input_path'] = manual_input
        intrusion_dates = import_joblib(manual_input)
        self.manualID_dates = intrusion_dates
        self.table_IDeffects['Dates'] = self.manualID_dates

        self.metadata_intrusions['manual_input_save'] = 'OFF'

        self.get_original_indices()
        self.get_intrusion_effects()

        self.table_coefficients['Temp_coefficient'] = [coefficients[0]]
        self.table_coefficients['Salt_coefficient'] = [coefficients[1]]

        self.OP_performance = self.intrusion_ID_performance(coefficients)
        result_comp = self.intrusion_date_comparison(self.manualID_dates, 
                                                self.intrusion_identification(coefficients))
        
        self.table_coefficients['Performance'] = [self.OP_performance]
        self.table_coefficients_error['Missed'] = result_comp['Only Manual']
        self.table_coefficients_error['Extra'] = result_comp['Only Estimated']
        self.table_coefficients_error['Found'] = result_comp['Matched']

    
    def record_metadata(self) -> None:
        row_num = self.count_csv_rows(self.meta_path+self.meta_table)
        rows_intrusion = len(self.table_IDeffects['Dates'])

        rows_missed = len(self.table_coefficients_error['Missed'])
        rows_extra = len(self.table_coefficients_error['Extra'])
        rows_found = len(self.table_coefficients_error['Found'])
        self.table_coefficients_error_comb['Type'] = ['Missed']*rows_missed + ['Extra']*rows_extra + ['Found']*rows_found
        self.table_coefficients_error_comb['Dates'] = list(self.table_coefficients_error['Missed']) + list(self.table_coefficients_error['Extra']) + [sub[-1] for sub in list(self.table_coefficients_error['Found'])]
        rows_error = len(self.table_coefficients_error_comb['Dates'])

        if row_num == 0:
            index = 1
            self.table_IDeffects['ID'] = [index]*rows_intrusion
            self.table_coefficients_error_comb['Error'] = [index]*rows_error
            DataF_IDeffects = pd.DataFrame(self.table_IDeffects)
            DataF_error = pd.DataFrame(self.table_coefficients_error_comb)
            DataF_IDeffects.to_csv(self.meta_path+self.intrusions_table,mode='a', header=True, index=False)
            DataF_error.to_csv(self.meta_path+self.coeff_error_table,mode='a', header=True, index=False)
            
        else:
            index = row_num
            self.table_IDeffects['ID'] = [index]*rows_intrusion
            self.table_coefficients_error_comb['Error'] = [index]*rows_error
            DataF_IDeffects = pd.DataFrame(self.table_IDeffects)
            DataF_error = pd.DataFrame(self.table_coefficients_error_comb)
            DataF_IDeffects.to_csv(self.meta_path+self.intrusions_table,mode='a', header=False, index=False)
            DataF_error.to_csv(self.meta_path+self.coeff_error_table,mode='a', header=False, index=False)
        
        self.record_single(self.meta_table, self.metadata_intrusions)
        self.record_single(self.coeff_table, self.table_coefficients)
    

def main() -> intrusions:
    # Get command line arguments
    file_name, intrusion_type, ID_type, manual_type, coefficients, save_manual, manual_input = get_command_line_args()

    path_data = '../DATA/PROCESSED/'
    file_PATH = path_data + file_name
    
    # Initializing intrusion object
    BBMP = intrusions(file_PATH)

    yearly_profiles = BBMP.separate_yearly_profiles()

    # Recording metadata
    BBMP.metadata_intrusions['ID_type'] = ID_type.upper()
    BBMP.metadata_intrusions['Current_time'] = [int(time.time())]
    BBMP.manualID_type = intrusion_type
    BBMP.metadata_intrusions['Intrusion_type'] = BBMP.manualID_type

    if ID_type.upper() == 'MANUAL':
        BBMP.metadata_intrusions['manual_input_type'] = manual_type.upper()
        
        if manual_type.upper() == 'MANUAL':
            # Manual identification through plots
            BBMP.metadata_intrusions['manual_input_path'] = 'N/A'
            BBMP.user_intrusion_selection(yearly_profiles)
            
            if save_manual.upper() == 'ON':
                # Save manually identified intrusion
                man_name = 'manualID_' + BBMP.manualID_type + str(int(time.time())) + '.pkl'
                save_joblib(BBMP.manualID_dates, man_name)
        else:
            # Manual indeitification through importing file
            manual_input_path = path_data + manual_input
            BBMP.metadata_intrusions['manual_input_path'] = manual_input_path
            intrusion_dates = import_joblib(manual_input_path)
            BBMP.manualID_dates = intrusion_dates
            BBMP.table_IDeffects['Dates'] = BBMP.manualID_dates
        
        # Record Metadata
        BBMP.metadata_intrusions['manual_input_save'] = save_manual.upper()
        BBMP.get_original_indices()
        BBMP.get_intrusion_effects()

        # Estimate optimized coefficients
        BBMP.estimate_coefficients()

    else:
        # Automated identification using specific coefficients
        BBMP.automatedID(coefficients, path_data + manual_input)

    # Record metadata in .csv files
    BBMP.record_metadata()

    return BBMP

 
if __name__ == '__main__':
    Data = main()

    