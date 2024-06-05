
import global_functions as gf
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

file_name = input("Enter the file name for intrusion identification (include .pkl):   ")

print('Importing Data')
selected_data = gf.import_joblib(file_name)

yearly_profiles = gf.separate_yearly_profiles(selected_data)

print('Intrusion identification in progress')

points = []
def onclick(event):
    if event.button == 1:
        if event.inaxes is not None:
            x, y = event.xdata, event.ydata

            points.append((x, y))

            print(f"Point selected: ({x}, {y})")
            event.inaxes.plot(x, y, 'ro')
            event.canvas.draw()

def onkey(event):
    if event.key == ' ':
        plt.close(event.canvas.figure)

def get_points():
    return points


range_1 = [0,10]
range_2 = [30.5,31.5]
range_3 = [0,12]
points_year = {}

timestamp = selected_data['sample_timestamps']
year_list = list(set([datetime.fromtimestamp(stamp).year for stamp in timestamp]))

for yr in year_list:
    fig = gf.plot_year_profiles(selected_data, yearly_profiles, 
                        yr,[range_1, range_2, range_3])

    cid_click = fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

    cid_key = fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

    plt.show()
print('Intrusion identification completed')

intrusion_dates = list(np.array(get_points())[:,0])
intrusion_datetimes = [gf.from_1970(dt) for dt in intrusion_dates]

version = input('Version name (Keep it simple):   ')
file_fname = 'manual_intrusions' + '_' + version + '.pkl'
gf.save_joblib(file_fname, intrusion_datetimes)
print(f'Saved as {file_fname}')



selected_data['sample_intrusion_timestamps'] = intrusion_datetimes

print('Estimating coefficients for optimized intrusion identification')
results = gf.estimate_coefficients(selected_data)

selected_data['intrusion_indice'] = [results['Intrusions Missed'], 
                                     results['Intrusions Extra'],
                                       results['Intrusions IDed']]


print('Recording results in data file')
selected_data['temperature_coeff'] = list(results['Estimated Coefficient'][0])[0]
selected_data['sallinity_coeff'] = list(results['Estimated Coefficient'][0])[1]
selected_data['Performance'] = results['Estimated Coefficient'][1]

desc = input("Write description:   ")
selected_data['Comments'] = desc

file_fname = input("Enter the file name for output data file (include .pkl):   ")
gf.save_joblib(file_fname, selected_data)
print(f'Saved as {file_fname}')