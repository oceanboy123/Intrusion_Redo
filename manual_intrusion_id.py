import global_functions as gf
import matplotlib.pyplot as plt
from datetime import datetime

file_name = 'BBMP_salected_data.pkl'

selected_data = gf.import_joblib(file_name)

yearly_profiles = gf.separate_yearly_profiles(selected_data)

points = []
def onclick(event):
    if event.button == 1:
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
points_year = {}

timestamp = selected_data['sample_timestamps']
year_list = list(set([datetime.fromtimestamp(stamp).year for stamp in timestamp]))

for yr in year_list:
    fig = gf.plot_year_profiles(selected_data, yearly_profiles, 
                        yr,[range_1, range_2])

    cid_click = fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

    cid_key = fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

    points_year[str(yr)] = get_points()

    plt.show()

points_name = 'manual_intrusions_'+ str(year_list[0]) + str(year_list[-1])
gf.save_joblib(points_name, points_year)