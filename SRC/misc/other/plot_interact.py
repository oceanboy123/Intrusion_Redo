import matplotlib.pyplot as plt

points = []   # Creates list to save the points selected
def onclick(event):
    """
    Allows you to select point from plots
    """

    if event.button == 1:
        if event.inaxes is not None:
            x, y = event.xdata, event.ydata

            points.append((x, y))


def onkey(event):
    """
    Terminates point selection stage
    """

    if event.key == ' ':
        plt.close(event.canvas.figure)


def get_points():
    return points
