import matplotlib.pyplot as plt

points = []   # Creates list to save the points selected
def onclick(event) -> None:
    """
    Allows you to select point from plots
    """
    if event.button == 1:
        if event.inaxes is not None:
            x, y = event.xdata, event.ydata
            points.append((x, y))

            event.inaxes.plot(x, y, 'ro')
            event.canvas.draw()



def onkey(event) -> None:
    """
    Terminates point selection stage
    """
    if event.key == ' ':
        plt.close(event.canvas.figure)


def get_points() -> list[int]:
    return points
