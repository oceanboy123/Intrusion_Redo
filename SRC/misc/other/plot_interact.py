import matplotlib.pyplot as plt

points = []   # List to save the points selected

def onclick(event) -> None:
    """
    Allows you to select points from plots by left-clicking.
    """
    if event.button == 1: # Left-click
        if event.inaxes is not None:
            x, y = event.xdata, event.ydata
            points.append((x, y))

            # Plot the selected point as a red circle
            event.inaxes.plot(x, y, 'ro')
            event.canvas.draw()



def onkey(event) -> None:
    """
    Terminates point selection stage when the spacebar is pressed.
    """
    if event.key == ' ':
        plt.close(event.canvas.figure)


def get_points() -> list[tuple[float, float]]:
    """
    Returns the list of selected points and clears the points list.
    """
    selected_points = points.copy()
    points.clear()  # Clear the list after returning points
    return selected_points
