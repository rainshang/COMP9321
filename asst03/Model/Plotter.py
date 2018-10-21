import matplotlib.pyplot as plt
from PIL import Image

class Plotter(object):
    """docstring for Plotter."""
    def __init__(self):
        super(Plotter, self).__init__()


    def pieChart(self, labels, sizes):

        fig1, ax1 = plt.subplots()
        ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
                shadow=True, startangle=90)
        ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

        plt.savefig('trend.jpg')
