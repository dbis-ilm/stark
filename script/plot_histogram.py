#!/usr/bin/python

import sys, getopt
from scipy import *
from pylab import *
from matplotlib.patches import Rectangle
import matplotlib.cm as cm


# draw a rectangle
def draw_box(xmin, ymin, xmax, ymax):
    currentAxis = plt.gca()
    currentAxis.add_patch(Rectangle((xmin, ymin), xmax-xmin, ymax-ymin, fill=False))

def plot(file):
    # load the histogram data
    min,max,freq = loadtxt(file, delimiter=',', unpack=True)
    width = max[0]-min[0]
    epsilon = 1e-7
    freq = [x+epsilon for x in freq]
    # create a histogram plot
    bar(min,freq, width)
    show()

def print_msg(exit_code):
    print('plot_histogram.py -i <clustering file>')
    sys.exit(exit_code)

if __name__ == "__main__":

    input = None

    try:
        opts, args = getopt.getopt(sys.argv[1:],"hi:",["input=" ])
    except getopt.GetoptError:
        print_msg(2)
    for opt, arg in opts:
        if opt == '-h':
            print_msg(0)
        elif opt in ("-i", "--input"):
            input = arg

    if input is None:
        print_msg(2)

    plot(input)
