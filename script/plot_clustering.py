#!/usr/bin/python

import sys, getopt
from scipy import *
from pylab import *
from matplotlib.patches import Rectangle
import matplotlib.cm as cm


# draw a rectangle
def draw_box(xmin, ymin, xmax, ymax):
    currentAxis = plt.gca()
    currentAxis.add_patch(Rectangle((xmin, ymin), xmax-xmin, ymax-ymin, fill=True,facecolor="r"))

def plot(file, partitions):
    # draw the rectangles representing the partitions
    if not partitions is None:
        xmin,ymin,xmax,ymax = loadtxt(partitions, delimiter=',', unpack=True)
        for i in xrange(len(xmin)):
            draw_box(xmin[i], ymin[i], xmax[i], ymax[i])

    # load the clustered data
    x,y,id = loadtxt(file, delimiter=',', unpack=True)
    print(len(x))
    # build a list of cluster ids
    cluster_ids = list(set(id))

    # ... and based on it, a dictionary for mapping the cluster_ids to [0, 1, 2, ...]
    ids = {}
    for i, c in enumerate(cluster_ids):
      ids[c] = i

    # map the cluster ids
    mapped_ids = [ids[c] for c in id]

    # and draw a scatter plot
    scatter(x,y,c=mapped_ids,cmap=plt.cm.RdYlGn)
    show()

def print_msg(exit_code):
    print('plot_clustering.py -i <clustering file> [-p <mbb file>]')
    sys.exit(exit_code)

if __name__ == "__main__":

    input = None
    partitions = None

    try:
        opts, args = getopt.getopt(sys.argv[1:],"hi:p:",["input=", "partitions=" ])
    except getopt.GetoptError:
        print_msg(2)
    for opt, arg in opts:
        if opt == '-h':
            print_msg(0)
        elif opt in ("-i", "--input"):
            input = arg
        elif opt in ("-p", "--partitions"):
            partitions = arg

    if input is None:
        print_msg(2)

    plot(input, partitions)
