#!/usr/bin/python

from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon

import sys,csv, getopt

# def draw_box(m, lats, lons):
def draw_box(m, xmin,ymin,xmax,ymax,color):

	lats = [ xmin, xmax, xmax, xmin ]
	lons = [ ymin, ymin, ymax, ymax ]

	x,y = m(lons,lats)
	xy = zip(x,y)
	# currentAxis = plt.gca()
	# currentAxis.add_patch(Rectangle((xmin, ymin), xmax-xmin, ymax-ymin, fill=True,facecolor="b"))
	poly=Polygon(xy, facecolor=color, alpha=0.4)
	# poly = Rectangle((xmin, ymin), xmax-xmin, ymax-ymin, fill=True,facecolor="b")
	plt.gca().add_patch(poly)


def draw_points(themap, filename, geomCol):
	# lists for latitude and longitude values
	lons = []
	lats = []

	with open(filename) as csvFile:
		reader = csv.reader(csvFile, delimiter=";")
		rows = 0
		for row in reader:
			# remove the WKT details
			s = str.replace(str.replace(row[geomCol], "POINT (", ""), ")", "").split()
			rows += 1
			# latitude is in first position
			lats.append(float(s[1]))
			lons.append(float(s[0]))

	x, y = themap(lons,lats)
	themap.scatter(x,y,10,marker='o',color='red')
	print("finished drawing points: "+str(rows))



def draw_partitions(themap, partitions):
	with open(partitions) as rectanglesFile:
		rows = 0
		reader = csv.reader(rectanglesFile, delimiter=",")
		for row in reader:
			draw_box(themap,float(row[0]),float(row[1]),float(row[2]),float(row[3]),"blue")
			rows += 1
	print("finished drawing partitions: "+str(rows))

def draw_cells(themap, cellFile):
	with open(cellFile) as cells:
		rows = 0
		reader = csv.reader(cells,delimiter=",")
		for row in reader:
			draw_box(themap,float(row[0]),float(row[1]),float(row[2]),float(row[3]),"green")
			rows += 1
	print("finished drawing cells: "+str(rows))

def print_msg(exit_code):
	print('plot_histogram.py -i <pointfile (wkt)> -g <geometry column> -p <partitions file> -c <cells file>')
	sys.exit(exit_code)

if __name__ == "__main__":
	geomCol = None
	points = None
	partitions = None
	cells = None

	try:
		opts, args = getopt.getopt(sys.argv[1:],"hi:g:p:c:",["points=", "geomcol=","partitions=","cells=" ])
	except getopt.GetoptError:
		print_msg(2)

	for opt, arg in opts:
		if opt == '-h':
			print_msg(0)
		elif opt in ("-i", "--points"):
			points = arg
		elif opt in ("-g", "--geomcol"):
			geomCol = int(arg)
		elif opt in ("-p", "--partitions"):
			partitions = arg
		elif opt in ("-c", "--cells"):
			cells = arg

	if (points is None or geomCol is None) and partitions is None and cells is None:
		print_msg(2)

	# draw map with markers for float locations
	themap = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,\
						llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')

	# themap.drawmapboundary(fill_color='#99ffff')
	#themap.fillcontinents(color='#cc9966',lake_color='#99ffff')
	themap.drawcoastlines()

	if points is not None and geomCol is not None:
		draw_points(themap, points, geomCol)


	if partitions is not None:
		draw_partitions(themap, partitions)

	if cells is not None:
		draw_cells(themap, cells)


	plt.title('Locations',fontsize=12)
	plt.show()
