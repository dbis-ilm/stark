from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

import sys
import csv

filename = sys.argv[1]
geomCol = int(sys.argv[2])

# lists for latitude and longitude values
lons = []
lats = []

with open(filename) as csvFile:
	reader = csv.reader(csvFile, delimiter=",")
	for row in reader:
		# remove the WKT details
		s = str.replace(str.replace(row[geomCol], "POINT(", ""), ")", "").split()

		# latitude is in first position
		lats.append(float(s[0]))
		lons.append(float(s[1]))

# draw map with markers for float locations
map = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,\
            llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')
x, y = map(lons,lats)
#map.drawmapboundary(fill_color='#99ffff')
#map.fillcontinents(color='#cc9966',lake_color='#99ffff')
map.drawcoastlines()
map.scatter(x,y,10,marker='o',color='red')
plt.title('Locations',fontsize=12)
plt.show()
