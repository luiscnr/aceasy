import zipfile as zp
import os
from shapely.geometry import Point
from shapely.geometry import Polygon


class CHECK_GEO():
    def __init__(self):
        self.polygon_image = None
        self.coords_image = None

    def check_zip_file(self,prod_path):
        valid = True
        try:
            zprod = zp.ZipFile(prod_path,'r')
            zprod.close()
        except:
            valid = False
        return valid

    def start_polygon_image_from_zip_manifest_file(self, prod_path):
        with zp.ZipFile(prod_path, 'r') as zprod:
            fname = prod_path.split('/')[-1][0:-4]
            if not fname.endswith('SEN3'):
                fname = fname + '.SEN3'
            geoname = os.path.join(fname, 'xfdumanifest.xml')
            if geoname in zprod.namelist():
                gc = zprod.open(geoname)
                for line in gc:
                    line_str = line.decode().strip()
                    if line_str.startswith('<gml:posList>'):
                        clist = line_str[len('<gml:posList>'):line_str.index('</gml:posList>')].split()
                        coords = []
                        for i in range(0, len(clist), 2):
                            coord_here = (float(clist[i + 1]), float(clist[i]))
                            coords.append(coord_here)
                        self.coords_image = coords
                        self.polygon_image = Polygon(coords)  # create polygon
                gc.close()

    def start_polygon_from_prod_manifest_file(self, prod_path):
        if os.path.exists(prod_path) and os.path.isdir(prod_path):
            geoname = os.path.join(prod_path, 'xfdumanifest.xml')
            if os.path.exists(geoname):
                gc = open(geoname)
                for line in gc:
                    line_str = line.strip()
                    if line_str.startswith('<gml:posList>'):
                        clist = line_str[len('<gml:posList>'):line_str.index('</gml:posList>')].split()
                        coords = []
                        for i in range(0, len(clist), 2):
                            coord_here = (float(clist[i + 1]), float(clist[i]))
                            coords.append(coord_here)
                        self.polygon_image = Polygon(coords)  # create polygon
                        self.coords_image = coords
                gc.close()




    def get_geo_limits(self):
        if self.polygon_image is None:
            return None

    def check_point(self, point_site):
        if not isinstance(point_site, Point):
            return -1
        if point_site.within(self.polygon_image):
            flag_location = 1
        else:
            flag_location = 0
        return flag_location

    def check_point_lat_lon(self, lat_point, lon_point):
        point_site = Point(lon_point, lat_point)
        return self.check_point(point_site)

    def check_polygon(self, polygon_area):
        if not isinstance(polygon_area, Polygon):
            return -1
        if self.polygon_image.intersects(polygon_area):
            flag_location = 1
        else:
            flag_location = 0
        return flag_location

    def check_geo_area(self, south, north, west, east):
        point_list = [Point(west, south), Point(west, north), Point(east, north), Point(east, south),
                      Point(west, south)]
        polygon_area = Polygon([[p.x, p.y] for p in point_list])
        return self.check_polygon(polygon_area)
