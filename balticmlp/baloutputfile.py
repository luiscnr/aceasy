from netCDF4 import Dataset
from datetime import datetime as dt
import numpy.ma as ma


class BalOutputFile:

    def __init__(self, ofname):
        self.FILE_CREATED = True
        try:
            self.OFILE = Dataset(ofname, 'w', format='NETCDF4')
        except PermissionError:
            # print('Permission denied: ', ofname)
            self.FILE_CREATED = False

    def set_global_attributes(self):
        # Atributes
        self.OFILE.creation_time = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        # satellite = at['satellite']
        # platform = at['platform']
        # sensor = at['sensor']
        # res_str = at['res']
        # self.EXTRACT.satellite = satellite
        # self.EXTRACT.platform = platform
        # self.EXTRACT.sensor = sensor
        # self.EXTRACT.description = f'{satellite}{platform} {sensor.upper()} {res_str} L2 extract'
        # self.EXTRACT.satellite_aco_processor = at['aco_processor']  # 'Atmospheric Correction processor: xxx'
        # self.EXTRACT.satellite_proc_version = at['proc_version']  # proc_version_str
        #
        # self.EXTRACT.insitu_site_name = at['station_name']
        # self.EXTRACT.insitu_lat = at['in_situ_lat']
        # self.EXTRACT.insitu_lon = at['in_situ_lon']

    def create_dimensions(self, ny, nx):
        # dimensions
        self.OFILE.createDimension('lat', ny)
        self.OFILE.createDimension('lon', nx)
        self.OFILE.createDimension('time', 1)

    def create_satellite_time_variable(self, satellite_start_time):
        satellite_time = self.OFILE.createVariable('time', 'i4', ('time'), fill_value=-999,
                                                   zlib=True, complevel=6)
        # print('Satellite start time es: ',satellite_start_time)
        dateref = dt(1981, 1, 1, 0, 0, 0)
        seconds = (satellite_start_time - dateref).total_seconds()
        satellite_time[0] = int(seconds)
        satellite_time.units = "seconds since 1981-01-01"
        satellite_time.long_name = "reference time"
        satellite_time.standard_name = "time"
        satellite_time.axis = "T"
        satellite_time.calendar = "Gregorian"

    def create_lat_long_variables(self, lat, lon):

        # latitude
        satellite_latitude = self.OFILE.createVariable('latitude', 'f4', ('lat', 'lon'), fill_value=-999, zlib=True,
                                                       complevel=6)
        satellite_latitude[:, :] = [lat[:, :]]
        satellite_latitude.units = "degrees_north"
        satellite_latitude.long_name = "latitude"
        satellite_latitude.standard_name = "latitude"
        # satellite_latitude.valid_min = 53.250
        # satellite_latitude.valid_max = 65.850

        # longitude
        satellite_longitude = self.OFILE.createVariable('longitude', 'f4', ('lat', 'lon'), fill_value=-999, zlib=True,
                                                        complevel=6)
        satellite_longitude[:, :] = [lon[:, :]]
        satellite_longitude.units = "degrees_east"
        satellite_longitude.long_name = "longitude"
        satellite_longitude.standard_name = "longitude"
        # satellite_longitude.valid_min = 9.2500
        # satellite_longitude.valid_max = 30.2500

    def create_data_variable(self, var_name, array):

        var = self.OFILE.createVariable(var_name, 'f4', ('lat', 'lon'), fill_value=-999, zlib=True, complevel=6)
        var[:, :] = [array[:, :]]
        return var

    def create_chla_variable(self, array):
        var = self.create_data_variable('CHL', array)
        var.coordinates = "lat lon"
        var.long_name = "Chlorophyll a concentration"
        var.standard_name = "mass_concentration_of_chlorophyll_a_in_sea_water"
        var.type = "surface"
        var.units = "milligram m^-3"
        var.missing_value = - 999
        var.valid_min = 0.0100000000000000
        var.valid_max = 300
        var.comment = "Reference"
        var.source = "OLCI - POLYMER v. 4.14 atmospheric processor - MLP"

    def close_file(self):
        self.OFILE.close()