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

        iop_comment = 'QAA v. 6 modified as in Brando et al. (2021)'
        iop_source ='OLCI - POLYMER v.4.14 Atmospheric Correction Processor'
        self.iop_attributes = {
            'ADG443':{
                'long_name':'Absorption due to gelbstoff and detrital material at 443 nm',
                'standard_name':'volume_absorption_coefficient_of_radiative_flux_in_sea_water_due_to_dissolved_organic_matter_and_non_algal_particles',
                'type':'surface',
                'units':'m^-1',
                'comment': iop_comment,
                'source': iop_source
            },
            'APH443':{
                'long_name': 'Absorption due to phytoplankton at 443 nm',
                'standard_name': 'volume_absorption_coefficient_of_radiative_flux_in_sea_water_due_to_phytoplankton',
                'type': 'surface',
                'units': 'm^-1',
                'comment': iop_comment,
                'source': iop_source
            },
            'BBP443':{
                'long_name': 'Particulate back-scattering coefficient at 443 nm',
                'standard_name': 'volume_backwards_scattering_coefficient_of_radiative_flux_in_sea_water_due_to_particles',
                'type': 'surface',
                'units': 'm^-1',
                'comment': iop_comment,
                'source': iop_source
            }
        }

    def set_global_attributes(self,ncpolymer):
        # Atributes
        self.OFILE.creation_time = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        self.OFILE.start_time = ncpolymer.start_time
        self.OFILE.stop_time = ncpolymer.stop_time
        self.OFILE.polymer_source_file = ncpolymer.l2_filename.split('/')[-1]
        self.OFILE.l1_source_file = ncpolymer.l1_filename.split('/')[-1]
        self.OFILE.timeliness = self.OFILE.polymer_source_file.split('_')[17]
        self.OFILE.platform = self.OFILE.polymer_source_file[2]
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

    def set_global_attributes_from_dict(self,varattr):
        if varattr is not None:
            if 'GLOBAL' in varattr.keys():
                #print(varattr['GLOBAL'])
                for at in varattr['GLOBAL']:
                    self.OFILE.setncattr(at,varattr['GLOBAL'][at])
        self.OFILE.creation_date = dt.utcnow().strftime("%a %b %d %Y")
        self.OFILE.creation_time = dt.utcnow().strftime("%H:%M:%S")






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
        if len(lat.shape)==2:
            satellite_latitude = self.OFILE.createVariable('lat', 'f4', ('lat', 'lon'), fill_value=-999, zlib=True,complevel=6)
            satellite_latitude[:, :] = [lat[:, :]]
        elif len(lat.shape)==1:
            satellite_latitude = self.OFILE.createVariable('lat', 'f4', ('lat',), fill_value=-999, zlib=True,
                                                           complevel=6)
            satellite_latitude[:] = [lat[:]]
        satellite_latitude.units = "degrees_north"
        satellite_latitude.long_name = "latitude"
        satellite_latitude.standard_name = "latitude"
        satellite_latitude.axis = "Y"


        # longitude
        if len(lon.shape) == 2:
            satellite_longitude = self.OFILE.createVariable('lon', 'f4', ('lat', 'lon'), fill_value=-999, zlib=True,complevel=6)
            satellite_longitude[:, :] = [lon[:, :]]
        elif len(lon.shape) == 1:
            satellite_longitude = self.OFILE.createVariable('lon', 'f4', ('lon',), fill_value=-999,
                                                            zlib=True, complevel=6)
            satellite_longitude[:] = [lon[:]]
        satellite_longitude.units = "degrees_east"
        satellite_longitude.long_name = "longitude"
        satellite_longitude.standard_name = "longitude"
        satellite_longitude.axis="X"


    def create_data_variable(self, var_name, array):
        var = self.OFILE.createVariable(var_name, 'f4', ('lat', 'lon'), fill_value=-999, zlib=True, complevel=6)
        var[:] = array[:]
        return var

    def create_flag_variable(self, var_name, array):
        var = self.OFILE.createVariable(var_name, 'i2', ('lat', 'lon'), fill_value=-999, zlib=True, complevel=6)
        var[:] = array[:]
        return var

    def create_chla_variable(self, array):
        var = self.create_data_variable('CHL', array)
        var.coordinates = "lat lon"
        var.long_name = "Chlorophyll a concentration"
        var.standard_name = "mass_concentration_of_chlorophyll_a_in_sea_water"
        var.type = "surface"
        var.units = "milligram m^-3"
        var.missing_value = - 999.0
        # var.valid_min = 0.0100000000000000
        # var.valid_max = 300
        var.comment = "Brando VE, Sammartino M, Colella S, Bracaglia M, Di Cicco A, D’Alimonte D, Kajiyama T, Kaitala S and Attila J (2021). Phytoplankton Bloom Dynamics in the Baltic Sea Using a Consistently Reprocessed Time Series of Multi-Sensor Reflectance and Novel Chlorophyll-a Retrievals. Remote Sens. 13:3071. doi: 10.3390/rs13163071"
        var.source = "OLCI - POLYMER v.4.14 Atmospheric Correction Processor - BAL MLP Ensemble"



    def create_rrs_variable(self, array, varname, wl, varattr,product_type):
        var= self.create_data_variable(varname,array)
        var.coordinates = "lat long"
        var.band_name = varname.lower()
        wlstr = varname[3:].replace('_','.')
        if product_type=='cci':
            var.long_name = f'Multi-sensor Remote Sensing Reflectance at {wlstr} nm (Rrs_{wlstr})'
        else:
            var.long_name = f'Remote Sensing Reflectance at {wlstr}'
            var.central_wavelength = wl
        if varattr is not None:
            if 'RRS' in varattr.keys():
                for at in varattr['RRS']:
                    var.setncattr(at,varattr['RRS'][at])

        # var.standard_name = 'surface_ratio_of_upwelling_radiance_emerging_from_sea_water_to_downwelling_radiative_flux_in_air'
        # var.units = 'sr^-1'
        # var.type = 'surface'
        # var.source = 'OLCI - POLYMER v.4.14 Atmospheric Correction Processor'
        #var.valid_min = 0.000001
        #var.valid_max = 1

    def create_iop_variable(self, array, varname):
        var = self.create_data_variable(varname, array)
        var.coordinates = "lat long"
        var.long_name = self.iop_attributes[varname]['long_name']
        var.standard_name = self.iop_attributes[varname]['standard_name']
        var.type = self.iop_attributes[varname]['type']
        var.units = self.iop_attributes[varname]['units']
        var.comment = self.iop_attributes[varname]['comment']
        var.source = self.iop_attributes[varname]['source']
        var.missing_value = -999.0

    def create_kd_variable(self,array, varname):
        var = self.create_data_variable(varname, array)
        var.coordinates = "lat long"
        var.long_name = "OLCI Diffuse Attenuation Coefficient at 490 nm"
        var.standard_name = "volume_attenuation_coefficient_of_downwelling_radiative_flux_in_sea_water"
        var.missing_value = -999.0
        var.type = 'surface'
        var.units = 'm^-1'
        var.source = 'OLCI - POLYMER v.4.14 Atmospheric Correction Processor'

    def create_var_general(self,array,varname,varattr):
        var = self.create_data_variable(varname, array)
        var.coordinates = "lat long"
        if varattr is not None:
            if varname in varattr.keys():
                for at in varattr[varname]:
                    var.setncattr(at,varattr[varname][at])

    def create_var_flag_general(self, array, varname, varattr):
        var = self.create_flag_variable(varname, array)
        var.coordinates = "lat long"
        if varattr is not None:
            if varname in varattr.keys():
                for at in varattr[varname]:
                    var.setncattr(at, varattr[varname][at])

    def close_file(self):
        self.OFILE.close()