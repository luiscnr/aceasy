import os.path
from netCDF4 import Dataset
import numpy as np
from datetime import datetime as dt
from add_qi import QI_ADD

class Splitter():
    def __init__(self,file_ini,date_file):
        self.file_ini = file_ini
        self.path_out = os.path.dirname(file_ini)
        self.prename = 'O'
        self.resolution = 'fr'
        self.area = 'bal'
        self.add_qi = True
        self.variable_input_lat = 'lat'
        self.variable_input_lon = 'lon'
        self.dimension_input_lat = 'lat'
        self.dimension_input_lon = 'lon'
        self.date_file  = date_file
        self.epoch_copernicus =dt(1981, 1, 1,0,0,0)
        self.mask_array  =None

    def make_multiple_split(self,output_path,splits):
        for ref in splits:
            self.make_split(output_path,ref,splits[ref])

    def make_split(self,output_path,ref,var_list):
        print(f'[INFO] Making split from {self.file_ini}')

        if os.path.isdir(output_path):
            yyyyjjj = self.date_file.strftime('%Y%j')
            ofile = os.path.join(output_path,f'{self.prename}{yyyyjjj}-{ref}-{self.area}-{self.resolution}.nc')
        else:
            ofile = output_path

        print(f'[INFO] Output file: {ofile}')

        nc_input = Dataset(self.file_ini)
        ny = len(nc_input.dimensions[self.dimension_input_lat])
        nx = len(nc_input.dimensions[self.dimension_input_lon])

        # flags to be used if --lowerleft is specified
        flip_latlon = {
            'lat': True if nc_input.variables[self.variable_input_lat][0] > nc_input.variables[self.variable_input_lat][ny - 1] else False,
            'lon': True if nc_input.variables[self.variable_input_lon][0] > nc_input.variables[self.variable_input_lon][nx - 1] else False
        }

       



        ncout = Dataset(ofile,'w')

        ##Dimensions
        ncout.createDimension('lat', ny)
        ncout.createDimension('lon', nx)
        ncout.createDimension('time', 1)



        ##Global attributes
        ncout.setncatts(nc_input.__dict__)

        ##Time variable
        ttime = ncout.createVariable('time', np.int32, ('time',))
        ttime.units = 'seconds since 1981-01-01'
        ttime.long_name =  'reference time'
        ttime.standard_name = 'time'
        ttime.axis = 'T'
        ttime.calendar = 'Gregorian'
        ttime[0] = int((self.date_file - self.epoch_copernicus).total_seconds())

        lat_lon_variables = {
            'lat': self.variable_input_lat,
            'lon': self.variable_input_lon
        }
        for v in lat_lon_variables:
            vin = nc_input.variables[lat_lon_variables[v]]
            ov = ncout.createVariable(v, vin.datatype, vin.dimensions, zlib=True, complevel=6)
            ov.setncatts(vin.__dict__)
            if flip_latlon[v]:
                vin = np.flipud(vin)
            ov[:] = vin[:]

        # create the variables
        for vname in var_list:
            vin = nc_input.variables[vname]
            if '_FillValue' in vin.ncattrs():
                fillvalue = vin.getncattr('_FillValue')

            dims_out = vin.dimensions
            if dims_out[0]!='time':
                dims_out = ('time',)+vin.dimensions
            ov = ncout.createVariable(vname, vin.datatype, dims_out, fill_value=fillvalue, zlib=True,
                                 complevel=6)
            ov.setncatts(vin.__dict__)

            vin = vin[:]
            vin = np.squeeze(vin)

            if flip_latlon['lat']:
                vin = np.flipud(vin)
            if flip_latlon['lon']:
                vin = np.fliplr(vin)
            if self.mask_array is not None:
                vin[self.mask_array==1]=fillvalue

            ov[0, :, :] = vin[:, :]

        nc_input.close()

        ##adding qi
        if self.add_qi:
            qiadd  = QI_ADD(ncout,None,self.date_file)
            qibands = qiadd.add_qi_bal(var_list)
            if qibands is not None and self.mask_array is not None:
                for qiband in qibands:
                    vin = ncout.variables[qiband]
                    fillvalue = -999.0
                    if '_FillValue' in vin.ncattrs():
                        fillvalue = vin.getncattr('_FillValue')
                    array = np.squeeze(np.array(vin[:]))
                    if self.mask_array is not None:
                        array[self.mask_array == 1] = fillvalue
                    vin[0,:,:] = array[:,:]
            qiadd.close_input_dataset()
        else:
            ncout.close()

        print(f'[INFO] Completed')

