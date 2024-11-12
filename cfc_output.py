from netCDF4 import Dataset
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import os, pytz


class CFC_Output():

    def __init__(self, output_file):
        self.output_file = output_file
        self.ncout = None
        self.variables_cfc_analysis = {
            'daily_cloud_free_map': ('time', 'indices_cfc'),
            'n_expected_map': ('time', 'indices_cfc'),
            'daily_cloud_free_p_map': ('time', 'indices_porc', 'indices_cfc'),
            'n_expected_p_map': ('time', 'indices_porc', 'indices_cfc'),
            'nindices_valid': ('time',),
            'sum_n_total_water_cfc': ('time',),
            'daily_cloud_free_sum': ('time',),
            'daily_cloud_free_percent': ('time',),
            'n_expected_sum': ('time',),
            'n_expected_percent': ('time',),
            'daily_cloud_free_p_sum': ('time', 'indices_porc'),
            'daily_cloud_free_p_percent': ('time', 'indices_porc'),
            'n_expected_p_sum': ('time', 'indices_porc'),
            'n_expected_p_percent': ('time', 'indices_porc'),
        }

    def start_output(self, file_mask, start_date, end_date):
        if not os.path.isfile(file_mask):
            print(f'[ERROR] File mask {file_mask} does not exist or  is not valid file')
            return
        try:
            dataset = Dataset(file_mask)
            if not 'Indices_Water_CFC' in dataset.variables:
                print(f'[ERROR] Variable Indices_Water_CFC is not avilable in file mask {file_mask}')
                return
            if not 'Land_Mask_CFC' in dataset.variables:
                print(f'[ERROR] Variable Land_Mask_CFC is not avilable in file mask {file_mask}')
                return
            indices_water_cfc = dataset.variables['Indices_Water_CFC'][:]
            n_indices_cfc = indices_water_cfc.shape[0]
            land_mask_cfc = dataset.variables['Land_Mask_CFC'][:]
            n_lat_cfc = land_mask_cfc.shape[0]
            n_lon_cfc = land_mask_cfc.shape[1]
            lat_cfc = dataset.variables['lat_cfc'][:]
            lon_cfc = dataset.variables['lon_cfc'][:]
            dataset.close()
        except:
            print(f'[ERROR] File mask {file_mask} is not a valid NetCDF file')
            return
        try:
            self.ncout = Dataset(self.output_file, 'w')
        except:
            print(f'[ERROR] Output file {self.output_file}could not be started. Please review path and permissions')
            return

        self.ncout.createDimension('indices_cfc', n_indices_cfc)
        self.ncout.createDimension('lat_cfc', n_lat_cfc)
        self.ncout.createDimension('lon_cfc', n_lon_cfc)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        n_days = (end_date - start_date).days + 1
        self.ncout.createDimension('time', n_days)
        self.ncout.createDimension('indices_porc', 7)

        self.ncout.start_date = start_date.strftime('%Y-%m-%d')
        self.ncout.end_date = end_date.strftime('%Y-%m-%d')

        lat = self.ncout.createVariable('lat_cfc','f4', ('lat_cfc',) ,complevel=6,zlib=True)
        lat[:] = lat_cfc[:]
        lon = self.ncout.createVariable('lon_cfc', 'f4', ('lon_cfc',), complevel=6, zlib=True)
        lon[:] = lon_cfc[:]

        icf = self.ncout.createVariable('Indices_Water_CFC', 'i2', ('indices_cfc',), complevel=6, zlib=True)
        icf[:] = indices_water_cfc[:]
        lm = self.ncout.createVariable('Land_Mask_CFC', 'i2', ('lat_cfc', 'lon_cfc'), complevel=6, zlib=True)
        lm[:] = land_mask_cfc[:]


        time_var = self.ncout.createVariable('time', 'f8', ('time',), complevel=6, zlib=True)
        time_array = []
        work_date = start_date
        while work_date <= end_date:
            time_array.append(work_date.replace(tzinfo=pytz.UTC).timestamp())
            work_date = work_date + timedelta(hours=24)
        time_var[:] = np.array(time_array)

        for name_var in self.variables_cfc_analysis:
            dims = self.variables_cfc_analysis[name_var]
            self.ncout.createVariable(name_var, 'f4', dims, complevel=6, zlib=True, fill_value=-999.0)

    def add_results(self,results,work_date):
        work_date = work_date.replace(hour=0,minute=0,second=0,microsecond=0)
        index_date = (work_date-dt.strptime(self.ncout.start_date,'%Y-%m-%d')).days
        print(f'[INFO] Addding data for date: {work_date.strftime("%Y-%m-%d")}')
        for name_var in self.variables_cfc_analysis:
            if name_var in results:
                array = results[name_var]
                self.ncout[name_var][index_date] = array



    def close_output_stream(self):
        if self.ncout is not None: self.ncout.close()
