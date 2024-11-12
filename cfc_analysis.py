from netCDF4 import Dataset
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import os


class CFC_Analysis():

    def __init__(self, file_mask):
        self.nc_mask = None
        self.nlat_cfc = -1
        self.nlon_cfc = -1
        self.nlat_data = -1
        self.nlon_data = -1
        if os.path.isfile(file_mask):
            self.nc_mask = Dataset(file_mask)
            to_check = ['lat', 'lon', 'Land_Mask', 'CFC_Mask', 'CFC_Y', 'CFC_X', 'CFC_Index', 'lat_cfc', 'lon_cfc',
                        'Land_Mask_CFC', 'NTotal_Water_Map_CFC', 'Indices_Water_CFC', 'NTotal_Water_CFC']
            valid = True
            for var in to_check:
                if var not in self.nc_mask.variables:
                    valid = False
                    print(
                        f'[ERROR] CFC Analysis could not be started as variable {var} is not available in mask file {file_mask}.')
                    print(f'[ERROR] Please use CREATE_MASK_CFC option to create a valid CFC mask file')
            if valid:
                self.nlat_cfc = len(self.nc_mask.variables['lat_cfc'][:])
                self.nlon_cfc = len(self.nc_mask.variables['lon_cfc'][:])
                self.nlat_data = len(self.nc_mask.variables['lat'][:])
                self.nlon_data = len(self.nc_mask.variables['lon'][:])
            else:
                self.nc_mask = None

        # date work
        self.work_date = None

        # cfc input
        self.nc_input_cfc = None
        self.cfc_day = None

        # data input
        self.input_data = None

        # paths
        self.path_data_daily = None
        self.path_cfc_daily = None

    def set_daily_paths(self, path_cfc, path_data):
        if os.path.isdir(path_cfc): self.path_cfc_daily = path_cfc
        if os.path.isdir(path_data): self.path_data_daily = path_data

    def set_daily_data_date(self, work_date):
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        if os.path.isdir(self.path_cfc_daily):
            name_cfc = f'CFCdm{work_date.strftime("%Y%m%d")}0000003UDAVPOS01UD.nc'
            file_cfc = os.path.join(self.path_cfc_daily, yyyy, name_cfc)
            if not os.path.isfile(file_cfc):
                print(f'[ERROR] CFC file {file_cfc} is not available')
                return False
        else:
            return False
        if os.path.isdir(self.path_data_daily):
            name_data = f'C{work_date.strftime("%Y%j")}-chl-bal-hr.nc'
            file_data = os.path.join(self.path_data_daily, yyyy, jjj, name_data)
            if not os.path.isfile(file_data):
                print(f'[ERROR] Data file {file_data} is not available')
                return False
        else:
            return False

        return self.set_daily_data(file_cfc, file_data)

    def set_daily_data(self, file_cfc, file_data):
        self.set_input_cfc(file_cfc)
        if self.work_date is None or self.nc_input_cfc is None or self.cfc_day is None:
            return False
        self.set_input_data(file_data)
        if self.input_data is None:
            return False
        return True

    def set_input_cfc(self, file_cfc):
        if os.path.isfile(file_cfc) and self.nc_mask is not None:
            self.nc_input_cfc = Dataset(file_cfc)
            if not 'cfc_day' in self.nc_input_cfc.variables or not 'time' in self.nc_input_cfc.variables:
                self.nc_input_cfc.close()
                self.nc_input_cfc = None
                print(
                    f'[ERROR] Input cfc could not be started from file {file_cfc} Variables cdf_day or time are not available')
            self.cfc_day = np.ma.squeeze(self.nc_input_cfc.variables['cfc_day'][:])
            if self.cfc_day.shape[0] != self.nlat_cfc and self.cfc_day.shape[1] != self.nlon_cfc:
                print(f'[ERROR] Dimensions of input variable cfc_day do not corrrespond with dimensions in mask')
                self.nc_input_cfc.close()
                self.nc_input_cfc = None
                self.work_date = None
                return
            date_here = dt(1970, 1, 1, 0, 0, 0) + timedelta(days=int(self.nc_input_cfc.variables['time'][0]))

            if self.work_date is None:

                self.work_date = date_here
            else:
                if self.work_date.strftime('%Y%m%d') != date_here.strftime('%Y%m%d'):
                    print(f'[ERROR] Dates from daily CFC and input data are not the same.')
                    self.nc_input_cfc.close()
                    self.nc_input_cfc = None
                    self.work_date = None
        else:
            print(
                f'[ERROR] Input CFC could not be started. File {file_cfc} is not valid or mask was not set in the previou step')

    def set_input_data(self, file_data):
        if os.path.isfile(file_data) and self.nc_mask is not None:
            nc_input = Dataset(file_data)
            if not 'CHL' in nc_input.variables or not 'time' in nc_input.variables:
                nc_input.close()
                print(
                    f'[ERROR] Input data could not be started from file {file_data} Variables CHL or time are not available')
                return
            self.input_data = np.ma.squeeze(nc_input.variables['CHL'][:])
            if self.input_data.shape[0] != self.nlat_data and self.input_data.shape[1] != self.nlon_data:
                print(f'[ERROR] Dimensions of input variable CHL do not corrrespond with dimensions in mask')
                self.input_data = None

            date_here = dt(1981, 1, 1, 0, 0, 0) + timedelta(seconds=int(nc_input.variables['time'][0]))

            if self.work_date is None:
                self.work_date = date_here
            else:
                if self.work_date.strftime('%Y%m%d') != date_here.strftime('%Y%m%d'):
                    print(f'[ERROR] Date from input data is not the same as previoulsy assigned to input CFC')
                    self.input_data = None
                    self.work_date = None

            nc_input.close()
        else:
            print(
                f'[ERROR] Input data could not be started. File {file_data} is not valid or mask was not set in the previou step')

    def get_daily_cfc_cloud_free(self):
        cfc_mask = self.nc_mask['Land_Mask_CFC'][:]
        ntotal_water_cfc = self.nc_mask['NTotal_Water_CFC'][:]
        nindices = len(ntotal_water_cfc)
        daily_cloud_free_map = 100 - self.cfc_day[cfc_mask == 0]
        nindices_valid = np.ma.count(daily_cloud_free_map)  ##between 0 and nindices(=721)
        n_expected_map = np.ma.round((daily_cloud_free_map / 100) * ntotal_water_cfc)
        thersholds = [10, 25, 40, 50, 60, 75, 90]
        nth = len(thersholds)
        daily_cloud_free_p_map = np.ma.zeros((nth, nindices))
        n_expected_p_map = np.ma.zeros((nth, nindices))
        for i in range(nth):
            th = thersholds[i]
            daily_cloud_free_p_map[i, daily_cloud_free_map >= th] = 1
            daily_cloud_free_p_map[i, daily_cloud_free_map.mask] = np.ma.masked
            n_expected_p_map[i, :] = daily_cloud_free_p_map[i, :] * ntotal_water_cfc[:]

        sum_n_total_water_cfc = np.ma.sum(ntotal_water_cfc[~daily_cloud_free_map.mask])

        daily_cloud_free_sum = np.ma.sum(daily_cloud_free_map)
        daily_cloud_free_percent = (daily_cloud_free_sum / (nindices_valid * 100)) * 100
        n_expected_sum = np.ma.sum(n_expected_map)
        n_expected_percent = (n_expected_sum / sum_n_total_water_cfc) * 100

        daily_cloud_free_p_sum = np.ma.sum(daily_cloud_free_p_map, axis=1)
        daily_cloud_free_p_percent = (daily_cloud_free_p_sum / (nindices_valid)) * 100
        n_expected_p_sum = np.ma.sum(n_expected_p_map, axis=1)
        n_expected_p_percent = (n_expected_p_sum / sum_n_total_water_cfc) * 100

        results = {
            'daily_cloud_free_map': daily_cloud_free_map,
            'n_expected_map': n_expected_map,
            'daily_cloud_free_p_map': daily_cloud_free_p_map,
            'n_expected_p_map': n_expected_p_map,
            'nindices_valid': nindices_valid,
            'sum_n_total_water_cfc': sum_n_total_water_cfc,
            'daily_cloud_free_sum':daily_cloud_free_sum,
            'daily_cloud_free_percent':daily_cloud_free_percent,
            'n_expected_sum':n_expected_sum,
            'n_expected_percent':n_expected_percent,
            'daily_cloud_free_p_sum': daily_cloud_free_p_sum,
            'daily_cloud_free_p_percent': daily_cloud_free_p_percent,
            'n_expected_p_sum': n_expected_p_sum,
            'n_expected_p_percent': n_expected_p_percent
        }
        return results


    def close_mask(self):
        if self.nc_mask is not None: self.nc_mask.close()

    def close_day(self):
        if self.nc_input_cfc is not None: self.nc_input_cfc.close()
        self.work_date = None
        self.nc_input_cfc = None
        self.cfc_day = None
        self.input_data = None
