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
            'n_coverage_map': ('time', 'indices_cfc'),
            'n_bloom_map': ('time', 'indices_cfc'),
            'n_nobloom_map': ('time', 'indices_cfc'),
            'n_subsurface_map': ('time', 'indices_cfc'),
            'n_surface_map': ('time', 'indices_cfc'),
            'n_concurrent_map': ('time', 'indices_cfc'),
            'n_coverage': ('time',),
            'n_bloom': ('time',),
            'n_nobloom': ('time',),
            'n_subsurface': ('time',),
            'n_surface': ('time',),
            'n_concurrent': ('time',)
        }

    def update_output_file(self, file_mask):
        if not os.path.isfile(self.output_file):
            return

        mask_dataset = Dataset(file_mask)
        ntotal_water_cfc = mask_dataset.variables['NTotal_Water_CFC'][:]
        mask_dataset.close()

        input_dataset = Dataset(self.output_file)
        n_expected_map = input_dataset.variables['n_expected_map'][:]
        n_coverage_map = input_dataset.variables['n_coverage_map'][:]
        n_expected = input_dataset.variables['n_expected_sum'][:]
        daily_cloud_free_map = input_dataset.variables['daily_cloud_free_map'][:]
        time = input_dataset.variables['time'][:]



        n_coverage_map_corrected = n_coverage_map.copy()
        n_coverage_map_corrected[n_expected_map.mask] = np.ma.masked

        n_coverage_corrected = np.ma.sum(n_coverage_map_corrected, axis=1)

        p_coverage_cf_map = np.ma.zeros(n_coverage_map_corrected.shape)
        p_coverage_cf_map[n_coverage_map_corrected.mask] = np.ma.masked
        p_coverage_cf_map[np.logical_and(n_expected_map == 0, n_coverage_map_corrected == 0)] = np.ma.masked
        p_coverage_cf_map[np.logical_and(n_expected_map == 0, n_coverage_map_corrected > 0)] = 100
        p_coverage_cf_map[np.logical_and(n_expected_map > 0, n_coverage_map_corrected == 0)] = 3
        p_coverage_cf_map[np.logical_and(n_expected_map > 0, n_coverage_map_corrected > 0)] = 4
        p_coverage_cf_map[p_coverage_cf_map >= 3] = (n_coverage_map_corrected[p_coverage_cf_map >= 3] / n_expected_map[
            p_coverage_cf_map >= 3]) * 100
        p_coverage_cf_map[p_coverage_cf_map > 100] = 100

        p_coverage_cf = np.ma.zeros(n_coverage_corrected.shape)
        p_coverage_cf[n_coverage_corrected.mask] = np.ma.masked
        p_coverage_cf[np.logical_and(n_expected == 0, n_coverage_corrected == 0)] = np.ma.masked
        p_coverage_cf[np.logical_and(n_expected == 0, n_coverage_corrected > 0)] = 100
        p_coverage_cf[np.logical_and(n_expected > 0, n_coverage_corrected == 0)] = 3
        p_coverage_cf[np.logical_and(n_expected > 0, n_coverage_corrected > 0)] = 4
        p_coverage_cf[p_coverage_cf >= 3] = (n_coverage_corrected[p_coverage_cf >= 3] / n_expected[
            p_coverage_cf >= 3]) * 100
        p_coverage_cf[p_coverage_cf > 100] = 100

        p_coverage_map = np.ma.masked_all(n_coverage_map_corrected.shape)
        for idx in range(n_coverage_map_corrected.shape[0]):
            p_coverage_map[idx, :] = (n_coverage_map_corrected[idx, :] / ntotal_water_cfc[:]) * 100

        p_coverage = (n_coverage_corrected / np.sum(ntotal_water_cfc)) * 100

        time_extended = np.zeros(n_coverage_map_corrected.shape)
        for idx in range(n_coverage_map_corrected.shape[0]):
            time_extended[idx, :] = time[idx]

        time_extended = time_extended.flatten()
        f_coverage = p_coverage_map.flatten()
        f_coverage_cf = p_coverage_cf_map.flatten()
        f_cloud_free = daily_cloud_free_map.flatten()



        time_extended = time_extended[f_coverage.mask == False]
        f_cloud_free = f_cloud_free[f_coverage.mask == False]
        f_coverage_cf = f_coverage_cf[f_coverage.mask == False]
        f_coverage = f_coverage[f_coverage.mask == False]
        num_flattened = len(f_coverage)

        # porc_ref = np.arange(0,101,5)
        # nporc = len(porc_ref)
        # ntotal = np.zeros(nporc)
        # nvalid = np.zeros(nporc)
        # for idx in range(num_flattened):
        #
        #     if f_cloud_free[idx]==0:
        #         continue
        #     cf_here = np.floor(f_cloud_free[idx] / 5)
        #     # # if f_cloud_free[idx]==0:
        #     # #     is_valid = 1 if f_coverage[idx]==0 else 0
        #     # # else:
        #     is_valid = 1 if f_coverage[idx]>=f_cloud_free[idx] else 0
        #     ntotal[int(cf_here)] = ntotal[int(cf_here)]+1
        #     nvalid[int(cf_here)] = nvalid[int(cf_here)] + is_valid
        # file_kk = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/COVERAGE_ANALYSIS/PLOTS/nvalid.csv'
        # fw = open(file_kk, 'w')
        # fw.write('Porc;NTotal;NValid;%Valid')
        # for idx in range(nporc):
        #     fw.write('\n')
        #     fw.write(f'{idx};{ntotal[idx]};{nvalid[idx]};{nvalid[idx]/ntotal[idx]}')
        # fw.close()

        # file_kk = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/COVERAGE_ANALYSIS/PLOTS/median_era_1.csv'
        # start_time = dt(1997,9,1).replace(tzinfo=pytz.utc).timestamp()
        # end_time = dt(2002,4,28).replace(tzinfo=pytz.utc).timestamp()
        # valid_era = np.logical_and(time_extended>=start_time,time_extended<=end_time)
        # f_coverage = f_coverage[valid_era]
        # f_cloud_free = f_cloud_free[valid_era]
        # increm = 5
        # porc_min = np.arange(0, 101, increm)
        # nporc = len(porc_min)
        # median_f_coverage = np.zeros((nporc))
        # avg_f_coverage = np.zeros((nporc))
        # for iporc in range(nporc):
        #     min_value = porc_min[iporc]
        #     max_value = porc_min[iporc]+increm
        #     # if min_value==0:
        #     #     values = f_coverage[np.logical_and(f_cloud_free>min_value,f_cloud_free<max_value)]
        #     # else:
        #     values = f_coverage[np.logical_and(f_cloud_free >= min_value, f_cloud_free < max_value)]
        #     median_f_coverage[iporc] = np.median(values)
        #     avg_f_coverage[iporc] = np.mean(values)
        # input_dataset.close()
        # fw = open(file_kk, 'w')
        # fw.write('Index;MinCloudFree;MaxCloudFree;MedianCoverage')
        # for idx in range(nporc):
        #     fw.write('\n')
        #     fw.write(f'{idx};{porc_min[idx]};{porc_min[idx]+increm};{median_f_coverage[idx]};{avg_f_coverage[idx]}')
        # fw.close()

        print(f'[INFO] Creating copy...')
        file_temp = os.path.join(os.path.dirname(self.output_file),'Temp.nc')
        ncout = Dataset(file_temp, 'w', format='NETCDF4')
        # copy global attributes all at once via dictionary
        ncout.setncatts(input_dataset.__dict__)
        # copy dimensions
        for name, dimension in input_dataset.dimensions.items():
            ncout.createDimension(
                name, (len(dimension) if not dimension.isunlimited() else None))
        #ncout.createDimension('indices_flatten',num_flattened)

        for name, variable in input_dataset.variables.items():
            fill_value = None
            if '_FillValue' in list(variable.ncattrs()):
                fill_value = variable._FillValue
            ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,complevel=6)
            # copy variable attributes all at once via dictionary
            ncout[name].setncatts(input_dataset[name].__dict__)
            # copy data
            ncout[name][:] = input_dataset[name][:]

        ##input_dataset.close()
        #
        # ##new variables
        # # var1 = ncout.createVariable('n_coverage_map_corrected','f4',('time','indices_cfc'),fill_value=-999.0,zlib=True,complevel=6)
        # # var1[:] = n_coverage_map_corrected
        # #
        # # var2 = ncout.createVariable('n_coverage_corrected', 'f4', ('time',), fill_value=-999.0,zlib=True, complevel=6)
        # # var2[:] = n_coverage_corrected
        # #
        # # var3 = ncout.createVariable('p_coverage_cf_map', 'f4', ('time', 'indices_cfc'), fill_value=-999.0,zlib=True, complevel=6)
        # # var3[:] = p_coverage_cf_map
        # #
        # # var4 = ncout.createVariable('p_coverage_cf', 'f4', ('time',), fill_value=-999.0, zlib=True, complevel=6)
        # # var4[:] = p_coverage_cf
        # #
        # # var5 = ncout.createVariable('p_coverage_map', 'f4', ('time', 'indices_cfc'), fill_value=-999.0,zlib=True, complevel=6)
        # # var5[:] = p_coverage_map
        # #
        # # var6 = ncout.createVariable('p_coverage', 'f4', ('time',), fill_value=-999.0, zlib=True, complevel=6)
        # # var6[:] = p_coverage
        # #
        # # var7 = ncout.createVariable('f_time', 'f4', ('indices_flatten',), fill_value=-999.0, zlib=True, complevel=6)
        # # var7[:] = time_extended
        # #
        # # var8 = ncout.createVariable('f_coverage', 'f4', ('indices_flatten',), fill_value=-999.0, zlib=True, complevel=6)
        # # var8[:] = f_coverage
        # #
        # # var9 = ncout.createVariable('f_cloud_free', 'f4', ('indices_flatten',), fill_value=-999.0, zlib=True, complevel=6)
        # # var9[:] = f_cloud_free

        # var10 = ncout.createVariable('f_coverage_cf', 'f4', ('indices_flatten',), fill_value=-999.0, zlib=True, complevel=6)
        # var10[:] = f_coverage_cf

        ncout.close()
        os.rename(file_temp,self.output_file)
        print(f'[INFO] Completed')

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

        lat = self.ncout.createVariable('lat_cfc', 'f4', ('lat_cfc',), complevel=6, zlib=True)
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

    def add_results(self, results, work_date):
        work_date = work_date.replace(hour=0, minute=0, second=0, microsecond=0)
        index_date = (work_date - dt.strptime(self.ncout.start_date, '%Y-%m-%d')).days
        print(f'[INFO] Addding data for date: {work_date.strftime("%Y-%m-%d")}')
        for name_var in self.variables_cfc_analysis:
            if name_var in results:
                array = results[name_var]
                self.ncout[name_var][index_date] = array

    def close_output_stream(self):
        if self.ncout is not None: self.ncout.close()
