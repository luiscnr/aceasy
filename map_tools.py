import argparse
import numpy as np
from netCDF4 import Dataset

parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True,
                    choices=['TEST', 'CREATE_MASK', 'APPLY_MASK', 'CREATE_MASK_CFC'])
parser.add_argument("-p", "--path", help="Input path")
parser.add_argument("-o", "--output_path", help="Output path")
parser.add_argument("-mvar", "--mask_variable", help="Mask variable")
parser.add_argument("-fref", "--file_ref", help="File ref with lat/lon variables for masking")
parser.add_argument("-fmask", "--file_mask", help="File mask for APPLY_MASK")
parser.add_argument("-ifile", "--input_file_name", help="Input file name for APPLY_MASK, DATE is replaced by YYYYjjj")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pr", "--preffix", help="Preffix")
parser.add_argument("-sf", "--suffix", help="Suffix")


args = parser.parse_args()


def create_mask_cfc(file_mask, mask_variable, file_cfc):
    input_nc = Dataset(file_mask, 'r')
    land_mask = input_nc.variables[mask_variable][:]
    lat_array = input_nc.variables['lat'][:]
    lon_array = input_nc.variables['lon'][:]

    input_cfc = Dataset(file_cfc, 'r')
    lat_cfc = input_cfc.variables['lat'][:]
    lon_cfc = input_cfc.variables['lon'][:]
    input_cfc.close()

    nlat = lat_array.shape[0]
    nlon = lon_array.shape[0]
    y_cfc = np.zeros((nlat, nlon))
    x_cfc = np.zeros((nlat, nlon))
    index_cfc = np.zeros((nlat, nlon))
    y_cfc[:] = -999
    x_cfc[:] = -999
    index_cfc[:] = -999
    nlat_cfc = lat_cfc.shape[0]
    nlon_cfc = lon_cfc.shape[0]
    land_mask_cfc = np.zeros((nlat_cfc, nlon_cfc))

    for y in range(nlat):
        print(y, '/', nlat)
        for x in range(nlon):
            row, column = find_row_column_from_lat_lon(lat_cfc, lon_cfc, lat_array[y], lon_array[x])
            index = (row * nlon_cfc) + column
            y_cfc[y, x] = row
            x_cfc[y, x] = column
            index_cfc[y, x] = index
            if land_mask[y, x] == 1:
                land_mask_cfc[row, column] = 1

    y_middle = int(nlat_cfc / 2)
    x_ini = -1
    for x in range(nlon_cfc):
        if land_mask_cfc[y_middle, x] == 1:
            x_ini = x
            break
    print(f'[INFO] XIni: {x_ini}')
    if x_ini >= 0: land_mask_cfc[:, 0:x_ini] = 1

    x_end = -1
    for x in range(nlon_cfc - 1, 0, -1):
        if land_mask_cfc[y_middle, x] == 1:
            x_end = x + 1
            break
    print(f'[INFO] XEnd: {x_end}')
    if x_end < nlon_cfc: land_mask_cfc[:, x_end:nlon_cfc] = 1

    x_middle = int(nlon_cfc / 2)
    y_ini = -1
    for y in range(nlat_cfc):
        if land_mask_cfc[y, x_middle] == 1:
            y_ini = y
            break
    print(f'[INFO] YIni: {y_ini}')
    if y_ini >= 0: land_mask_cfc[0:y_ini, :] = 1
    y_end = -1
    for y in range(nlat_cfc - 1, 0, -1):
        if land_mask_cfc[y, x_middle] == 1:
            y_end = y + 1
            break
    print(f'[INFO] YEnd: {y_end}')
    if y_end < nlat_cfc: land_mask_cfc[y_end:nlat_cfc, :] = 1

    cfc_mask = land_mask.copy()
    for y in range(nlat):

        print(y, '-->', nlat)
        for x in range(nlon):
            row = int(y_cfc[y, x])
            column = int(x_cfc[y, x])
            # print(row,column)
            if land_mask_cfc[row, column] == 1:
                cfc_mask[y, x] = 1

    indices_water = index_cfc[cfc_mask == 0]
    indices_water_cfc = np.unique(indices_water)
    nindices = len(indices_water_cfc)
    ntotal_water_cfc = np.zeros((nindices,))
    for idx in range(nindices):
        index_here = indices_water_cfc[idx]
        ntotal_water_cfc[idx] = len(np.where(index_cfc==index_here)[0])

    ntotal_water_cfc_map = np.zeros(land_mask_cfc.shape)
    indices_water_cfc_yx = np.where(land_mask_cfc==0)
    ntotal_water_cfc_map[indices_water_cfc_yx]=ntotal_water_cfc[:]


    file_out = f'{file_mask[:-3]}_CFC.nc'
    print(f'[INFO] Creating output file: {file_out}')
    ncout = Dataset(file_out, 'w')
    ncout.createDimension('lat', nlat)
    ncout.createDimension('lon', nlon)
    ncout.createDimension('lat_cfc', nlat_cfc)
    ncout.createDimension('lon_cfc', nlon_cfc)
    ncout.createDimension('indices_cfc',nindices)

    for name, variable in input_nc.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue
        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             complevel=6)
        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_nc[name].__dict__)
        # copy data
        ncout[name][:] = input_nc[name][:]
    input_nc.close()

    var = ncout.createVariable('CFC_Mask', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    var[:] = cfc_mask
    var = ncout.createVariable('CFC_Y', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    var[:] = y_cfc
    var = ncout.createVariable('CFC_X', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    var[:] = x_cfc
    var = ncout.createVariable('CFC_Index', 'i2', ('lat', 'lon'), zlib=True, complevel=6)
    var[:] = index_cfc

    var_lat = ncout.createVariable('lat_cfc', 'f4', ('lat_cfc',), zlib=True, complevel=6, fill_value=-999)
    var_lat[:] = lat_cfc
    var_lon = ncout.createVariable('lon_cfc', 'f4', ('lon_cfc',), zlib=True, complevel=6, fill_value=-999)
    var_lon[:] = lon_cfc
    var_land = ncout.createVariable('Land_Mask_CFC', 'i2', ('lat_cfc', 'lon_cfc'), zlib=True, complevel=6)
    var_land[:] = land_mask_cfc
    var_ntotal_map = ncout.createVariable('NTotal_Water_Map_CFC', 'i2', ('lat_cfc', 'lon_cfc'), zlib=True, complevel=6)
    var_ntotal_map[:] = ntotal_water_cfc_map

    var_indices_water_cfc = ncout.createVariable('Indices_Water_CFC', 'i2', ('indices_cfc',), zlib=True, complevel=6)
    var_indices_water_cfc[:] = indices_water_cfc
    var_ntotal_water_cfc = ncout.createVariable('NTotal_Water_CFC', 'i2', ('indices_cfc',), zlib=True, complevel=6)
    var_ntotal_water_cfc[:] = ntotal_water_cfc

    ncout.close()

    print(f'[INFO] Completed')


def find_row_column_from_lat_lon(lat, lon, lat0, lon0):
    if contain_location(lat, lon, lat0, lon0):
        if lat.ndim == 1 and lon.ndim == 1:
            r = np.argmin(np.abs(lat - lat0))
            c = np.argmin(np.abs(lon - lon0))
        else:
            dist_squared = (lat - lat0) ** 2 + (lon - lon0) ** 2
            r, c = np.unravel_index(np.argmin(dist_squared),
                                    lon.shape)  # index to the closest in the latitude and longitude arrays
    else:
        r = np.nan
        c = np.nan
    return r, c

def contain_location(lat, lon, in_situ_lat, in_situ_lon):
    if lat.min() <= in_situ_lat <= lat.max() and lon.min() <= in_situ_lon <= lon.max():
        contain_flag = 1
    else:
        contain_flag = 0
    return contain_flag

def make_test():
    print('[INFO] TEST CODE')
    # file_cfc = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/MASKS/BAL_Land_Mask_hr_CFC.nc'
    # dset  =Dataset(file_cfc,'r')
    # variables = list(dset.variables.keys())
    # print(variables)
    # dset.close()
    from cfc_analysis import  CFC_Analysis
    from datetime import datetime as dt
    import os
    dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/COVERAGE_ANALYSIS'
    file_mask = os.path.join(dir_base,'BAL_Land_Mask_hr_CFC.nc')
    cdfA = CFC_Analysis(file_mask)
    cdfA.set_daily_paths(dir_base,dir_base)
    if cdfA.set_daily_data_date(dt(1998,7,30)):
        cdfA.get_daily_cfc_cloud_free()

def main():
    if args.mode == 'TEST':
        make_test()
    if args.mode == 'CREATE_MASK_CFC':
        mask_variable = 'Land_Mask'
        if args.mask_variable:
            mask_variable = args.mask_variable
        create_mask_cfc(args.path, mask_variable, args.file_ref)


if __name__ == '__main__':
    print('[INFO] Started map_tools')
    main()
    print('[INFO] Exit map tools')
