import argparse,os
import numpy as np
from netCDF4 import Dataset
from cfc_analysis import CFC_Analysis
from cfc_output import CFC_Output
from datetime import datetime as dt
from datetime import timedelta

parser = argparse.ArgumentParser(description='Check upload')
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", type=str, required=True,
                    choices=['TEST', 'CREATE_MASK', 'APPLY_MASK', 'CREATE_MASK_CFC','CREATE_CFC_OUTPUT_ANALYSIS','UPDATE_CFC','DISTANCE_MASK'])
parser.add_argument("-p", "--path", help="Input path")
parser.add_argument("-o", "--output_path", help="Output path")
parser.add_argument("-p_cfc","--path_cfc",help="Path to CFC (CLARA) files. Default: /store3/OC/CLOUD_COVER_CLARA_AVHRR_V003",default='/store3/OC/CLOUD_COVER_CLARA_AVHRR_V003')
parser.add_argument("-p_chl","--path_chl",help="Path to CHL files for coverage analysis. Default: /store3/OC/CCI_v2017/daily_v202411",default='/store3/OC/CCI_v2017/daily_v202411')

parser.add_argument("-mvar", "--mask_variable", help="Mask variable")
parser.add_argument("-fref", "--file_ref", help="File ref with lat/lon variables for masking")
parser.add_argument("-fmask", "--file_mask", help="File mask for APPLY_MASK")
parser.add_argument("-ifile", "--input_file_name", help="Input file name for APPLY_MASK, DATE is replaced by YYYYjjj")
parser.add_argument("-sd", "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-ed", "--end_date", help="Start date (yyyy-mm-dd)")
parser.add_argument("-pr", "--preffix", help="Preffix")
parser.add_argument("-sf", "--suffix", help="Suffix")
parser.add_argument("-if", "--ifile", help="iFile")


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

def create_mask_distance(file_mask,mask_variable,i_file):
    i_file = int(i_file)
    if not os.path.isfile(file_mask):
        print(f'[ERROR] {file_mask} is not a valid file or does not exist.')
        return

    dataset = Dataset(file_mask)
    mask_land = dataset.variables[mask_variable][:]


    indices_water = np.where(mask_land==0)
    indices_land = np.where(mask_land==1)
    y_water = indices_water[0]
    x_water = indices_water[1]
    y_land = indices_land[0]
    x_land = indices_land[1]
    n_water = len(y_water)
    n_land = len(y_land)
    dist_w = np.zeros((n_water))

    nrep = 50
    y_land_m = np.repeat(y_land.reshape((1, n_land)), nrep, axis=0).transpose()
    x_land_m = np.repeat(x_land.reshape((1, n_land)), nrep, axis=0).transpose()
    indices_col = np.arange(0,nrep)
    for i_water in range(0,n_water,nrep):
        if i_file>=0:
            limit_min = i_file*10000
            limit_max = limit_min+10000
            if i_water<limit_min:
                continue
            if i_water==limit_max:
                break
        print(f'[INFO] Processing {i_water} / {n_water}')
        i_min = i_water
        i_max = i_water+nrep
        if i_max>n_water:
            i_max = n_water
            nrep = i_max-i_min
            indices_col = np.arange(0, nrep)
            y_land_m = y_land_m[:,0:nrep]
            x_land_m = x_land_m[:,0:nrep]

        y_water_here = np.repeat(y_water[i_min:i_max],n_land).reshape((nrep,n_land)).transpose()
        x_water_here = np.repeat(x_water[i_min:i_max],n_land).reshape((nrep,n_land)).transpose()
        d_here = ((y_land_m - y_water_here) ** 2) + ((x_land_m - x_water_here) ** 2)
        index_min = np.argmin(d_here,axis=0)
        dist_w[i_min:i_max] = d_here[index_min,indices_col]





    # dist_w = np.zeros((n_water))
    #
    # for i_water in range(3000,3100):
    #     if (i_water%1000)==0:
    #         print(f'[INFO] Computing distances {i_water}/{n_water}')
    #     y_here = y_water[i_water]
    #     x_here = x_water[i_water]
    #     d_here = ((y_land-y_here)**2) + ((x_land-x_here)**2)
    #     index_min = np.argmin(d_here)
    #     print(i_water,'->',index_min,'->',d_here[index_min],'->',tal[i_water-3000])
    #     dist_w[i_water] = d_here[index_min]

    dist_w = np.sqrt(dist_w)
    dist_map = np.zeros(mask_land.shape)
    dist_map[indices_water]=dist_w

    if i_file<0:
        name_out = f'{os.path.basename(file_mask)[:-3]}_distance.nc'
    else:
        name_out = f'{os.path.basename(file_mask)[:-3]}_distance_{i_file}.nc'
    file_out = os.path.join(os.path.dirname(file_mask),name_out)
    ncout = Dataset(file_out,'w')

    # copy global attributes all at once via dictionary
    ncout.setncatts(dataset.__dict__)

    # copy dimensions
    for name, dimension in dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    # copy variables
    for name, variable in dataset.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,complevel=6)
        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(dataset[name].__dict__)
        ncout[name][:] = dataset[name][:]

    var_distance = ncout.createVariable('LandDistance','f4',dataset.variables[mask_variable].dimensions,fill_value=-999.0,zlib=True,complevel=6)
    var_distance[:] = dist_map[:]
    ncout.close()
    dataset.close()
    print('[INFO] Completed')



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
    from cfc_output import CFC_Output
    from datetime import datetime as dt
    from datetime import timedelta
    import os


    # dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/COVERAGE_ANALYSIS'
    # dir_data = dir_base
    # dir_cfc = dir_base
    # #dir_base = '/store/COP2-OC-TAC/BAL_Evolutions/slurmscripts_202411'
    # # dir_data = '/store3/OC/CCI_v2017/daily_v202411'
    # # dir_cfc = '/store3/OC/CLOUD_COVER_CLARA_AVHRR_V003'
    # file_mask = os.path.join(dir_base,'BAL_Land_Mask_hr_CFC.nc')
    #
    # cdfA = CFC_Analysis(file_mask)
    # cdfA.set_daily_paths(dir_cfc,dir_data)
    # work_date = dt(2004,3,4)
    # if cdfA.set_daily_data_date(work_date):
    #     cdfA.get_daily_cfc_cloud_free()
    #     cdfA.close_day()
    # # work_date = dt(1997,9,4)
    # # end_date = dt(2023,12,31)
    # # while work_date<=end_date:
    # #     if cdfA.set_daily_data_date(work_date):
    # #         cdfA.get_daily_cfc_cloud_free()
    # #         cdfA.close_day()
    # #     work_date = work_date + timedelta(hours=24)
    # cdfA.close_mask()

    # dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/COVERAGE_ANALYSIS'
    # output_file =  os.path.join(dir_base,'CoverageAnalysis_1.nc')
    # file_mask = os.path.join(dir_base,'BAL_Land_Mask_hr_CFC.nc')
    # cfcO = CFC_Output(output_file)
    # cfcO.start_output(file_mask,dt(2004,3,1),dt(2004,3,5))
    # cdfA = CFC_Analysis(file_mask)
    # cdfA.set_daily_paths(dir_base,dir_base)
    # work_date = dt(2004,3,4)
    # if cdfA.set_daily_data_date(work_date):
    #     results = cdfA.get_daily_cfc_cloud_free()
    #     cfcO.add_results(results,work_date)
    #     cdfA.close_day()
    # cfcO.close_output_stream()

    #dir_base = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION_202411/COVERAGE_ANALYSIS'
    dir_base = '/store3/OC/CCI_v2017/daily_v202411'
    file_out = os.path.join(dir_base,'CCICoverageWithCoast.csv')
    fw = open(file_out,'w')
    fw.write('DATE;NCHL')
    work_date = dt(1997,9,4)
    end_date = dt(2024,8,31)
    while work_date<=end_date:
        dir_date = os.path.join(dir_base,work_date.strftime('%Y'),work_date.strftime('%j'))
        name_file = f'C{work_date.strftime("%Y")}{work_date.strftime("%j")}-chl-bal-hr.nc'
        file_nc = os.path.join(dir_date,name_file)
        if os.path.exists(file_nc):
            print(f'[INFO] Working with date {work_date}')
            dataset = Dataset(file_nc)
            chl = dataset.variables['CHL'][:]
            dataset.close()
            nchl = np.ma.count(chl)
            line = f'{work_date.strftime("%Y-%m-%d")};{nchl}'
            fw.write('\n')
            fw.write(line)
        work_date = work_date + timedelta(hours=24)

    fw.close()

def main():
    if args.mode == 'TEST':
        make_test()
    if args.mode == 'DISTANCE_MASK':
        ifile = args.ifile if args.ifile else -1
        mask_variable = args.mask_variable if args.mask_variable else 'Land_Mask'
        create_mask_distance(args.path,mask_variable,ifile)


    if args.mode == 'CREATE_MASK_CFC':
        mask_variable = 'Land_Mask'
        if args.mask_variable:
            mask_variable = args.mask_variable
        create_mask_cfc(args.path, mask_variable, args.file_ref)

    if args.mode == 'CREATE_CFC_OUTPUT_ANALYSIS':
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        if not args.file_mask:
            print(f'[ERROR] -fmask (--file_mask) option is required')
            return
        if not args.output_path:
            print(f'[ERROR] -o (--output_path) option is required')
            return

        path_cfc = args.path_cfc
        path_chl = args.path_chl

        file_mask = args.file_mask
        output_path = args.output_path
        if not os.path.isfile(file_mask):
            print(f'[ERROR] Mask file {file_mask} does not exist or is not a valid file')
            return
        if not os.path.isdir(os.path.dirname(output_path)) and len(os.path.dirname(output_path))>0:
            print(f'[ERROR] Directory for output path {os.path.dirname(output_path)} does not exist or is not a valid directory')
            return
        if not os.path.isdir(path_cfc):
            print(f'[ERROR] CFC path {path_cfc} does not exist or is not a valid directory')
            return
        if not os.path.isdir(path_chl):
            print(f'[ERROR] CFC path {path_chl} does not exist or is not a valid directory')
            return

        cfcO = CFC_Output(output_path)
        cfcO.start_output(file_mask, start_date,end_date)
        cfcA = CFC_Analysis(file_mask)
        cfcA.set_daily_paths(path_cfc, path_chl)
        work_date = start_date
        while work_date<=end_date:
            if cfcA.set_daily_data_date(work_date):
                results = cfcA.get_daily_cfc_cloud_free()
                cfcO.add_results(results,work_date)
                cfcA.close_day()
            work_date = work_date + timedelta(hours=24)
        cfcA.close_mask()
        cfcO.close_output_stream()

    if args.mode == 'UPDATE_CFC':
        if not args.path_cfc:
            print(f'[ERROR] CFC file (option --path_cfc or -p_cfc) is required')
            return
        if not args.file_mask:
            print(f'[ERROR] Mask file (option --file_mask or -fmask) is required')
            return
        path_cfc = args.path_cfc
        file_mask = args.file_mask
        if not os.path.isfile(path_cfc):
            print(f'[ERROR] CFC file {path_cfc} does not exist or is not a valid file')
            return
        if not os.path.isfile(file_mask):
            print(f'[ERROR] Mask file {file_mask} does not exist of is not a valid file')
            return
        cfcO = CFC_Output(path_cfc)
        cfcO.update_output_file(file_mask)


def get_dates_from_arg():

    start_date = None
    end_date = None
    if args.start_date:
        try:
            start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        except:
            try:
                tdelta = int(args.start_date)
                start_date = dt.now() + timedelta(days=tdelta)
                start_date = start_date.replace(hour=12, minute=0, second=0, microsecond=0)
            except:
                print(f'[ERROR] Start date {args.start_date} is not in the correct format: YYYY-mm-dd or integer')
    if args.end_date:
        try:
            end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        except:
            try:
                tdelta = int(args.end_date)
                end_date = dt.now() + timedelta(days=tdelta)
                end_date = end_date.replace(hour=12, minute=0, second=0, microsecond=0)
            except:
                print(f'[ERROR] End date {args.end_date} is not in the correct format: YYYY-mm-dd or integer')
    if args.start_date and not args.end_date:
        end_date = start_date

    return start_date, end_date
if __name__ == '__main__':
    print('[INFO] Started map_tools')
    main()
    print('[INFO] Exit map tools')
