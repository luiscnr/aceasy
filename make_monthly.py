import argparse, configparser, calendar, os
import shutil
from datetime import timedelta
from datetime import datetime as dt
from netCDF4 import Dataset
import numpy as np

parser = argparse.ArgumentParser(description="Make CMEMS monthly processing")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-c", "--config_file",
                    help="Config file, it should be a section [MONTHLY_PROCESSING] with all the options", required=True)
parser.add_argument("-sd", "--start_date", help="Start date", required=True)
parser.add_argument("-ed", "--end_date", help="End date")
args = parser.parse_args()

def test():
    print('TEST')
    from datetime import datetime as dt
    from datetime import timedelta
    work_date = dt(2024,8,1)
    end_date = dt(2024,8,31)
    dir_base = '/dst04-data1/OC/OLCI/daily_v202311_bc'
    output_dir = '/store/COP2-OC-TAC/temp'
    while work_date<=end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        input_file = os.path.join(dir_base,yyyy,jjj,f'O{yyyy}{jjj}-chl-bal-fr.nc')
        output_dir_year = os.path.join(output_dir,yyyy)
        if not os.path.exists(output_dir_year):
            os.mkdir(output_dir_year)
        output_dir_jday = os.path.join(output_dir_year,jjj)
        if not os.path.exists(output_dir_jday):
            os.mkdir(output_dir_jday)
        output_file = os.path.join(output_dir_jday,f'O{yyyy}{jjj}-chl-bal-fr.nc')
        if os.path.exists(input_file):
            shutil.copy(input_file,output_file)
        work_date = work_date + timedelta(hours=24)
    return True
def main():
    if test():
        return
    print('[INFO] Started MONTHLY PROCESSING')
    start_date, end_date = get_dates_from_arg()
    if start_date is None or end_date is None:
        return
    if not os.path.isfile(args.config_file):
        print(f'[ERROR] {args.config_file} does not exist or is not a valid file.')
        return
    try:
        options = configparser.ConfigParser()
        options.read(args.config_file)
    except:
        print(f'[ERROR] Error parsing the config file: {args.config_file}')
        return

    section = 'MONTHLY_PROCESSING'
    if not options.has_section(section):
        print(f'[ERROR] Config file {args.config_file} should contain a section called {section}')
        return
    keys = ['input_dir', 'name_file_format', 'name_file_output_format', 'variable']
    for key in keys:
        if not options.has_option(section, key):
            print(f'[ERROR] Option {key} in section {section} is required')
            return
    input_dir = options[section]['input_dir']
    if not os.path.isdir(input_dir):
        print(f'[ERROR] {input_dir} does not exist or is not a valid directory')
        return
    name_file_format = options[section]['name_file_format']
    name_file_format_date = '%Y%j'
    if options.has_option(section, 'name_file_format_date'):
        name_file_format_date = options[section]['name_file_format_date']
    name_file_output_format = options[section]['name_file_output_format']
    name_file_output_format_date = '%Y%j%j'
    if options.has_option(section, 'name_file_output_format_date'):
        name_file_format_date = options[section]['name_file_output_format_date']

    output_dir = input_dir
    if options.has_option(section, 'output_dir'):
        output_dir = options[section]['output_dir']
    if not os.path.isdir(output_dir):
        try:
            os.mkdir(output_dir)
        except:
            print(f'[ERROR] Output directory {output_dir} is not a valid directory and could not be created')
            return

    options_impl = {
        'input_dir': input_dir,
        'output_dir': output_dir,
        'name_file_format': name_file_format,
        'name_file_format_date': name_file_format_date,
        'name_file_output_format': name_file_output_format,
        'name_file_output_format_date': name_file_output_format_date,
        'variable': options[section]['variable']
    }
    work_date = start_date
    months_done = []
    while work_date <= end_date:
        ym = work_date.strftime('%Y%m')
        if ym not in months_done:
            months_done.append(ym)
            launch_monthly_processing_impl(options_impl, work_date.year, work_date.month)
        work_date = work_date + timedelta(hours=24)


def launch_monthly_processing_impl(options, year, month):
    print(f'[INFO] Launching monthly processing for {year}-{month}')
    first_date = dt(year, month, 1)
    last_date = dt(year, month, calendar.monthrange(year, month)[1])
    file_out = None
    if options['name_file_output_format_date'] == '%Y%j%j':
        date_out_str = f'{first_date.strftime("%Y%j")}{last_date.strftime("%j")}'
        name_out = options['name_file_output_format'].replace('DATE', date_out_str)
        file_out = os.path.join(options['output_dir'], first_date.strftime('%Y'), name_out)
    if file_out is None:
        print(
            f'[ERROR] Output file could not be started. Review name_file_output_format_date option, you should use %%Y%%j%%j')
        return

    n_days = calendar.monthrange(year, month)[1]
    list_files = [None] * n_days
    variable = options['variable']
    var_avg_name = variable.upper()
    var_avg_count_name = f'{variable.upper()}_count'
    var_avg_error_name = f'{variable.upper()}_error'

    work_date = first_date
    n_processed = 0
    dataset_w = None
    while work_date <= last_date:
        index_date = (work_date - first_date).days
        name_in = options['name_file_format'].replace('DATE', work_date.strftime(options['name_file_format_date']))
        file_in = os.path.join(options['input_dir'], work_date.strftime('%Y'), work_date.strftime('%j'), name_in)
        if os.path.isfile(file_in):
            dataset = Dataset(file_in)
            array = None
            if variable in dataset.variables:
                array =  dataset.variables[variable][:]

            #array = dataset.variables[variable][:] if variable in dataset.variables else None
            #array[array < dataset.variables[variable].valid_min] = np.ma.masked
            #array[array > dataset.variables[variable].valid_max] = np.ma.masked
            dataset.close()
            if array is not None:
                list_files[index_date] = file_in
                n_processed = n_processed + 1
                if dataset_w is None:
                    dataset_w = start_output_dataset(file_in, file_out,
                                                     [var_avg_name, var_avg_count_name, var_avg_error_name])
                    sarray = np.ma.zeros(array.shape)
                    xarray = np.ma.zeros(array.shape)
                    x2array = np.ma.zeros(array.shape)
                    count_array = np.ma.zeros(array.shape)

                if np.ma.count(array) > 0:
                    indices = np.where(array.mask == False)
                    xarray[indices] = xarray[indices] + array[indices]
                    sarray[indices] = sarray[indices] + array[indices]
                    x2array[indices] = x2array[indices] + np.power(array[indices], 2)
                    count_array[indices] = count_array[indices] + 1

        else:
            print(f'[WARNING] Daily file {file_in} was not found')

        work_date = work_date + timedelta(hours=24)

    if n_processed == 0:
        print(
            f'[ERROR] No input files found for computing average in folder {options["input_dir"]}. Ouptput file was not created')
        # dataset_w.close()
        # os.remove(file_out)
        return

    indices_good = np.where(count_array > 0)
    indices_mask = np.where(count_array == 0)
    sarray[indices_good] = sarray[indices_good] / count_array[indices_good]
    xarray[indices_good] = np.power(xarray[indices_good], 2) / count_array[indices_good]

    indices_var = np.where(count_array > 1)
    coef_array = np.ma.zeros(count_array.shape)
    error_array = np.ma.zeros(coef_array.shape)
    coef_array[indices_var] = 1 / (count_array[indices_var]-1)
    print(np.min(coef_array),'..',np.min(xarray),'??',np.min(x2array))
    error_array[indices_var] = coef_array[indices_var] * (x2array[indices_var] - xarray[indices_var])
    error_array[error_array<0]=0##only happen with very small negative values
    error_array[indices_var] = np.sqrt(error_array[indices_var])

    dataset_kk = Dataset('/mnt/c/DATA_LUIS/OCTACWORK/2024/O2024245274-chl_monthly-bs-fr_ORIG.nc')
    sarray[indices_mask] = np.ma.masked
    count_array[indices_mask] = np.ma.masked
    media_here = dataset_kk.variables['CHL'][:]
    count_here = dataset_kk.variables['CHL_count'][:]
    count_here[count_here==0]=np.ma.masked
    dataset_kk.close()
    #print(np.ma.count(media_here),'<->',np.ma.count(sarray),'-->',np.ma.count(sarray)-np.ma.count(media_here))
    print(np.ma.count(count_here), '<->', np.ma.count(count_array), '-->', np.ma.count(count_array) - np.ma.count(count_here))
    count_diff = count_array-count_here
    print('Conti fati su: ',np.ma.count(count_diff))
    print('Di piu in here: ',len(count_diff[count_diff < 0]))
    print('Di piu in new: ', len(count_diff[count_diff > 0]))
    print('--> ', np.count_nonzero(count_diff == 1))
    print('--> ', np.count_nonzero(count_diff == 2))
    indices_piu_uno = np.where(count_diff==1)
    print('Uguale: ', len(count_diff[count_diff == 0]))


    print('check indices')
    work_date = first_date
    while work_date <= last_date:
        name_in = options['name_file_format'].replace('DATE', work_date.strftime(options['name_file_format_date']))
        file_in = os.path.join(options['input_dir'], work_date.strftime('%Y'), work_date.strftime('%j'), name_in)
        dataset = Dataset(file_in)
        data = dataset.variables[variable][:]
        sensor = dataset.variables['SENSORMASK'][:]
        data_r = data[indices_piu_uno]
        sensor_r = sensor[indices_piu_uno]
        print(work_date,'->',np.ma.count(data_r),np.ma.count(sensor_r))
        dataset.close()
        work_date = work_date +timedelta(hours=24)







    if indices_mask is not None and len(indices_mask[0]) > 0:
        sarray[indices_mask] = np.ma.masked
        count_array[indices_mask] = np.ma.masked
        error_array[indices_mask] = np.ma.masked

    dataset_w.variables[var_avg_name][:] = sarray[:]
    dataset_w.variables[var_avg_count_name][:] = count_array[:]
    dataset_w.variables[var_avg_error_name][:] = error_array[:]

    dataset_w.close()
    if n_processed < n_days:
        print(
            f'[WARNING] Output monthly file {file_out} for {year}/{month} was created using only {n_processed}/{n_days} daily files.')
    else:
        print(f'[INFO] Output monthly file {file_out} for {year}/{month} was created using {n_processed} daily files.')


def start_output_dataset(file_in, file_out, var_names):
    ncin = Dataset(file_in)
    ncout = Dataset(file_out, 'w', format='NETCDF4')

    # copy global attributes all at once via dictionary
    ncout.setncatts(ncin.__dict__)

    # copy dimensions
    for name, dimension in ncin.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    # copy lat, lon and time variables
    variables_base = ['lat', 'lon', 'time']
    for name in variables_base:
        variable = ncin.variables[name]
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue
        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             complevel=6)
        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(ncin[name].__dict__)
        # copy variable data
        ncout[name][:] = ncin[name][:]

    ncin.close()

    for var_name in var_names:
        ncout.createVariable(var_name, 'f4', ('time', 'lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)

    return ncout


def get_dates_from_arg():
    from datetime import datetime as dt
    from datetime import timedelta
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
    main()
