import argparse
import configparser
import os, stat
import subprocess
import sys
from datetime import datetime
from datetime import timedelta

# code_home = os.path.abspath('')
# print(code_home)
from c2rcc import C2RCC
from polymer_lois import POLYMER
from fub_csiro_lois import FUB_CSIRO
from acolite_lois import ACOLITE
from idepix_lois import IDEPIX
from baltic_mlp import BALTIC_MLP
from baltic_all import BALTIC_ALL
import zipfile as zp
from check_geo import CHECK_GEO

from multiprocessing import Pool

parser = argparse.ArgumentParser(description="Atmospheric correction launcher")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-p", "--product", help="Input product (testing)")
parser.add_argument('-i', "--inputpath", help="Input directory")
parser.add_argument('-o', "--outputpath", help="Output directory", required=True)
parser.add_argument('-tp', "--temp_path", help="Temporary directory")
parser.add_argument('-sd', "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument('-ed', "--end_date", help="End date (yyyy-mm-dd")
parser.add_argument('-c', "--config_file", help="Configuration file (Default: aceasy_config.ini)")
parser.add_argument('-ac', "--atm_correction", help="Atmospheric correction",
                    choices=["C2RCC", "POLYMER", "FUB_CSIRO", "ACOLITE", "IDEPIX", "BALMLP", "BALALL"], required=True)
args = parser.parse_args()


# def save_areas(input_path, output_path):
#     for name in os.listdir(input_path):
#
#         if name.startswith('S3A_OL_1_EFR') or name.startswith('S3B_OL_1_EFR'):
#             prod_path = os.path.join(input_path, name)
#             cgeo = CHECK_GEO()
#             if prod_path.endswith('SEN3'):
#                 cgeo.start_polygon_from_prod_manifest_file(prod_path)
#             if prod_path.endswith('zip'):
#                 cgeo.start_polygon_image_from_zip_manifest_file(prod_path)
#             output_file = os.path.join(output_path, name[:-3] + '.kml')
#             cgeo.save_polygon_image_askml(output_file)

# Params-> 0: corrector; 1: input_path; 2: output_path; 3: zipped input path; 4: alternative path, 5: zipped alternative input path
def run_parallel_corrector(params):
    corrector = params[0]
    path_product_input = params[1]
    output_file = params[2]
    zipped_input_path = params[3]
    alt_path_product_input = params[4]
    zipped_alt_input_path = params[5]

    if zipped_input_path:  # zipped
        path_product_input = do_zip(path_product_input)

    valid_input, iszipped_input = check_path_validity(path_product_input, None)
    if valid_input and not iszipped_input:
        b = corrector.run_process(path_product_input, output_file)

        ##ERROR,WORKING WITH ALTERNATIVE PATH
        if not b and alt_path_product_input is not None:
            if args.verbose:
                print(f'[INFO] Error with main path. Working with alternative path: {alt_path_product_input}')
            if zipped_alt_input_path:
                alt_path_product_input = do_zip(alt_path_product_input)
            valid_input_alt, iszipped_input_alt = check_path_validity(alt_path_product_input, None)
            if valid_input_alt and not iszipped_input_alt:
                corrector.run_process(alt_path_product_input, output_file)
            if zipped_alt_input_path:
                delete_unzipped_path(alt_path_product_input)

    else:
        print(f'[ERROR] Path {path_product_input} is not valid. Skiping...')

    if zipped_input_path:
        delete_unzipped_path(path_product_input)


def delete_folder_content(path_folder):
    res = True
    for f in os.listdir(path_folder):
        try:
            os.remove(os.path.join(path_folder, f))
        except OSError:
            res = False
    return res


def delete_unzipped_path(path_prod_u):
    if args.verbose:
        print(f'[INFO] Deleting unzipped path prod {path_prod_u}')
    cmd = f'rm -rf {path_prod_u}'
    prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
    out, err = prog.communicate()

    cmd = f'rmdir {path_prod_u}'
    prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
    out, err = prog.communicate()


def get_geo_limits(option):
    geo_limits = None
    if option.strip().lower() == 'bal':
        geo_limits = [53.25, 65.85, 9.25, 30.25]
    else:
        try:
            lstr = options.strip().split(',')
            if len(lstr) == 4:
                geo_limits = [float(lstr[0]), float(lstr[1]), float(lstr[2]), float(lstr[3])]
        except:
            geo_limits = None

    return geo_limits


def check_geo_limits(prod_path, geo_limits, iszipped):
    if geo_limits is None:
        return 1
    cgeo = CHECK_GEO()
    if iszipped:
        if not cgeo.check_zip_file(prod_path):
            return -1
        cgeo.start_polygon_image_from_zip_manifest_file(prod_path)
        check_geo = cgeo.check_geo_area(geo_limits[0], geo_limits[1], geo_limits[2], geo_limits[3])
        return check_geo

    if not iszipped:
        cgeo.start_polygon_from_prod_manifest_file(prod_path)
        check_geo = cgeo.check_geo_area(geo_limits[0], geo_limits[1], geo_limits[2], geo_limits[3])
        return check_geo


def print_check_geo_errors(check_geo):
    if args.verbose:
        if check_geo == 0:
            print(f'[WARNING] Image out of the interest area. Skiping')
        elif check_geo == -1:
            print(f'[WARNING] Image covegara could not be checked: invalid product. Skiping')


def check_exist_output_file(prod_path, output_dir, suffix):
    prod_name = prod_path.split('/')[-1]
    valid = False
    if os.path.isdir(prod_path) and prod_name.endswith('.SEN3') and prod_name.find('EFR') > 0:
        valid = True
    if not os.path.isdir(prod_path) and prod_name.endswith('.zip') and prod_name.find('EFR') > 0:
        valid = True
    if not valid:
        return -1, None

    output_path = None
    if prod_name.endswith('.zip'):
        prod_name = prod_name[:-4]
        if not prod_name.endswith('.SEN3'):
            prod_name = prod_name + '.SEN3'

    if prod_name.endswith('.SEN3'):
        if os.path.isdir(output_dir):
            output_name = prod_name[0:-5] + '_' + suffix + '.nc'
            output_path = os.path.join(output_dir, output_name)

    if output_path is None:
        return -1, None

    if os.path.exists(output_path):
        return 1, output_path
    else:
        return 0, output_path


def check_path_validity(prod_path, prod_name):
    valid = False
    iszipped = False
    if prod_path is None:
        return valid, iszipped
    if prod_name is None:
        prod_name = prod_path.split('/')[-1]

    if args.atm_correction == 'BALMLP' and prod_name.endswith('_POLYMER.nc'):
        valid = True
        return valid, iszipped

    if args.atm_correction == 'BALALL':
        valid = True
        return valid, iszipped

    if os.path.isdir(prod_path) and prod_name.endswith('.SEN3') and prod_name.find('EFR') > 0:
        valid = True
        return valid, iszipped
    if not os.path.isdir(prod_path) and prod_name.endswith('.zip') and prod_name.find('EFR') > 0:
        iszipped = True
        if not args.temp_path:
            print(
                f'[ERROR] Temporary path must be defined to work with zip files. Use the option -tp')
            valid = False
            return valid, iszipped
        if not os.path.exists(args.temp_path):
            print(f'[ERROR] Temporary path {args.temp_path} does not exist')
            valid = False
            return valid, iszipped
        valid = True
        return valid, iszipped

    return valid, iszipped


def search_alternative_prod_path(f, data_alternative_path, year_str, day_str):
    # print(data_alternative_path)
    # print(f)
    # print(year_str)
    # print(day_str)
    if data_alternative_path is None:
        return None
    output_path = os.path.join(data_alternative_path, year_str, day_str)
    if not os.path.exists(output_path):
        output_path = data_alternative_path
    #print(f'Output path {output_path}' )
    if not os.path.exists(output_path) or not os.path.isdir(output_path):
        return None
    sdate, edate = get_start_end_times_from_file_name(f)
    #print(f'SDate: {sdate} Edate {edate}')
    if sdate is None or edate is None:
        return None
    sensor = f[0:3]
    for fout in os.listdir(output_path):
        output_path_jday = os.path.join(output_path, fout)

        if fout.startswith(sensor) and fout.find('EFR') > 0:
            sdate_o, edate_o = get_start_end_times_from_file_name(fout)
            #print(f'Alternative path: {output_path_jday} Sdate {sdate_o} Edate {edate_o}')
            if sdate_o is not None and edate_o is not None:
                #print(f'Here: {sdate}>={sdate_o} --- {edate}<={edate_o}')
                if sdate >= sdate_o and edate <= edate_o:
                    return output_path_jday
                if sdate_o <= sdate <= edate_o < edate:
                    sec_total = (edate - sdate).total_seconds()
                    sec_out = (edate - edate_o).total_seconds()
                    porc = ((sec_total - sec_out) / sec_total) * 100
                    if porc > 50:
                        return output_path_jday
                if sdate < sdate_o <= edate <= edate_o:
                    sec_total = (edate - sdate).total_seconds()
                    sec_out = (sdate_o - sdate).total_seconds()
                    porc = ((sec_total - sec_out) / sec_total) * 100
                    if porc > 50:
                        return output_path_jday

    return None


def get_start_end_times_from_file_name(fname):
    lfname = fname.split('_')
    sdate = None
    edate = None
    for l in lfname:
        if sdate is None and edate is None:
            try:
                sdate = datetime.strptime(l.strip(), '%Y%m%dT%H%M%S')
            except:
                pass
        elif sdate is not None and edate is None:
            try:
                edate = datetime.strptime(l.strip(), '%Y%m%dT%H%M%S')
            except:
                pass
    return sdate, edate


def do_zip(prod_path):
    if args.verbose:
        print(f'[INFO] Working with zip path: {prod_path}')
    unzip_path = args.temp_path
    with zp.ZipFile(prod_path, 'r') as zprod:
        if args.verbose:
            print(f'[INFO] Unziping {prod_name} to {unzip_path}')
        zprod.extractall(path=unzip_path)
        path_prod_u = prod_path.split('/')[-1][0:-4]
        if not path_prod_u.endswith('.SEN3'):
            path_prod_u = path_prod_u + '.SEN3'
        path_prod_u = os.path.join(unzip_path, path_prod_u)

    if os.path.exists(path_prod_u):
        if args.verbose:
            print(f'[INFO] Running atmospheric correction for {path_prod_u}')
        return path_prod_u
    return None


# input_file (param[0]) could be repeated
def optimize_param_list(param_list):
    param_list_new = [param_list[0]]

    for idx in range(1, len(param_list)):
        repeated = False
        for icheck in range(idx):
            if param_list[idx][1] == param_list[icheck][1]:
                repeated = True
                break
        if not repeated:
            param_list_new.append(param_list[idx])
    return param_list_new


def get_alternative_path(f, data_alternative_path):
    sat_time, sat_time_o = get_start_end_times_from_file_name(f)
    year_str = sat_time.strftime('%Y')
    day_str = sat_time.strftime('%j')
    prod_path_altn = search_alternative_prod_path(f, data_alternative_path, year_str, day_str)
    prod_path_alt = None
    iszipped_alt = False
    if prod_path_altn is not None:
        prod_name_altn = prod_path_altn.split('/')[-1]
        valid_alt, iszipped_alt = check_path_validity(prod_path_altn, prod_name_altn)
        if valid_alt:
            check_geo = check_geo_limits(prod_path_altn, geo_limits, iszipped_alt)
            if check_geo == 1:
                prod_path_alt = prod_path_altn

    return prod_path_alt, iszipped_alt


def get_unzipped_path(prod_path, output_path):
    with zp.ZipFile(prod_path, 'r') as zprod:
        if args.verbose:
            print(f'[INFO] Unziping {f} to {output_path}')
        zprod.extractall(path=output_path)
    path_prod_u = prod_path.split('/')[-1][0:-4]
    if not path_prod_u.endswith('.SEN3'):
        path_prod_u = path_prod_u + '.SEN3'
    path_prod_u = os.path.join(output_path, path_prod_u)
    return path_prod_u


def check():
    print('CHECKING...')
    # file_in = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/MONTHLY_BASE/O2022335365-chl_monthly-bal-fr.nc'
    file_in = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/MONTHLY_BASE/O2022335365-kd490_monthly-bal-fr.nc'
    from netCDF4 import Dataset
    import numpy as np
    from datetime import datetime as dt
    from datetime import timedelta
    dataset = Dataset(file_in, 'a')
    time_new = dt(2022, 12, 1, 0, 0, 0)
    time_new_seconds = int((time_new - dt(1981, 1, 1, 0, 0, 0)).total_seconds())

    var_time = dataset.variables['time']
    time_array = np.array(var_time[:])
    time_array[0] = time_new_seconds
    var_time[:] = [time_array[:]]

    # var_chl = dataset.variables['CHL']
    # chl_array = np.array(var_chl)
    # chl_array[:] = -999.0
    # var_chl[:] = [chl_array[:]]
    #
    # var_chl_count = dataset.variables['CHL_count']
    # chl_count_array = np.array(var_chl_count)
    # chl_count_array[:] = 0.0
    # var_chl_count[:] = [chl_count_array[:]]
    #
    # var_chl_error = dataset.variables['CHL_error']
    # chl_error_array = np.array(var_chl_error)
    # chl_error_array[:] = -999.0
    # var_chl_error[:] = [chl_error_array[:]]

    var_chl = dataset.variables['KD490']
    chl_array = np.array(var_chl)
    chl_array[:] = -999.0
    var_chl[:] = [chl_array[:]]

    var_chl_count = dataset.variables['KD490_count']
    chl_count_array = np.array(var_chl_count)
    chl_count_array[:] = 0.0
    var_chl_count[:] = [chl_count_array[:]]

    var_chl_error = dataset.variables['KD490_error']
    chl_error_array = np.array(var_chl_error)
    chl_error_array[:] = -999.0
    var_chl_error[:] = [chl_error_array[:]]

    dataset.start_date = time_new.strftime('%Y-%m-%d')
    dataset.stop_date = dt(2022, 12, 31).strftime('%Y-%m-%d')

    dataset.close()
    return True


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('[INFO] Started')
    # b = check()
    # if b:
    #     sys.exit()
    fconfig = None
    if args.config_file:
        fconfig = args.config_file
    else:
        if os.path.exists('aceasy_config.ini'):
            fconfig = 'aceasy_config.ini'
        elif os.path.exists(os.path.join('aceasy', 'aceasy_config.ini')):
            fconfig = os.path.join('aceasy', 'aceasy_config.ini')
    if fconfig is None:
        print(f'[ERROR] Config file is required')
        exit(1)
    if not os.path.exists(fconfig):
        print(f'[ERROR] Config file: {fconfig} does not exist')
        exit(1)
    if not args.product and not args.inputpath:
        print(f'[ERROR] Product name or input folder are required')
        exit(1)
    input_path = None
    if args.inputpath:
        input_path = args.inputpath
    if args.product:
        prod_path = args.product
    if not args.outputpath:
        print(f'[ERROR] Output folder option is required')
        exit(1)
    output_path = args.outputpath

    if args.atm_correction == 'C2RCC':
        corrector = C2RCC(fconfig, args.verbose)
        suffix = 'C2RCC'
    elif args.atm_correction == 'POLYMER':
        corrector = POLYMER(fconfig, args.verbose)
        suffix = 'POLYMER'
    elif args.atm_correction == 'FUB_CSIRO':
        corrector = FUB_CSIRO(fconfig, args.verbose)
        suffix = 'FUB'
    elif args.atm_correction == 'ACOLITE':
        corrector = ACOLITE(fconfig, args.verbose)
        suffix = 'ACOLITE'
    elif args.atm_correction == 'IDEPIX':
        corrector = IDEPIX(fconfig, args.verbose)
        suffix = 'IDEPIX'
    elif args.atm_correction == 'BALMLP':
        corrector = BALTIC_MLP(fconfig, args.verbose)
    elif args.atm_correction == 'BALALL':
        corrector = BALTIC_ALL(fconfig, args.verbose)

    applyPool = 0
    geo_limits = None
    data_alternative_path = None
    options = configparser.ConfigParser()
    options.read(fconfig)
    if options.has_section('GLOBAL'):
        if options.has_option('GLOBAL', 'pool'):
            applyPool = int(options['GLOBAL']['pool'])
        if options.has_option('GLOBAL', 'geolimits'):
            geo_limits = get_geo_limits(options['GLOBAL']['geolimits'])
        if options.has_option('GLOBAL', 'data_alternative_path'):
            data_alternative_path = options['GLOBAL']['data_alternative_path'].strip()
            if not os.path.exists(data_alternative_path):
                data_alternative_path = None
    start_date = None
    end_date = None
    if args.start_date and args.end_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] Start date: {args.start_date} should be in format: yyyy-mm-dd')
            exit(1)
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] End date: {args.end_date} should be in format: yyyy-mm-dd')
            exit(1)
        if start_date > end_date:
            print(f'[ERROR] End date should be equal or greater than start date')
            exit(1)

        if args.verbose:
            print(f'[INFO] Start date: {args.start_date} End date: {args.end_date}')

    if not corrector.check_runac():
        exit(1)
    if args.verbose:
        print(f'[INFO] Started {args.atm_correction} processor')

    if input_path is None:  # single product, for testing
        print('input path is none, pero prod_path es: ',prod_path)
        f = os.path.basename(prod_path)
        print(f)
        print(os.path.isdir(prod_path))
        print(f.endswith('.SEN3'))
        print(f.find('EFR'))

        if args.atm_correction == 'BALMLP' and f.endswith('.nc'):
            p = corrector.run_process(prod_path, output_path)
        elif args.atm_correction == 'BALALL' and os.path.isdir(prod_path):
            p = corrector.run_process(prod_path, output_path)
        elif os.path.isdir(prod_path) and f.endswith('.SEN3') and f.find('EFR') > 0:
            print('me deberia llegar aqui')
            check_geo = check_geo_limits(prod_path, geo_limits, False)
            if check_geo == 1:
                p = corrector.run_process(prod_path, output_path)
                if not p and data_alternative_path is not None:
                    output_name = f[0:-5] + '_POLYMER.nc'
                    file_output_orig = os.path.join(output_path, output_name)
                    prod_path_alt, iszipped_alt = get_alternative_path(f, data_alternative_path)
                    print(f'[WARNING] Error in Polymer. Working with alternative path: {prod_path_alt}')
                    if prod_path_alt is not None:
                        if iszipped_alt:
                            prod_path_u = get_unzipped_path(prod_path_alt, output_path)
                        else:
                            prod_path_u = prod_path_alt
                        p = corrector.run_process(prod_path_u, file_output_orig)
                        delete_unzipped_path(prod_path_u)
            else:
                print_check_geo_errors(check_geo)
        elif not os.path.isdir(prod_path) and f.endswith('.zip') and f.find('EFR') > 0:
            if args.verbose:
                print(f'[INFO] Working with zip path: {prod_path}')
            check_geo = check_geo_limits(prod_path, geo_limits, True)
            if check_geo == 1:
                with zp.ZipFile(prod_path, 'r') as zprod:
                    if args.verbose:
                        print(f'[INFO] Unziping {f} to {output_path}')
                    zprod.extractall(path=output_path)
                path_prod_u = prod_path.split('/')[-1][0:-4]
                if not path_prod_u.endswith('.SEN3'):
                    path_prod_u = path_prod_u + '.SEN3'
                path_prod_u = os.path.join(output_path, path_prod_u)
                if args.verbose:
                    print(f'[INFO] Running atmospheric correction for {path_prod_u}')
                p = corrector.run_process(path_prod_u, output_path)
                delete_unzipped_path(path_prod_u)
                if not p and data_alternative_path is not None:

                    input_name_orig = os.path.basename(path_prod_u)
                    output_name = input_name_orig[0:-5] + '_POLYMER.nc'
                    file_output_orig = os.path.join(output_path, output_name)

                    prod_path_alt, iszipped_alt = get_alternative_path(f, data_alternative_path)
                    print(f'[WARNING] Error in Polymer. Working with alternative path: {prod_path_alt}')
                    if prod_path_alt is not None:
                        if iszipped_alt:
                            prod_path_u = get_unzipped_path(prod_path_alt, output_path)
                        else:
                            prod_path_u = prod_path_alt
                        p = corrector.run_process(prod_path_u, file_output_orig)
                        delete_unzipped_path(prod_path_u)
            else:
                print_check_geo_errors(check_geo)
        if args.verbose:
            print('--------------------------------------------------')
    else:  ##WORKING WITH FOLDERS
        if start_date is not None and end_date is not None:  # formato year/jjj
            date_here = start_date
            while date_here <= end_date:
                year_str = date_here.strftime('%Y')
                day_str = date_here.strftime('%j')

                input_path_date = os.path.join(input_path, year_str, day_str)

                if os.path.exists(input_path_date):
                    output_path_year = os.path.join(output_path, year_str)
                    if not os.path.exists(output_path_year):
                        st = os.stat(output_path)
                        os.chmod(output_path, st.st_mode | stat.S_IWOTH | stat.S_IWGRP)
                        os.mkdir(output_path_year)

                    output_path_jday = os.path.join(output_path_year, day_str)
                    if not os.path.exists(output_path_jday):
                        st = os.stat(output_path_year)
                        os.chmod(output_path_year, st.st_mode | stat.S_IWOTH | stat.S_IWGRP)
                        os.mkdir(output_path_jday)
                        st = os.stat(output_path_jday)
                        os.chmod(output_path_jday, st.st_mode | stat.S_IWOTH | stat.S_IWGRP)
                    if args.verbose:
                        print('*************************************************')
                        print(f'DATE: {date_here}')

                    if args.atm_correction == 'BALALL':
                        corrector.run_process(input_path_date, output_path_jday)
                        date_here = date_here + timedelta(hours=24)
                        if args.verbose:
                            print('--------------------------------------------------')
                        continue

                    ##first we obtain list of param (corrector,input_path,output_path,iszipped)
                    param_list = []
                    for f in os.listdir(input_path_date):
                        prod_name = f
                        prod_path = os.path.join(input_path_date, prod_name)
                        print('---------------')

                        if args.atm_correction == 'BALMLP':
                            if prod_name.endswith('_POLYMER.nc'):
                                params = [corrector, prod_path, output_path_jday, False, None, False]
                                param_list.append(params)
                                continue
                            else:
                                continue

                        coutput, output_file_path = check_exist_output_file(prod_path, output_path_jday, suffix)
                        if coutput == -1:
                            ##format no valid
                            continue
                        elif coutput == 1:
                            print(f'[INFO] Output file for path: {prod_path} already exists. Skiping...')
                            continue

                        # path validity and geo_limits for path_prod
                        valid, iszipped = check_path_validity(prod_path, prod_name)
                        if not valid:
                            continue
                        check_geo = check_geo_limits(prod_path, geo_limits, iszipped)

                        if check_geo == 1:
                            # alternative prod path, it's useful for Polymer if the trim fails
                            prod_path_altn = search_alternative_prod_path(f, data_alternative_path, year_str, day_str)
                            prod_path_alt = None
                            iszipped_alt = False
                            if prod_path_altn is not None:
                                prod_name_altn = prod_path_altn.split('/')[-1]
                                valid_alt, iszipped_alt = check_path_validity(prod_path_altn, prod_name_altn)
                                if valid_alt:
                                    check_geo = check_geo_limits(prod_path_altn, geo_limits, iszipped_alt)
                                    if check_geo == 1:
                                        prod_path_alt = prod_path_altn
                            ##end definining alternative path
                            params = [corrector, prod_path, output_file_path, iszipped, prod_path_alt, iszipped_alt]
                            param_list.append(params)
                        else:
                            print_check_geo_errors(check_geo)

                    ##run the list of product as parallel processes
                    if len(param_list) == 0:
                        print(f'[WARNING] No valid products were found for date: {date_here}')
                        date_here = date_here + timedelta(hours=24)
                        continue
                    param_list = optimize_param_list(param_list)
                    if applyPool == 0:
                        if args.verbose:
                            print(f'[INFO] Starting sequencial processing. Number of products: {len(param_list)}')
                        for params in param_list:
                            if args.atm_correction == 'BALMLP':
                                corrector = BALTIC_MLP(fconfig, args.verbose)
                                params[0] = corrector
                            run_parallel_corrector(params)

                    else:
                        if args.verbose:
                            print(f'[INFO] Starting parallel processing. Number of products: {len(param_list)}')
                            print(f'[INFO] CPUs: {os.cpu_count()}')
                            print(f'[INFO] Parallel processes: {applyPool}')
                        if applyPool < 0:
                            poolhere = Pool()
                        else:
                            poolhere = Pool(applyPool)
                        poolhere.map(run_parallel_corrector, param_list)

                else:
                    if args.verbose:
                        print(f'[WARNING] Path: {input_path_date} does not exist. Skipping..')

                date_here = date_here + timedelta(hours=24)
                if args.verbose:
                    print('--------------------------------------------------')
        else:
            for f in os.listdir(input_path):
                prod_path = os.path.join(input_path, f)
                if os.path.isdir(prod_path) and f.endswith('.SEN3') and f.find('EFR') > 0:
                    if args.verbose:
                        print('--------------------------------------------------')
                    p = corrector.run_process(prod_path, output_path)
                    if not p:
                        input_name_orig = os.path.basename(prod_path)
                        output_name = input_name_orig[0:-5] + '_POLYMER.nc'
                        file_output_orig = os.path.join(output_path, output_name)
                        prod_path_alt, iszipped_alt = get_alternative_path(f,data_alternative_path)

                        if prod_path_alt is not None:
                            if args.verbose:
                                print(f'[INFO] Running with alternative path: {prod_path_alt}')
                            if iszipped_alt:
                                prod_path_u = get_unzipped_path(prod_path_alt, output_path)
                            else:
                                prod_path_u = prod_path_alt
                            p = corrector.run_process(prod_path_u, file_output_orig)
                            delete_unzipped_path(prod_path_u)



    # if args.atm_correction == 'C2RCC':
    #     corrector = C2RCC(fconfig, args.verbose)
    #     if not corrector.check_runac():
    #         exit(1)
    #     if args.verbose:
    #         print('[INFO] Started C2RCC processor')
    #
    #     if input_path is None: #single product, for testing
    #         p = corrector.run_process(prod_path,output_path)
    #         if args.verbose:
    #             print('--------------------------------------------------')
    #     else:
    #         for f in os.listdir(input_path):
    #             prod_path = os.path.join(input_path,f)
    #             if os.path.isdir(prod_path) and f.endswith('.SEN3'):
    #                 if args.verbose:
    #                     print('--------------------------------------------------')
    #                 p = corrector.run_process(prod_path,output_path)
