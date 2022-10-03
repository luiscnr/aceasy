import argparse
import configparser
import os
import subprocess
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
                    choices=["C2RCC", "POLYMER", "FUB_CSIRO", "ACOLITE", "IDEPIX", "BALMLP"], required=True)
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

# Params-> 0: corrector; 1: input_path; 2: output_path; 3: delete input path; 4: alternative path
def run_parallel_corrector(params):
    corrector = params[0]
    b = corrector.run_process(params[1], params[2])
    if not b and params[4] is not None:
        corrector.run_process(params[4], params[2])
    if params[3]:
        delete_unzipped_path(params[1])


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
        return -1

    if prod_name.endswith('.zip'):
        prod_name = prod_name[:-4]
        if not prod_name.endswith('.SEN3'):
            prod_name = prod_name + '.SEN3'

    if prod_name.endswith('.SEN3'):
        if os.path.isdir(output_dir):
            output_name = prod_name[0:-5] + '_' + suffix + '.nc'
            output_path = os.path.join(output_dir, output_name)
            if os.path.exists(output_path):
                return 1

    return 0


def search_alternative_prod_path(f, data_alternative_path, year_str, day_str):
    if data_alternative_path is None:
        return None
    output_path = os.path.join(data_alternative_path, year_str, day_str)
    # print(f'Output path {output_path}' )
    if not os.path.exists(output_path) or not os.path.isdir(output_path):
        return None
    sdate, edate = get_start_end_times_from_file_name(f)
    # print(f'SDate: {sdate} Edate {edate}')
    if sdate is None or edate is None:
        return None
    sensor = f[0:3]
    for fout in os.listdir(output_path):
        output_path_jday = os.path.join(output_path, fout)

        if fout.startswith(sensor) and fout.find('EFR') > 0:
            sdate_o, edate_o = get_start_end_times_from_file_name(fout)
            print(f'Alternative path: {output_path_jday} Sdate {sdate_o} Edate {edate_o}')
            if sdate_o is not None and edate_o is not None:
                print(f'Here: {sdate}>={sdate_o} --- {edate}<={edate_o}')
                if sdate >= sdate_o and edate <= edate_o:
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('[INFO] Started')
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
        f = os.path.basename(prod_path)
        if args.atm_correction == 'BALMLP' and f.endswith('.nc'):
            p = corrector.run_process(prod_path, output_path)
        elif os.path.isdir(prod_path) and f.endswith('.SEN3') and f.find('EFR') > 0:
            check_geo = check_geo_limits(prod_path, geo_limits, False)
            if check_geo == 1:
                p = corrector.run_process(prod_path, output_path)
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
                        os.mkdir(output_path_year)
                    output_path_jday = os.path.join(output_path_year, day_str)
                    if not os.path.exists(output_path_jday):
                        os.mkdir(output_path_jday)
                    if args.verbose:
                        print('*************************************************')

                    ##first we obtain list of param (corrector,input_path,output_path,iszipped)
                    param_list = []
                    for f in os.listdir(input_path_date):
                        prod_path = os.path.join(input_path_date, f)
                        print('-----------------------------------------------------------------------')

                        coutput = check_exist_output_file(prod_path, output_path_jday, suffix)
                        print(prod_path, '-->', coutput)
                        if coutput == -1:
                            ##format no valid
                            continue
                        elif coutput == 1:
                            print(f'[INFO] Output file for path: {prod_path} already exists. Skiping...')
                            continue
                        else:  # temporary code, for working with alternative source file
                            prod_path_alt = search_alternative_prod_path(f, data_alternative_path, year_str, day_str)
                            if prod_path_alt is None:
                                if args.verbose:
                                    print('[INFO] Alternative path was not found. Skiping')
                                continue
                            else:
                                if args.verbose:
                                    print(f'[INFO] Working with alternative path: {prod_path}')
                                prod_path = prod_path_alt

                        if os.path.isdir(prod_path) and f.endswith('.SEN3') and f.find('EFR') > 0:
                            check_geo = check_geo_limits(prod_path, geo_limits, False)
                            if check_geo == 1:
                                params = [corrector, prod_path, output_path_jday, False, prod_path_alt]
                                param_list.append(params)
                            else:
                                print_check_geo_errors(check_geo)
                        if not os.path.isdir(prod_path) and f.endswith('.zip') and f.find('EFR') > 0:
                            if not args.temp_path:
                                print(
                                    f'[ERROR] Temporary path must be defined to work with zip files. Use the option -tp')
                                continue
                            if not os.path.exists(args.temp_path):
                                print(f'[ERROR] Temporary path {args.temp_path} does not exist')
                                continue
                            if args.verbose:
                                print(f'[INFO] Working with zip path: {prod_path}')
                            unzip_path = args.temp_path
                            iszipped = True
                            check_geo = check_geo_limits(prod_path, geo_limits, True)
                            if check_geo == 1:
                                with zp.ZipFile(prod_path, 'r') as zprod:
                                    if args.verbose:
                                        print(f'[INFO] Unziping {f} to {unzip_path}')
                                    zprod.extractall(path=unzip_path)
                                path_prod_u = prod_path.split('/')[-1][0:-4]
                                if not path_prod_u.endswith('.SEN3'):
                                    path_prod_u = path_prod_u + '.SEN3'
                                path_prod_u = os.path.join(unzip_path, path_prod_u)
                                if args.verbose:
                                    print(f'[INFO] Running atmospheric correction for {path_prod_u}')
                                params = [corrector, path_prod_u, output_path_jday, True, prod_path_alt]
                                param_list.append(params)
                            else:
                                print_check_geo_errors(check_geo)
                                continue

                    ##run the list of product as parallel processes
                    if len(param_list) == 0:
                        print(f'[WARNING] No valid products were found for date: {date_here}')
                        date_here = date_here + timedelta(hours=24)
                        continue
                    if applyPool == 0:
                        if args.verbose:
                            print(f'[INFO] Starting sequencial processing. Number of products: {len(param_list)}')
                        for params in param_list:
                            corrector = params[0]
                            b = corrector.run_process(params[1], params[2])
                            if not b and params[4] is not None:
                                corrector.run_process(params[4], params[2])
                            if params[3]:
                                delete_unzipped_path(params[1])
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
