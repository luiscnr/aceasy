import warnings, argparse, os
from datetime import datetime as dt
from datetime import timedelta
from netCDF4 import Dataset

import numpy as np

warnings.filterwarnings("ignore", category=UserWarning)

parser = argparse.ArgumentParser(
    description="Obtaining information for running MDB_builder.")

parser.add_argument('-m', "--mode", help='Mode option', choices=["check_neg_olci_values", "correct_neg_olci_values"],
                    required=True)
parser.add_argument('-i', "--input_path", help="Input path.")
parser.add_argument('-o', "--output", help="Output file.")
parser.add_argument("-sd", "--start_date", help="Start date.")
parser.add_argument("-ed", "--end_date", help="End date.")
parser.add_argument("-r", "--region", help="Region.", choices=["BS","MED","BAL","ARC"])
# parser.add_argument('-s', "--source_path", help="Source path.",default="/dst04-data1/OC/OLCI/daily_v202311_bc")
# parser.add_argument('-p', "--param", help="Param for TEST")
args = parser.parse_args()


def check_neg_olci_values(input_path, output_file, start_date, end_date, region):
    work_date = start_date
    fw = open(output_file, 'w')
    fw.write('Date;WL;OFile')
    bands = ['400', '412_5', '442_5', '490', '510', '560', '620', '665', '673_75', '681_25', '708_75', '753_75',
             '778_75', '865', '885', '1020']
    bands_n = [float(x.replace('_','.')) for x in bands]
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        yyyymmdd =work_date.strftime('%Y-%m-%d')
        print(f'[INFO] Work date: {yyyymmdd}')
        for iband,band in enumerate(bands):
            # file_a = os.path.join(input_path, yyyy, jjj, f'Oa{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            # file_b = os.path.join(input_path, yyyy, jjj, f'Ob{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            file_o = os.path.join(input_path, yyyy, jjj, f'O{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            print(f'[INFO]--> File: {file_o}')
            # min_a = 'N/A'
            # min_b = 'N/A'
            min_o = 'N/A'
            # if os.path.exists(file_a):
            #     min_a = get_min_rrs_value(file_a,band)
            # if os.path.exists(file_b):
            #     min_b = get_min_rrs_value(file_b,band)
            if os.path.exists(file_o):
                min_o = get_min_rrs_value(file_o,band)
            #line = f'{yyyymmdd};{bands_n[iband]};{min_a};{min_b};{min_o}'
            line = f'{yyyymmdd};{bands_n[iband]};{min_o}'
            fw.write('\n')
            fw.write(line)

        work_date = work_date + timedelta(hours=24)
    fw.close()

def get_min_rrs_value(file,band):
    dataset = Dataset(file, 'r')
    array = dataset.variables[f'RRS{band}'][:]
    min_v = np.ma.min(array)
    dataset.close()
    return f'{min_v}'

def main():
    print('[INFO] Started utils')

    # file = '/mnt/c/DATA_LUIS/OCTACWORK/2022/139/O2022139-rrs400-bs-fr.nc'
    # dataset  = Dataset(file)
    # array = dataset.variables['RRS400'][:]
    # print(np.ma.min(array))
    # print(np.min(array))
    # dataset.close()

    if args.mode == 'check_neg_olci_values':
        arguments = ['input_path', 'output', 'start_date', 'end_date','region']
        if not check_required_params(arguments):
            return
        if not check_directory(args.input_path, False):
            return
        if not check_output_file(args.output, 'csv'):
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        check_neg_olci_values(args.input_path, args.output, start_date, end_date,args.region)


def check_required_params(param_list):
    b = True
    for param in param_list:
        if not args.__dict__[param]:
            print(f'[ERROR] {param} is required for mode {args.mode}')
            b = False
    return b


def check_output_file(file, ext):
    if ext is not None and not file.endswith(ext):
        print(f'[ERROR] {file} should be a {ext} file')
        return False
    if not check_directory(os.path.dirname(file), False):
        return False
    return True


def check_directory(directory, createIfNotExist):
    if not os.path.isdir(directory):
        if createIfNotExist:
            try:
                os.mkdir(directory)
            except:
                print(f'[ERROR] {directory} does not exist and could not be created. Please review permissions')
                return False
        else:
            print(f'[ERROR] {directory} does not exist or is not a valid directory')
            return False
    return True


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
