import warnings, argparse, os
from datetime import datetime as dt
from datetime import timedelta
from netCDF4 import Dataset

import numpy as np

warnings.filterwarnings("ignore", category=UserWarning)

parser = argparse.ArgumentParser(
    description="Obtaining information for running MDB_builder.")

parser.add_argument('-m', "--mode", help='Mode option',
                    choices=["test", "check_neg_olci_values", "correct_neg_olci_values",
                             "correct_neg_olci_values_slurm"],
                    required=True)
parser.add_argument('-i', "--input_path", help="Input path.")
parser.add_argument('-o', "--output", help="Output file.")
parser.add_argument("-sd", "--start_date", help="Start date.")
parser.add_argument("-ed", "--end_date", help="End date.")
parser.add_argument("-r", "--region", help="Region.", choices=["BS", "MED", "BAL", "ARC"])
parser.add_argument("-ncores", "--num_cores",
                    help="Number of cores for correct_neg_olci_values_slurm mode (Deafult:12)", default=12)
# parser.add_argument('-s', "--source_path", help="Source path.",default="/dst04-data1/OC/OLCI/daily_v202311_bc")
# parser.add_argument('-p', "--param", help="Param for TEST")
args = parser.parse_args()


def check_neg_olci_values(input_path, output_file, start_date, end_date, region):
    work_date = start_date
    fw = open(output_file, 'w')
    fw.write('Date;OFile;WL;MinValue;MinAttr;NInvalid')
    bands = ['400', '412_5', '442_5', '490', '510', '560', '620', '665', '673_75', '681_25', '708_75', '753_75',
             '778_75', '865', '885', '1020']
    bands_n = [float(x.replace('_', '.')) for x in bands]
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        yyyymmdd = work_date.strftime('%Y-%m-%d')
        print(f'[INFO] Work date: {yyyymmdd}')
        for iband, band in enumerate(bands):
            # file_a = os.path.join(input_path, yyyy, jjj, f'Oa{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            # file_b = os.path.join(input_path, yyyy, jjj, f'Ob{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            file_o = os.path.join(input_path, yyyy, jjj, f'O{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            print(f'[INFO]--> File: {file_o}')
            # min_a = 'N/A'
            # min_b = 'N/A'
            min_o = 'N/A'
            attr = 'N/A'
            n_invalid = -1
            # if os.path.exists(file_a):
            #     min_a = get_min_rrs_value(file_a,band)
            # if os.path.exists(file_b):
            #     min_b = get_min_rrs_value(file_b,band)
            if os.path.exists(file_o):
                min_o, attr, n_invalid = get_min_rrs_value(file_o, band)
            # line = f'{yyyymmdd};{bands_n[iband]};{min_a};{min_b};{min_o}'
            line = f'{yyyymmdd};O{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc;{bands_n[iband]};{min_o};{attr};{n_invalid}'
            fw.write('\n')
            fw.write(line)

        work_date = work_date + timedelta(hours=24)
    fw.close()


def correct_neg_olci_values(input_path, start_date, end_date, region):
    work_date = start_date
    bands = ['400', '412_5', '442_5', '490', '510', '560', '620', '665', '673_75', '681_25', '708_75', '753_75',
             '778_75', '865', '885', '1020']
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        yyyymmdd = work_date.strftime('%Y-%m-%d')
        print(f'[INFO] Work date: {yyyymmdd}')
        for band in bands:
            file_a = os.path.join(input_path, yyyy, jjj, f'Oa{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            file_b = os.path.join(input_path, yyyy, jjj, f'Ob{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            file_o = os.path.join(input_path, yyyy, jjj, f'O{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            file_out = f'{file_o[:-3]}_temp.nc'
            neg_band = f'RRS{band}'
            print(f'[INFO]--> File: {file_o}')
            correct_neg_olci_values_impl(file_o, file_out, file_a, file_b, neg_band)
            os.rename(file_out, file_o)

        work_date = work_date + timedelta(hours=24)


def correct_neg_olci_values_impl(input_file, output_file, file_a, file_b, neg_band):
    input_dataset = Dataset(input_file)
    ncout = Dataset(output_file, 'w', format='NETCDF4')

    # new_array = input_array.copy()
    dataset_a = Dataset(file_a)
    array_a = dataset_a.variables[neg_band][:]
    dataset_a.close()
    dataset_b = Dataset(file_b)
    array_b = dataset_b.variables[neg_band][:]
    dataset_b.close()

    new_array = np.ma.mean(np.ma.concatenate([array_a, array_b], axis=0), axis=0)

    ##TESTING CHECK CODE
    # input_array = np.ma.squeeze(input_dataset.variables[neg_band][:])
    # array_a = np.ma.squeeze(array_a)
    # array_b = np.ma.squeeze(array_b)
    # indices = np.where(np.logical_and(array_a>0,array_b>0))
    # prev_valid = input_array[indices]
    # new_valid = new_array[indices]
    # ratio = prev_valid/new_valid
    # print(np.ma.min(ratio),np.ma.max(ratio))
    # print('old', np.ma.min(input_array),np.ma.max(input_array))
    # print('new', np.ma.min(new_array), np.ma.max(new_array))

    # copy global attributes all at once via dictionary
    ncout.setncatts(input_dataset.__dict__)

    # copy dimensions
    for name, dimension in input_dataset.dimensions.items():
        ncout.createDimension(
            name, (len(dimension) if not dimension.isunlimited() else None))

    for name, variable in input_dataset.variables.items():
        fill_value = None
        if '_FillValue' in list(variable.ncattrs()):
            fill_value = variable._FillValue

        ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                             complevel=6)
        # copy variable attributes all at once via dictionary
        ncout[name].setncatts(input_dataset[name].__dict__)
        if name == neg_band:
            ncout[name][:] = new_array[:]
        else:
            ncout[name][:] = input_dataset[name][:]

    ncout.close()
    input_dataset.close()


def correct_neg_olci_values_slurm(dir_log, start_date, end_date, region):
    try:
        nmax = int(args.num_cores)
    except:
        print(f'[ERROR] Number of cores option -ncores (--num_cores) must be an integer value')
        return
    input_olci_path = '/dst04-data1/OC/OLCI/daily_v202311_bc'
    work_path = '/home/gosuser/Processing/gos-oc-processingchains_v202411/s3olciProcessing'
    line_py_base = f'python {work_path}/make_merge_olci_202311.py -d DATE -a {region.lower()} -p RRS -v'
    file_list = []
    date_str_list = []

    bands = ['400', '412_5', '442_5', '490', '510', '560', '620', '665', '673_75', '681_25', '708_75', '753_75',
             '778_75', '865', '885', '1020']
    min_values = [-0.0063, -0.0058, -0.0046, -0.0029, -0.0024, -0.0017, -0.0012, -0.00083, -0.000794, -0.00071,
                  -0.00065, 0.0, 0.0, 0.0, 0.0, 0.0]
    work_date = start_date
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        fslurm = os.path.join(dir_log, f'LaunchMergeOLCI_{work_date.strftime("%Y%m%d")}.slurm')
        fw = open(fslurm, 'w')
        fw.write('#!/bin/bash')
        add_new_line(fw, '#SBATCH --nodes=1')
        add_new_line(fw, '#SBATCH --ntasks=1')
        add_new_line(fw, f'#SBATCH --output {fslurm.replace(".slurm", ".log")}')
        add_new_line(fw, '#SBATCH -p octac_rep')
        add_new_line(fw, '#SBATCH --mail-type=BEGIN,END,FAIL')
        add_new_line(fw, '#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it,lorenzo.amodio@artov.ismar.cnr.it')
        add_new_line(fw, '')
        add_new_line(fw, 'source /home/gosuser/load_miniconda3.source')
        add_new_line(fw, 'conda activate op_proc_202211v2')
        add_new_line(fw, f'cd {work_path}')
        add_new_line(fw, '')
        line_py = line_py_base.replace("DATE", work_date.strftime('%Y-%m-%d'))
        add_new_line(fw, line_py)
        add_new_line(fw, '')
        add_new_line(fw, 'wait')
        add_new_line(fw, '')
        for iband, band in enumerate(bands):
            file_o = os.path.join(input_olci_path, yyyy, jjj, f'O{yyyy}{jjj}-rrs{band}-{region.lower()}-fr.nc')
            if os.path.exists(file_o):
                line = f'ncatted -h -a  valid_min,RRS{band},o,d,{min_values[iband]} {file_o}'
                add_new_line(fw, line)

        fw.close()
        file_list.append(fslurm)
        date_str_list.append(work_date.strftime("%Y-%m-%d"))
        work_date = work_date + timedelta(hours=24)

    file_sh = os.path.join(dir_log, f'Launcher_{int(dt.now().timestamp())}.sh')
    file_mail = os.path.join(dir_log, f'Launcher_{int(dt.now().timestamp())}.mail')
    fmail = open(file_mail, 'w')
    fmail.write('LAUNCHING MULTIPLE CORRECTION OF NEGATIVE OLCI VALUES')
    add_new_line(fmail, f'Region: {region}')
    add_new_line(fmail, f'Start date: {start_date.strftime("%Y-%m-%d")}')
    add_new_line(fmail, f'End date: {end_date.strftime("%Y-%m-%d")}')
    add_new_line(fmail, f'SH file: {file_sh}')
    add_new_line(fmail, '')
    fmail.close()

    fw = open(file_sh, 'w')
    fw.write('#!/bin/bash')
    add_new_line(fw, '')
    add_new_line(fw, f'tfile={file_mail}')
    add_new_line(fw, '')

    for ifile, file in enumerate(file_list):
        if ifile >= nmax:
            iwait = ifile - nmax
            line = f'job{ifile}=$(sbatch --dependency=afterany:$jobid{iwait} {file})'
        else:
            line = f'job{ifile}=$(sbatch {file})'
        add_new_line(fw, line)
        line = f'jobid{ifile}=$(echo "$job{ifile}" | awk \'{{print $NF}}\')'
        add_new_line(fw, line)
        if ifile >= nmax:
            line = f'echo " Date: {date_str_list[ifile]} Slurm id: $jobid{ifile} Log file: {file.replace(".slurm", ".log")} Processed after slurm id: $jobid{iwait}">>$tfile'
        else:
            line = f'echo " Date: {date_str_list[ifile]} Slurm id: $jobid{ifile} Log file: {file.replace(".slurm", ".log")} ">>$tfile'
        add_new_line(fw, line)
        add_new_line(fw, '')

    add_new_line(fw, '')
    add_new_line(fw, '')
    add_new_line(fw, '')
    add_new_line(fw, '##start e-mail')
    add_new_line(fw,
                 f'subject="LAUNCH MULTIPLE OLCI MERGING - {region} {start_date.strftime("%Y-%m-%d")} - {end_date.strftime("%Y-%m-%d")}"')
    add_new_line(fw,
                 f'mailrcpt="luis.gonzalezvilas@artov.ismar.cnr.it,lorenzo.amodio@artov.ismar.cnr.it,filippo.manfredonia@artov.ismar.cnr.it"')
    add_new_line(fw, f'cat $tfile | mail -s "$subject" "$mailrcpt"')

    fw.close()

    import subprocess
    cmd = f'sh {file_sh}'
    prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
    out, err = prog.communicate()
    if err:
        print(f'[ERROR]{err}')


def add_new_line(fw, line):
    fw.write('\n')
    fw.write(line)


def get_min_rrs_value(file, band):
    dataset = Dataset(file, 'r')
    array = dataset.variables[f'RRS{band}'][:]
    min_v = np.ma.min(array)
    attr = dataset.variables[f'RRS{band}'].valid_min
    ninvalid_s = np.ma.count(array[array < (-400)])
    dataset.close()
    return f'{min_v}', attr, ninvalid_s


def main():
    print('[INFO] Started utils')

    if args.mode == 'test':

        folder_olci = '/dst04-data1/OC/OLCI/daily_v202311_bc'
        start_date = dt(2018, 5, 15)
        end_date = dt(2024, 12, 31)
        bands = ['412', '443', '490', '510', '555', '670']
        for band in bands:
            work_date = start_date
            file_band = f'/store/COP2-OC-TAC/INCIDENTS/ISSUE_OLCI_NEGRRS/CSV_TEST/ObBand_{band}.csv'
            fw = open(file_band, 'w')
            fw.write('Date;MinValue;NMin')
            while work_date <= end_date:
                yyyy = work_date.strftime('%Y')
                jjj = work_date.strftime('%j')
                file = os.path.join(folder_olci, yyyy, jjj, f'Ob{yyyy}{jjj}-rrs{band}-bs-hr.nc')
                if not os.path.exists(file):
                    work_date = work_date + timedelta(hours=24)
                    continue
                dataset = Dataset(file, 'r')
                array = dataset.variables[f'RRS{band}'][:]
                array = np.ma.masked_invalid(array)
                min_v = np.ma.min(array)
                if min_v < 0:
                    indices = np.where(np.logical_and(array.mask==False,array < 0))
                    line = f'{work_date.strftime("%Y-%m-%d")};{np.ma.min(array)};{len(indices[0])}'
                    fw.write('\n')
                    fw.write(line)
                dataset.close()
                work_date = work_date + timedelta(hours=24)
            fw.close()

        # input_basic = '/mnt/c/DATA_LUIS/OCTAC_WORK/INC_NEG_OLCI_VALUES/original/2022/139'
        # input_slurm = '/mnt/c/DATA_LUIS/OCTAC_WORK/INC_NEG_OLCI_VALUES/slurm/2022/139'
        # bands = ['400', '412_5', '442_5', '490', '510', '560', '620', '665', '673_75', '681_25', '708_75', '753_75',
        #          '778_75', '865', '885', '1020']
        #
        # for iband,band in enumerate(bands):
        #     file_b = os.path.join(input_basic, f'O2022139-rrs{band}-bs-fr.nc')
        #     file_s = os.path.join(input_slurm, f'O2022139-rrs{band}-bs-fr.nc')
        #     dataset_b = Dataset(file_b)
        #     dataset_s = Dataset(file_s)
        #     array_b = dataset_b.variables[f'RRS{band}'][:]
        #     ninvalid_b = np.ma.count(array_b[array_b<(-400)])
        #     array_s = dataset_s.variables[f'RRS{band}'][:]
        #     ninvalid_s = np.ma.count(array_s[array_s < (-400)])
        #     print(f'{band} Original: {np.ma.min(array_b)} {np.ma.max(array_b)} Valid min.: {dataset_b.variables[f"RRS{band}"].valid_min} Invalid: {ninvalid_b}')
        #     print(
        #         f'{band} Slurm: {np.ma.min(array_s)} {np.ma.max(array_s)} Valid min.: {dataset_s.variables[f"RRS{band}"].valid_min} Invalid: {ninvalid_s}')
        #     print('-----------------------')
        #
        #     dataset_b.close()
        #     dataset_s.close()

    if args.mode == 'check_neg_olci_values':
        arguments = ['input_path', 'output', 'start_date', 'end_date', 'region']
        if not check_required_params(arguments):
            return
        if not check_directory(args.input_path, False):
            return
        if not check_output_file(args.output, 'csv'):
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        check_neg_olci_values(args.input_path, args.output, start_date, end_date, args.region)

    if args.mode == 'correct_neg_olci_values' or args.mode == 'correct_neg_olci_values_slurm':
        arguments = ['input_path', 'start_date', 'end_date', 'region']
        if not check_required_params(arguments):
            return
        if not check_directory(args.input_path, False):
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        if args.mode == 'correct_neg_olci_values':
            correct_neg_olci_values(args.input_path, start_date, end_date, args.region)

        if args.mode == 'correct_neg_olci_values_slurm':
            correct_neg_olci_values_slurm(args.input_path, start_date, end_date, args.region)


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
