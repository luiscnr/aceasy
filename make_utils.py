import warnings, argparse, os
from datetime import datetime as dt
from datetime import timedelta
from netCDF4 import Dataset

import numpy as np

warnings.filterwarnings("ignore", category=UserWarning)

parser = argparse.ArgumentParser(
    description="Obtaining information for running MDB_builder.")

parser.add_argument('-m', "--mode", help='Mode option', choices=["check_neg_olci_values", "correct_neg_olci_values","correct_neg_olci_values_slurm"],
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
            correct_neg_olci_values_impl(file_o,file_out,file_a,file_b,neg_band)
            os.rename(file_out,file_o)

        work_date = work_date + timedelta(hours=24)

def correct_neg_olci_values_impl(input_file,output_file,file_a,file_b,neg_band):
    input_dataset = Dataset(input_file)
    ncout = Dataset(output_file, 'w', format='NETCDF4')


    #new_array = input_array.copy()
    dataset_a = Dataset(file_a)
    array_a = dataset_a.variables[neg_band][:]
    dataset_a.close()
    dataset_b = Dataset(file_b)
    array_b = dataset_b.variables[neg_band][:]
    dataset_b.close()

    new_array = np.ma.mean(np.ma.concatenate([array_a,array_b],axis=0),axis=0)


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


def correct_neg_olci_values_slurm(dir_log,start_date, end_date, region):
    work_path = '/home/gosuser/Processing/gos-oc-processingchains_v202411/s3olciProcessing'
    line_py = f'python {work_path}/make_merge_olci_202311.new.py -d DATE -a {region.lower()} -p RRS -v'
    file_list = []
    date_str_list = []
    work_date = start_date
    while work_date<=end_date:
        fslurm = os.path.join(dir_log,f'LaunchMergeOLCI_{work_date.strftime("%Y%m%d")}.slurm')
        fw = open(fslurm,'w')
        fw.write('#!/bin/bash')
        add_new_line(fw,'#SBATCH --nodes=1')
        add_new_line(fw,'#SBATCH --ntasks=1')
        add_new_line(fw, f'#SBATCH --output {fslurm.replace(".slurm",".log")}')
        add_new_line(fw,'#SBATCH -p octac_rep')
        add_new_line(fw,'#SBATCH --mail-type=BEGIN,END,FAIL')
        add_new_line(fw,'#SBATCH --mail-user=luis.gonzalezvilas@artov.ismar.cnr.it,lorenzo.amodio@artov.ismar.cnr.it')
        add_new_line(fw,'')
        add_new_line(fw,'source /home/gosuser/load_miniconda3.source')
        add_new_line(fw,'conda activate op_proc_202211v2')
        add_new_line(fw,f'cd {work_path}')
        add_new_line(fw,'')
        line_py = line_py.replace("DATE",work_date.strftime('%Y-%m-%d'))
        add_new_line(fw,line_py)
        fw.close()
        file_list.append(fslurm)
        date_str_list.append(work_date.strftime("%Y-%m-%d"))
        work_date = work_date + timedelta(hours=24)

    file_sh = os.path.join(dir_log,f'Launcher_{int(dt.now().timestamp())}.sh')
    file_mail = os.path.join(dir_log,f'Launcher_{int(dt.now().timestamp())}.mail')
    fmail = open(file_mail,'w')
    fmail.write('LAUNCHING MULTIPLE CORRECTION OF NEGATIVE OLCI VALUES')
    add_new_line(fmail,f'Region: {region}')
    add_new_line(fmail, f'Start date: {start_date.strftime("%Y-%m-%d")}')
    add_new_line(fmail, f'End date: {end_date.strftime("%Y-%m-%d")}')
    add_new_line(fmail, f'SH file: {file_sh}')
    add_new_line(fmail,'')
    fmail.close()

    nmax = 12

    fw = open(file_sh,'w')
    fw.write('#!/bin/bash')
    add_new_line(fw,'')
    add_new_line(fw,f'tfile={file_mail}')
    add_new_line(fw,'')

    for ifile,file in enumerate(file_list):
        if ifile>=nmax:
            iwait = ifile-nmax
            line = f'job{ifile}=$(sbatch --dependency=afterany:$jobid{iwait} {file})'
        else:
            line = f'job{ifile}=$(sbatch {file})'
        add_new_line(fw, line)
        line = f'jobid{ifile}=$(echo "$job{ifile}" | awk \'{{print $NF}}\')'
        add_new_line(fw, line)
        if ifile>=nmax:
            line = f'echo " Date: {date_str_list[ifile]} Slurm id: $jobid{ifile} Processed after slurm id: $jobid{iwait}">>$tfile'
        else:
            line = f'echo " Date: {date_str_list[ifile]} Slurm id: $jobid{ifile}">>$tfile'
        add_new_line(fw, line)
        add_new_line(fw,'')


    add_new_line(fw,'')
    add_new_line(fw,'')
    add_new_line(fw,'')
    add_new_line(fw,'##start e-mail')
    add_new_line(fw,f'mailrcpt="luis.gonzalezvilas@artov.ismar.cnr.it,lorenzo.amodio@artov.ismar.cnr.it,filippo.manfredonia@artov.ismar.cnr.it"')
    add_new_line(fw,f'cat $tfile | mail -s "$subject" "$mailrcpt"')

    fw.close()

def add_new_line(fw,line):
    fw.write('\n')
    fw.write(line)

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

    if args.mode == 'correct_neg_olci_values' or args.mode=='correct_neg_olci_values_slurm':
        arguments = ['input_path', 'start_date', 'end_date', 'region']
        if not check_required_params(arguments):
            return
        if not check_directory(args.input_path, False):
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        if args.mode=='correct_neg_olci_values':
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
