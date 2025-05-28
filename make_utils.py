import sys
import warnings, argparse, os, configparser
import xxlimited
from datetime import datetime as dt
from datetime import timedelta

import pandas as pd
from netCDF4 import Dataset

import numpy as np

warnings.filterwarnings("ignore", category=UserWarning)

parser = argparse.ArgumentParser(
    description="Obtaining information for running MDB_builder.")

parser.add_argument('-m', "--mode", help='Mode option',
                    choices=["test", "check_neg_olci_values", "correct_neg_olci_values",
                             "correct_neg_olci_values_slurm","image_stats","compare_images","cyano_stats","plot_cyano","sensormask_stats"],
                    required=True)
parser.add_argument('-i', "--input_path", help="Input path.")
parser.add_argument('-o', "--output", help="Output file.")
parser.add_argument('-c',"--config_file", help="Configuration file")
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

def make_stats(options_stats,start_date,end_date,output_file):
    print(f'[INFO] Started making stats...')
    basic_stats = ['N','AVG','STD','MEDIAN','MIN','MAX','P25','P75','RANGE','IQR']
    corr_stats = ['N', 'SLOPE_I', 'SLOPE_II', 'OFFSET_I', 'OFFSET_II', 'STD_ERR_I', 'STD_SLOPE_II', 'STD_OFFSET_II',
                   'R', 'P_VALUE', 'R2', 'RMSD','BIAS','MdBIAS','APD','RPD','MdAPD','MdRPD','CRMSE','MAD','MdAD']

    col_names = ['DATE']
    for var in options_stats['var_list']:
        for stat in basic_stats:
            col_names.append(f'{var}_{stat}')

    if options_stats['alt_var_list'] is not None:
        for var in options_stats['alt_var_list']:
            for stat in basic_stats:
                col_names.append(f'alt_{var}_{stat}')

    if options_stats['correlation_stats_var'] is not None:
        for var_pair in options_stats['correlation_stats_var']:
            for stat in corr_stats:
                col_names.append(f'{var_pair[0]}_{var_pair[1]}_{stat}')

    if options_stats['alt_correlation_stats_var'] is not None:
        for var_pair in options_stats['alt_correlation_stats_var']:
            for stat in corr_stats:
                col_names.append(f'{var_pair[0]}_alt_{var_pair[1]}_{stat}')

    use_log = options_stats['use_log']
    alt_use_log = options_stats['alt_use_log']
    var_list = options_stats['var_list']
    alt_var_list = options_stats['alt_var_list']
    ndates = (end_date-start_date).days+1
    df = pd.DataFrame(index=range(ndates),columns=col_names)
    work_date = start_date
    index = 0
    while work_date<=end_date:
        print(f'[INFO] Working stats for day: {work_date}')
        df.loc[0,"DATE"] = work_date.strftime('%Y-%m-%d')
        ##BASIC STATS
        arrays = get_arrays_for_stats(options_stats,work_date,False)
        if arrays is not None:
            for ivar,var in enumerate(var_list):
                use_log_here = use_log[ivar] if len(use_log)==len(var_list) else use_log[0]
                results = compute_basic_stats(arrays[var],var,use_log_here)
                df.loc[0,results.keys()] = results.values()

        ##ALT BASIC STATS
        if options_stats['alt_input_path'] is not None:
            arrays_alt = get_arrays_for_stats(options_stats, work_date, True)
            if arrays_alt is not None:
                for ivar, var in enumerate(alt_var_list):
                    use_log_here = alt_use_log[ivar] if len(alt_use_log) == len(alt_var_list) else alt_use_log[0]
                    results = compute_basic_stats(arrays_alt[var], f'alt_{var}', use_log_here)
                    df.loc[0, results.keys()] = results.values()

        ##CORRELATION STATS
        if options_stats['correlation_stats_var'] is not None:
            for var_pair in enumerate(options_stats['correlation_stats_var']):
                ivar_0 = var_list.index(var_pair[0])
                use_log_here_0 = use_log[ivar_0] if len(use_log) == len(var_list) else use_log[0]
                ivar_1 = var_list.index(var_pair[0])
                use_log_here_1 = use_log[ivar_1] if len(use_log) == len(var_list) else use_log[0]
                use_log_here = use_log_here_0 if use_log_here_0==use_log_here_1 else None
                if use_log_here is None:
                    print(f'[WARNING] Variables to compute correlations should both have the same use_log values')
                else:
                    array_0 = arrays[var_pair[0]]
                    array_1 = arrays[var_pair[1]]
                    var_prefix = f'{var_pair[0]}_{var_pair[1]}_'
                    results = compute_correlation_stats(array_0,array_1,var_prefix,use_log_here)

        index = index + 1
        work_date = work_date+timedelta(hours=24)

def compute_correlation_stats(xarray,yarray,xbitmask,ybitmask,var_prefix,use_log):
    indices_non_mask = np.where(np.logical_and(xarray.mask==False,yarray.mask==False))
    xarray = xarray[indices_non_mask].compressed()
    yarray = yarray[indices_non_mask].compressed()
    if xbitmask is not None and ybitmask is not None:
        xbitmask = xbitmask[indices_non_mask].compressed()
        ybitmask = ybitmask[indices_non_mask].compressed()


    if use_log:
        valid_array = np.logical_and(yarray > 0, xarray > 0)
        xarray = xarray[valid_array]
        yarray = yarray[valid_array]

    rel_diff = 100 * ((yarray - xarray) / xarray) #for computing APD, RPD, MdRPD, MdAPD
    if use_log:
        xarray = np.log10(xarray)
        yarray = np.log10(yarray)

    try:
        from scipy import stats
        slope, intercept, r_value, p_value, std_err = stats.linregress(xarray, yarray)
    except:
        slope, intercept, r_value, p_value, std_err = [-999.0]*5

    try:
        from pylr2 import regress2
        results_r2 = regress2(np.array(xarray, dtype=np.float64), np.array(yarray, dtype=np.float64),_method_type_2="reduced major axis")
        slope_II = results_r2['slope']
        intercept_II = results_r2['intercept']
        std_slope_II = results_r2['std_slope']
        std_intercept_II = results_r2['std_intercept']
    except:
        slope_II,intercept_II,std_slope_II,std_intercept_II = [-999.0]*4

    xdiff = xarray - np.mean(xarray)
    ydiff = yarray - np.mean(yarray)

    abs_diff = np.abs(rel_diff)
    adiff = np.abs(yarray - xarray)
    histo,bin_edges = np.histogram(adiff,1000)
    res_histo = pd.DataFrame(index=range(1000),columns=['min','max','num'])
    res_histo['min'] = bin_edges[:-1]
    res_histo['max'] = bin_edges[1:]
    res_histo['num'] = histo[:]


    #n_low_adiff = np.count_nonzero(adiff<1e-4)
    # n_bad = np.count_nonzero(adiff>1e-4)
    # xbitmask_bad = xbitmask[adiff>1e-4]
    # ybitmask_bad = ybitmask[adiff>1e-4]
    # nx_zero = np.count_nonzero(xbitmask_bad==0)
    # nx_case2 = np.count_nonzero(xbitmask_bad == 1024)
    # ny_zero = np.count_nonzero(ybitmask_bad == 0)
    # ny_case2 = np.count_nonzero(ybitmask_bad == 1024)
    # # xbitmask_bad_unique = np.unique(xbitmask_bad)
    # # ybitmask_bad_unique = np.unique(ybitmask_bad)
    # print(f'{adiff.shape[0]} {n_low_adiff} {n_bad} {n_low_adiff+n_bad}')
    # print(f'{len(xbitmask_bad)}->{nx_zero}->{nx_case2}')
    # print(f'{len(ybitmask_bad)}->{ny_zero}->{ny_case2}')

    results = {
        f'{var_prefix}_N': xarray.shape[0],
        f'{var_prefix}_SLOPE_I': slope,
        f'{var_prefix}_SLOPE_II': slope_II,
        f'{var_prefix}_OFFSET_I': intercept,
        f'{var_prefix}_OFFSET_II': intercept_II,
        f'{var_prefix}_STD_ERR_I': std_err,
        f'{var_prefix}_STD_SLOPE_II': std_slope_II,
        f'{var_prefix}_STD_OFFSET_II': std_intercept_II,
        f'{var_prefix}_R': r_value,
        f'{var_prefix}_PVALUE': p_value,
        f'{var_prefix}_R2': r_value * r_value,
        f'{var_prefix}_RMSD': rmse(yarray,xarray),
        f'{var_prefix}_BIAS': np.mean(yarray - xarray),
        f'{var_prefix}_MdBIAS': np.median(yarray-xarray),
        f'{var_prefix}_APD': np.mean(abs_diff),
        f'{var_prefix}_RPD': np.mean(rel_diff),
        f'{var_prefix}_MdAPD': np.median(abs_diff),
        f'{var_prefix}_MdRPD': np.median(rel_diff),
        f'{var_prefix}_CRMSE': rmse(ydiff,xdiff),
        f'{var_prefix}_MAD': np.mean(adiff),
        f'{var_prefix}_MdAD': np.median(adiff),
        'HISTO_RES': res_histo
    }

    if use_log:
        ##convert statistict to linear scale again
        stats_to_convert = ['RMSD', 'CRMSE', 'MAD', 'MdAD']
        for stat in stats_to_convert:
            results[f'{var_prefix}_{stat}'] = np.power(10,results[f'{var_prefix}_{stat}'])
        sign_stats_to_convert = ['BIAS', 'MdBIAS']
        for stat in sign_stats_to_convert:
            bias_neg = results[f'{var_prefix}_{stat}'] < 0
            results[f'{var_prefix}_{stat}'] = np.power(10, np.abs(results[f'{var_prefix}_{stat}']))
            if bias_neg:
                results[f'{var_prefix}_{stat}'] = results[f'{var_prefix}_{stat}'] * (-1)

    return results




    # self.valid_stats['RPD'] = np.mean(rel_diff)
    # #  the mean of absolute (unsigned) percent differences
    # self.valid_stats['APD'] = np.mean(np.abs(rel_diff))
    # the median of relative (signed) percent differences
    # rel_diff = 100 * ((sat_obs - ref_obs) / ref_obs)
    # self.valid_stats['MdRPD'] = np.median(rel_diff)
    # #  the median of absolute (unsigned) percent differences
    # self.valid_stats['MdAPD'] = np.median(np.abs(rel_diff))



def rmse(predictions, targets):
    return np.sqrt(((np.asarray(predictions) - np.asarray(targets)) ** 2).mean())

def compute_basic_stats(array,var_name,use_log):
    #basic_stats = ['N', 'AVG', 'STD', 'MEDIAN', 'MIN', 'MAX', 'P25', 'P75', 'RANGE', 'IQR']
    data = array.compressed()
    if use_log:
        data = np.log10(data)
    results = {
        f'{var_name}_N': data.shape[0],
        f'{var_name}_AVG': np.mean(data),
        f'{var_name}_STD': np.std(data),
        f'{var_name}_MEDIAN': np.median(data),
        f'{var_name}_MIN': np.min(data),
        f'{var_name}_MAX': np.max(data),
        f'{var_name}_P25': np.percentile(data,25),
        f'{var_name}_P75': np.percentile(data,75),
        f'{var_name}_RANGE': -999.0,
        f'{var_name}_IQR': -999.0,
    }
    results[f'{var_name}_RANGE']=results[f'{var_name}_MAX']-results[f'{var_name}_MIN']
    results[f'{var_name}_IQR'] = results[f'{var_name}_P75'] - results[f'{var_name}_P25']

    return results

def get_arrays_for_stats(options_stats,work_date,isalt):
    input_path = options_stats['alt_input_path'] if isalt else options_stats['input_path']
    org = options_stats['alt_input_path_organization'] if isalt else options_stats['input_path_organization']
    dataset_name_file = options_stats['alt_dataset_name_file'] if isalt else options_stats['dataset_name_file']
    dataset_name_format_date = options_stats['alt_dataset_name_format_date'] if isalt else options_stats['dataset_name_format_date']
    file_date = get_file_date(input_path,org,dataset_name_file,dataset_name_format_date,work_date)
    if file_date is None:
        return None
    arrays = {}
    var_list = options_stats['alt_var_list'] if isalt else options_stats['var_list']
    for var in var_list:
        dataset = Dataset(file_date,'r')
        arrays[var] = dataset.variables[var][:]
        dataset.close()
    return arrays

def get_file_date(input_path,org,dataset_name_file,dataset_name_format_date,work_date):

    input_path_date = input_path
    if org!='NONE':
        for tformat in org.split('/'):
            input_path_date = os.path.join(input_path_date,work_date.strftime(tformat))

    if not os.path.isdir(input_path_date):
        print(f'[WARNING] Input path for date {work_date.strftime("%Y-%m-%d")}: {input_path_date} is not available. Skipping...')
        return None

    name_file = dataset_name_file.replace('$DATE$',work_date.strftime(dataset_name_format_date))
    file_date = os.path.join(input_path_date,name_file)
    if not os.path.isfile(file_date):
        print(f'[WARNING] File date for date {work_date.strftime("%Y-%m-%d")}: {file_date} is not available. Skipping...')
        return None

    return file_date

def make_chl_comparison():
    dir_out = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/POLYMER_TEST_V5/comparison_valid_v4_v5'
    dir_1 = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/POLYMER_TEST_V5/v4'
    dir_2 = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/POLYMER_TEST_V5/v5'
    file_1 = os.path.join(dir_1,'S3B_OL_2_WFR____20240619T094235_20240619T094535_20240620T122332_0179_094_193_1980_MAR_O_NT_002_POLYMER_BAL202411.nc')
    file_2 = os.path.join(dir_2,'S3B_OL_2_WFR____20240619T094235_20240619T094535_20240620T122332_0179_094_193_1980_MAR_O_NT_002_POLYMER_OUT_BAL202411.nc')

    dataset_1 = Dataset(file_1)
    chla_1 = dataset_1.variables['CHL'][:]
    dataset_1.close()

    dataset_2 = Dataset(file_2)
    chla_2 = dataset_2.variables['CHL'][:]
    dataset_2.close()

    valid = np.logical_and(chla_1.mask==False,chla_2.mask==False)

    chla_1 = chla_1[valid].compressed()
    chla_2 = chla_2[valid].compressed()
    dif_log_abs = np.abs(np.log10(chla_1)-np.log10(chla_2))
    valid = np.zeros(chla_1.shape)
    valid[dif_log_abs > 1e-3] = 1
    valid[dif_log_abs > 1e-2] = 2
    valid[dif_log_abs > 1e-1] = 3
    array_end = np.column_stack([chla_1, chla_2,valid])

    nvalid = chla_1.shape[0]
    df = pd.DataFrame(index=range(nvalid),columns=['v4','v5','valid'],data=array_end)
    file_out = os.path.join(dir_out,'chla_comparison.csv')
    df.to_csv(file_out,sep=';',index=False)

    for idx in range(4):
        df_here = df[df['valid']==idx]
        file_out = os.path.join(dir_out, f'chla_comparison_{idx}.csv')
        df_here.to_csv(file_out,sep=';',index=False)







def make_image_comparison():
    dir_out = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/POLYMER_TEST_V5/comparison_valid_v4_v5'
    dir_1 = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/POLYMER_TEST_V5/v4'
    dir_2 = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/POLYMER_TEST_V5/v5'
    cols = ['v4','v5','bitmask_1','bitmask_2','bitmask','valid']
    wl_list = ['400','412','443','490','510','560','620','665','674','681','709','754','779','865']
    # wl_list = ['412', '443', '490', '510', '560', '620', '665', '674', '681', '709', '754', '779', '865']
    # wl_list = ['400']

    file_stats = os.path.join(dir_out,'Stats_Valid.csv')
    col_stats = ['NAME','WL','N4_14','N15','NCOMMON','R2','RMSD','MdBIAS','MdAD','MdRPD','MdAPD','N_VALID','N_ADIF-4','N_ADIF-3','N_ADIF-2','P_VALID','P_ADIF-4','P_ADIF-3','P_ADIF-2']
    df_stats = None

    for name in os.listdir(dir_1):

        file_1 = os.path.join(dir_1,name)
        file_2 = os.path.join(dir_2,name)
        if os.path.exists(file_1) and os.path.exists(file_2):
            print(f'[INFO] File: {name}')
            dataset_1 = Dataset(file_1)
            dataset_2 = Dataset(file_2)

            ##flagging analysis
            ##v.4 or v. 4.14
            # desc = dataset_1.variables['bitmask'].description
            # flags = [int(x.strip().split(':')[1]) for x in desc.split(',')]
            # flags_meanings = [x.strip().split(':')[0] for x in desc.split(',')]
            # meanings = " ".join(flags_meanings)
            #bitmask = dataset_1.variables['bitmask'][:]

            ##v.5
            # flags = dataset_2.variables['flags'].flag_masks
            # meanings = dataset_2.variables['flags'].flag_meanings
            # flags_meanings = [x.strip() for x in meanings.split(' ')]
            # bitmask = dataset_2.variables['flags'][:]
            #
            # sys.path.append('/home/lois/PycharmProjects/hypernets_val/COMMON')
            # from Class_Flags_OLCI import Class_Flags_Polymer
            # cPolymer = Class_Flags_Polymer(flags,meanings)
            # bitmask = bitmask.compressed()
            # for flag in flags_meanings:
            #     flag_array = cPolymer.Mask(bitmask,[flag])
            #     nflag = np.count_nonzero(flag_array>0)
            #     print(f'{flag};{nflag}')




            for wl in wl_list:
                print(f'[INFO] --> {wl}')
                stats_here = {key:[''] for key in col_stats}.copy()
                stats_here['NAME'][0] = name
                stats_here['WL'][0] = wl

                array_1 = dataset_1.variables[f'Rw{wl}'][:]
                nprev_1 = np.ma.count(array_1)
                bitmask_1 = dataset_1.variables['bitmask'][:]
                valid_1 = np.logical_or(bitmask_1==0,bitmask_1==1024)
                #array_1[np.bitwise_and(bitmask_1,1023)!=0] = np.ma.masked
                array_1[valid_1==False] = np.ma.masked
                bitmask_1[array_1.mask] = np.ma.masked
                nafter_1 = np.ma.count(array_1)

                array_2 = dataset_2.variables[f'rho_w_{wl}'][:]
                nprev_2 = np.ma.count(array_2)
                bitmask_2 =dataset_2.variables['flags'][:]
                valid_2 = np.logical_or(bitmask_2 == 0, bitmask_2 == 1024)
                #array_2[np.bitwise_and(bitmask_2,1023) != 0] = np.ma.masked
                array_2[valid_2 == False] = np.ma.masked
                bitmask_2[array_2.mask] = np.ma.masked
                nafter_2 = np.ma.count(array_2)

                print(f'[INFO] Array 1: {nprev_1}->{nafter_1}  Array 2: {nprev_2}->{nafter_2} ')
                stats_here['N4_14'][0] = nafter_1
                stats_here['N15'][0] = nafter_2

                ##working with common indices
                results_here = compute_correlation_stats(array_1,array_2,bitmask_1,bitmask_2,'x',False)
                indices_common = np.where(np.logical_and(array_1.mask==False,array_2.mask==False))
                array_1 = array_1[indices_common].compressed()
                array_2 = array_2[indices_common].compressed()

                stats_here['NCOMMON'][0] = array_1.shape[0]
                stats_here['R2'][0] = results_here['x_R2']
                stats_here['RMSD'][0] = results_here['x_RMSD']
                stats_here['MdBIAS'][0] = results_here['x_MdBIAS']
                stats_here['MdAD'][0] = results_here['x_MdAD']
                stats_here['MdRPD'][0] = results_here['x_MdRPD']
                stats_here['MdAPD'][0] = results_here['x_MdAPD']
                #stats_here['N_LOW_ADIFF'][0] = results_here['x_N_LOW_ADIFF']

                # histo_res = results_here['HISTO_RES']
                # index_min = np.min(np.where(histo_res['num']<3000))
                # th = histo_res.loc[index_min,'min']
                th_1 = 1e-4
                th_2 = 1e-3
                th_3 = 1e-2

                bitmask_1 = bitmask_1[indices_common].compressed()
                bitmask_2 = bitmask_2[indices_common].compressed()
                bitmask = np.zeros(bitmask_1.shape)
                bitmask[np.logical_or(bitmask_1 > 0, bitmask_2 > 0)] = 1
                a_diff = np.abs(array_1-array_2)
                a_diff_th = np.zeros(a_diff.shape)
                a_diff_th[a_diff > th_1] = 1
                a_diff_th[a_diff > th_2] = 2
                a_diff_th[a_diff > th_3] = 3

                array_end = np.column_stack((array_1, array_2, bitmask_1, bitmask_2, bitmask,a_diff_th))
                file_c = os.path.join(dir_out, name[:-3] + f'_{wl}.csv')
                df = pd.DataFrame(index=range(array_1.shape[0]),columns=cols,data = array_end)
                df.to_csv(file_c,sep=';',index=False)

                histo_res = results_here['HISTO_RES']
                file_histo = os.path.join(dir_out,f'histo_{wl}.csv')
                histo_res.to_csv(file_histo,sep=';',index=False)



                stats_here['N_VALID'][0] =  np.count_nonzero(a_diff_th==0)
                stats_here['N_ADIF-4'][0] = np.count_nonzero(a_diff_th==1)
                stats_here['N_ADIF-3'][0] = np.count_nonzero(a_diff_th==2)
                stats_here['N_ADIF-2'][0] = np.count_nonzero(a_diff_th == 3)
                stats_here['P_VALID'][0] = (stats_here['N_VALID'][0]/array_1.shape[0])*100
                stats_here['P_ADIF-4'][0] = (stats_here['N_ADIF-4'][0]/array_1.shape[0])*100
                stats_here['P_ADIF-3'][0] = (stats_here['N_ADIF-3'][0] / array_1.shape[0]) * 100
                stats_here['P_ADIF-2'][0] = (stats_here['N_ADIF-2'][0] / array_1.shape[0]) * 100

                if df_stats is None:
                    df_stats = pd.DataFrame(stats_here)
                else:
                    df_stats = pd.concat([df_stats,pd.DataFrame(stats_here)],ignore_index=True)

            dataset_1.close()
            dataset_2.close()
    print(f'[INFO] Saving stats...')
    if df_stats is not None:
        df_stats.to_csv(file_stats, sep=';', index=False)
    print(f'[INFO] Completed')

def make_cyano_stats():


    dir_base = '/store3/OC/OLCI_BAL/dailyolci_202411'
    work_date = dt(2016, 4, 26)
    end_date = dt(2024, 12, 31)

    file_out = os.path.join(dir_base, f'CyanoOlci.csv')
    fw = open(file_out, 'w')
    fw.write('Year;NValid;NSubSurface;NSurface;NBoth;NAny')
    year_ref = 0
    count = None
    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        jday = int(jjj)
        if jday<161 or jday>270:
            work_date = work_date + timedelta(hours=24)
            continue
        file_data = os.path.join(dir_base,yyyy,jjj,f'O{yyyy}{jjj}-chl-bal-fr.nc')

        if os.path.exists(file_data):
            if work_date.year!=year_ref:
                if year_ref>0:
                    line_year = f'{year_ref};{count[0]};{count[1]};{count[2]};{count[3]};{count[4]}'
                    fw.write('\n')
                    fw.write(line_year)
                year_ref = work_date.year
                count = [0]*5

            dataset = Dataset(file_data)
            data = np.ma.squeeze(dataset.variables['CYANOBLOOM'][:])
            dataset.close()
            data_valid = data.compressed()
            count[0] = count[0] + np.count_nonzero(data_valid>=0)
            count[1] = count[1] + np.count_nonzero(data_valid == 1)
            count[2] = count[2] + np.count_nonzero(data_valid == 2)
            count[3] = count[3] + np.count_nonzero(data_valid == 3)
            count[4] = count[4] + np.count_nonzero(data_valid >= 1)

        work_date = work_date + timedelta(hours=24)

    line_year = f'{year_ref};{count[0]};{count[1]};{count[2]};{count[3]};{count[4]}'
    fw.write('\n')
    fw.write(line_year)
    fw.close()

def plot_cyano():
    import os
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import matplotlib as mpl
    import matplotlib.gridspec as gridspec

    ## -- path for input and output
    dir_base = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CYANOBLOOM_EVOLUTION'
    ## -- input
    file_cyano = os.path.join(dir_base, 'CyanoEvolution_CyanoPeriod.csv')
    ## -- output
    file_out = os.path.join(dir_base, 'CyanoEvolutionPlot.png')

    # -- Reading input data
    df_cyano = pd.read_csv(file_cyano, sep=';')
    sub = np.array(df_cyano['CoverageSub'][1:])
    sur = np.array(df_cyano['CoverageSurface'][1:])
    both = np.array(df_cyano['CoverageBoth'][1:])

    year_start = 1998
    year_end = 2024

    # -- Figure size & division
    fig = plt.figure(figsize=(11.7, 8.3))
    gs = gridspec.GridSpec(2, 1, height_ratios=[100, 35])

    # -- Define style of the plot
    mpl.style.use('ggplot')

    # -- 1st axis (the 2nd one is for the logos)
    ax = plt.subplot(gs[0])

    # -- Legends & title
    plt.xlabel(' Year ', size='xx-large', fontweight='bold')
    plt.ylabel(r'Coverage area (day·km$^2$·10$^6$)', size='xx-large', fontweight='bold')

    tt = plt.title('Baltic Sea summer bloom coverage (1998-2024)', fontsize=18)
    tt.set_position([0.5, 1.05])

    ## -- Time axis
    time = np.arange(year_start, year_end + 1, 1)

    ax.set_xticks(time)
    time_labels = [str(x) if (x % 2) == 0 else '' for x in time]
    ax.set_xticklabels(time_labels, size='small')

    ax.set_xlim([time[0] - 1, time[-1] + 1])
    ax.tick_params(labelsize=12)

    # -- Plotting data
    hsur = plt.bar(time, sur, color=(1.0, 0.65, 0), linewidth=1.0, edgecolor='k')
    hboth = plt.bar(time, both, bottom=sur, color=(0.58, 0.44, 0.86), linewidth=1.0, edgecolor='k')
    hsub = plt.bar(time, sub, bottom=sur + both, color=(0.0, 0.0, 1.0), linewidth=1.0, edgecolor='k')

    # -- Y-axis
    ydata = [0.0e6, 0.5e6, 1.0e6, 1.5e6, 2.0e6, 2.5e6, 3.0e6]
    yticks = ['0', '0.5', '1.0', '1.5', '2.0', '2.5', '3.0']
    plt.yticks(ydata, yticks)
    plt.grid(True, ls='dotted', alpha=0.6)

    # -- Legend
    str_legend = ['Subsurface bloom (Rrs555)', 'Concurrent bloom', 'Surface bloom(Rrs670)']
    plt.legend([hsub, hboth, hsur], str_legend, loc=9, fontsize=12)

    # -- Credits and datatype
    ax.text(1997.5, 2.9e6,
            s='Datatype : Multi-Sensor Satellite Observation \nCredit : E.U. Copernicus Marine Service Information',
            bbox={'facecolor': 'white', 'alpha': 0.5}, fontsize=9)

    # -- Add the logos as subplot
    logo = plt.imread(os.path.join(dir_base, 'LogosOMIBand-100-mv.png'))
    axlogo = plt.subplot(gs[1])
    axlogo.imshow(logo)
    axlogo.axis('off')

    # -- Figure caption
    axlogo.text(-20, 150,
                'Summer subsurface and surface bloom coverage time series for the Baltic Sea.\nConcurrent bloom are the areas where both surface and subsurface thresholds were exceeded.',
                style='italic', fontsize=13)

    # -- Graphical settings
    plt.subplots_adjust(wspace=0, hspace=-0.7)
    plt.tight_layout()

    ## -- Saving output file
    plt.savefig(file_out, dpi=300)


def plot_cyano_kk():
    from matplotlib import pyplot as plt
    import matplotlib.pyplot as plt
    import numpy as np
    import matplotlib as mpl
    from netCDF4 import Dataset
    import matplotlib.gridspec as gridspec
    from matplotlib.offsetbox import OffsetImage, AnnotationBbox
    from scipy.stats import linregress

    file_cyano = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CYANOBLOOM_EVOLUTION/CyanoEvolution_CyanoPeriod.csv'
    file_out = os.path.join(os.path.dirname(file_cyano), 'CyanoEvolutionPlot.png')
    df_cyano = pd.read_csv(file_cyano, sep=';')
    sub = np.array(df_cyano['CoverageSub'][1:])
    sur = np.array(df_cyano['CoverageSurface'][1:])
    both = np.array(df_cyano['CoverageBoth'][1:])
    year_start = 1998
    year_end = 2024

    # -- Figure size & division
    fig = plt.figure(figsize=(11.7, 8.3))
    gs = gridspec.GridSpec(2, 1, height_ratios=[100, 35])
    #plt.grid(True, ls='dotted', alpha=0.6)

    # -- Define style of the plot
    mpl.style.use('ggplot')

    # -- 1st axis (the 2nd one is for the logos)
    ax = plt.subplot(gs[0])

    # -- Legends & title
    plt.xlabel(' Year ', size='xx-large', fontweight='bold')
    plt.ylabel(r'Coverage area (day·km$^2$·10$^6$)', size='xx-large', fontweight='bold')

    tt = plt.title('Baltic Sea summer bloom coverage (1998-2024)', fontsize=18)
    tt.set_position([0.5, 1.05])

    time = np.arange(year_start, year_end + 1, 1)

    ax.set_xticks(time)
    time_labels = [str(x) if (x%2)==0 else '' for x in time]
    ax.set_xticklabels(time_labels,size='small')
    #ax.set_xticklabels(time, rotation=30, ha='left', size='small')  # size --> x-small,small,medium,large,...
    ax.set_xlim([time[0] - 1, time[-1] + 1])
    ax.tick_params(labelsize=12)


    hsur = plt.bar(time, sur, color=(1.0, 0.65, 0), linewidth=1.0, edgecolor='k')
    hboth = plt.bar(time, both, bottom=sur, color=(0.58, 0.44, 0.86), linewidth=1.0, edgecolor='k')
    hsub = plt.bar(time, sub, bottom=sur + both, color=(0.0, 0.0, 1.0), linewidth=1.0, edgecolor='k')

    ydata = [0.0e6, 0.5e6, 1.0e6, 1.5e6, 2.0e6, 2.5e6, 3.0e6]
    yticks = ['0', '0.5', '1.0', '1.5', '2.0', '2.5', '3.0']
    plt.yticks(ydata, yticks)
    plt.grid(True, ls='dotted', alpha=0.6)

    #plt.tick_params(axis='x', which='both', bottom=False)

    # -- Credits and datatype
    str_legend = ['Subsurface bloom (Rrs555)', 'Concurrent bloom', 'Surface bloom(Rrs670)']
    plt.legend([hsub,hboth,hsur],str_legend,loc=9,fontsize=12)

    ##datacredits,location using data coordinates
    ax.text(1997.5,2.9e6,s='Datatype : Multi-Sensor Satellite Observation \nCredit : E.U. Copernicus Marine Service Information',bbox = {'facecolor':'white', 'alpha':0.5},fontsize=9)

    # -- Add the logos as subplot
    logo = plt.imread(os.path.join(os.path.dirname(file_cyano),'LogosOMIBand-100-mv.png'))
    axlogo = plt.subplot(gs[1])
    img = axlogo.imshow(logo)
    axlogo.axis('off')

    # --

    axlogo.text(-20, 150, 'Summer subsurface and surface bloom coverage time series for the Baltic Sea.\nConcurrent bloom are the areas where both surface and subsurface thresholds were exceeded.', style='italic',fontsize=13)


    # -- Graphical settings
    plt.subplots_adjust(wspace=0, hspace=-0.7)
    plt.tight_layout()

    plt.savefig(file_out, dpi=300)

def plot_cyano_deprecated():
    file_cyano = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CYANOBLOOM_EVOLUTION/CyanoEvolution_CyanoPeriod.csv'
    file_out = os.path.join(os.path.dirname(file_cyano),'CyanoEvolutionPlot.png')
    df_cyano  = pd.read_csv(file_cyano,sep=';')
    sub = np.array(df_cyano['CoverageSub'][1:])
    sur = np.array(df_cyano['CoverageSurface'][1:])
    both = np.array(df_cyano['CoverageBoth'][1:])
    from matplotlib import pyplot as plt
    from matplotlib.ticker import AutoMinorLocator
    xdata = np.arange(1998,2025)
    xticks = [str(x) if (x%2)==0 else '' for x in xdata]
    plt.figure(figsize=(18.74,9.37),dpi=300)
    hsur = plt.bar(xdata,sur,color=(1.0,0.65,0), linewidth=1.0, edgecolor='k')
    hboth = plt.bar(xdata,both,bottom=sur,color=(0.58,0.44,0.86), linewidth=1.0, edgecolor='k')
    hsub =plt.bar(xdata,sub,bottom=sur+both,color=(0.0,0.0,1.0), linewidth=1.0, edgecolor='k')
    plt.xticks(xdata,xticks,fontsize=22,minor=False)
    ydata = [0.0e6,0.5e6,1.0e6,1.5e6,2.0e6,2.5e6,3.0e6]
    yticks = ['0','0.5','1.0','1.5','2.0','2.5','3.0']
    plt.yticks(ydata,yticks,fontsize=22)
    plt.gca().yaxis.set_minor_locator(AutoMinorLocator())

    plt.tick_params(axis='x',which='both',bottom=False)
    plt.tick_params(axis='y',which='major',direction='in',length=30)
    plt.tick_params(axis='y', which='minor', direction='in', length=15,left=True,right=True)
    plt.xlabel('Year',fontsize=22)
    plt.ylabel(r'Coverage area (day·km$^2$·10$^6$)',fontsize=22)
    plt.title('Summer bloom Baltic Sea',fontsize=22)
    str_legend = ['Subsurface bloom (Rrs555)','Concurrent bloom','Surface bloom(Rrs670)']
    plt.legend([hsub,hboth,hsur],str_legend,fontsize=22,bbox_to_anchor=(0.35,0.97),edgecolor='black')
    plt.tight_layout()
    plt.savefig(file_out,dpi=300)

def check_sensormask_stats():
    from Class_Flags import Flags_General

    dir_base = '/store3/OC/MULTI/daily_v202311_x'
    file_out = '/store/COP2-OC-TAC/PQD/sensor_mask_multi_chl_med.csv'

    ##LOCAL TEST
    dir_base = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/NOW/OCTAC_QWG'
    file_out = os.path.join(dir_base,'sensor_mask_multi_chl_med.csv')

    work_date = dt(2023,5,1)
    end_date = dt(2025,5,27)
    fw = open(file_out,'w')
    mask_list = None
    flag_g = None
    while work_date<=end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        file_date = os.path.join(dir_base,f'{yyyy}',f'{jjj}',f'X{yyyy}{jjj}-chl-med-hr.nc')

        if os.path.isfile(file_date):
            dataset =  Dataset(file_date)
            smask = dataset.variables['SENSORMASK'][:]

            if mask_list is None:
                comment = dataset.variables['SENSORMASK'].comment
                comment = comment[0:comment.index('.')]
                mask_list = {x.split('=')[0].strip():int(x.split('=')[1].strip()) for x in comment.split(';')}
                first_line = f'Date;{";".join(list(mask_list.keys()))};All'
                fw.write(first_line)
                flag_g = Flags_General(list(mask_list.keys()),list(mask_list.values()),smask.dtype.name)

            line = f'{work_date.strftime("%Y-%m-%d")}'

            smask = smask.compressed()

            for flag in flag_g.flagMeanings:
                mask_here = flag_g.Mask(smask,[flag])
                line = f'{line};{np.count_nonzero(mask_here)}'
            line = f'{line};{np.count_nonzero(smask)}'
            fw.write('\n')
            fw.write(line)

            dataset.close()
        work_date = work_date + timedelta(hours=25)

    fw.close()

def check_coverage():
    dir_base = '/store3/OC/MULTI/daily_v202311_x'
    file_out = '/store/COP2-OC-TAC/PQD/coverage_multi_chl_med.csv'
    file_mask = '/store/COP2-OC-TAC/PQD/MED_Land_hr.nc'

    coverage_dirs = [
        '/store3/OC/MODISA/daily_v202311',
        '/store3/OC/VIIRSJ/daily_v202311',
        '/store3/OC/VIIRS/daily_v202311',
        '/dst04-data1/OC/OLCI/daily_v202311_bc',
        '/dst04-data1/OC/OLCI/daily_v202311_bc'
    ]

    ##LOCAL TEST
    # dir_base = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/NOW/OCTAC_QWG'
    # file_out = os.path.join(dir_base, 'coverage_multi_chl_med.csv')
    # file_mask = os.path.join(dir_base,'MED_Land_hr.nc')
    # coverage_dirs = [dir_base] * 5



    datanameformat = ['A$DATE$-coverage-med.nc',
                      'J$DATE$-coverage-med.nc',
                      'V$DATE$-coverage-med.nc',
                      'Oa$DATE$-coverage-med.nc',
                      'Ob$DATE$-coverage-med.nc']
    sensors = ['AQUA','VIIRS-J','VIIRS-N','OLCI-A','OLCI-B','TOTAL']
    first_line = ['DATE','NDOMAIN']
    for sensor in sensors:
        first_line.append(f'{sensor}_coverage')
        first_line.append(f'{sensor}_pcoverage')

    dmask = Dataset(file_mask)
    mask_array = dmask.variables['Land_Mask'][:]
    ndomain = np.count_nonzero(mask_array == 0)
    dmask.close()


    work_date = dt(2023, 5, 1)
    end_date = dt(2025, 5, 26)
    fw = open(file_out, 'w')
    fw.write(";".join(first_line))

    while work_date <= end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        yyyyjjj = f'{yyyy}{jjj}'
        line = f'{work_date.strftime("%Y-%m-%d")};{ndomain}'
        coverage_end = np.zeros(mask_array.shape)
        for ifile,coverage_dir in enumerate(coverage_dirs):
            file_date = os.path.join(coverage_dir,yyyy,jjj, datanameformat[ifile].replace('$DATE$',yyyyjjj))
            if os.path.isfile(file_date):
                dataset = Dataset(file_date)
                coverage = np.ma.squeeze(dataset.variables['coverage'][:])
                coverage = np.ma.filled(coverage,2)
                coverage=np.ma.masked_where(mask_array==1,coverage)
                ncoverage = np.count_nonzero(coverage==1)
                pcoverage = (ncoverage/ndomain)*100
                line = f'{line};{ncoverage};{pcoverage}'
                dataset.close()
                coverage_end[coverage==1]=coverage_end[coverage==1]+1
            else:
                line = f'{line};0.0;0.0'

        ncoverage = np.count_nonzero(coverage_end>=1)
        pcoverage = (ncoverage / ndomain) * 100
        line = f'{line};{ncoverage};{pcoverage}'
        fw.write('\n')
        fw.write(line)
        print(F'[INFO] Date: {work_date.strftime("%Y-%m-%d")} Coverage total: {ncoverage} %: {pcoverage}')
        work_date = work_date + timedelta(hours=24)

    fw.close()

def main():
    print('[INFO] Started utils')

    if args.mode == 'plot_cyano':
        plot_cyano()

    if args.mode == 'cyano_stats':
        make_cyano_stats()

    if args.mode == 'sensormask_stats':
        #check_sensormask_stats()
        check_coverage()

    if args.mode == 'test':

        folder_olci = '/dst04-data1/OC/OLCI/daily_v202311_bc'
        folder_multi ='/store3/OC/MULTI/daily_v202311_x'
        start_date = dt(2018, 5, 15)
        end_date = dt(2024, 12, 31)
        bands = ['412', '443', '490', '510', '555', '670']
        for band in bands:
            work_date = start_date
            file_band = f'/store/COP2-OC-TAC/INCIDENTS/ISSUE_OLCI_NEGRRS/CSV_TEST/XBand_{band}.csv'
            fw = open(file_band, 'w')
            fw.write('Date;MinValue;NMin')
            while work_date <= end_date:
                yyyy = work_date.strftime('%Y')
                jjj = work_date.strftime('%j')
                #file = os.path.join(folder_olci, yyyy, jjj, f'Ob{yyyy}{jjj}-rrs{band}-bs-hr.nc')
                file = os.path.join(folder_multi, yyyy, jjj, f'X{yyyy}{jjj}-rrs{band}-bs-hr.nc')
                if not os.path.exists(file):
                    work_date = work_date + timedelta(hours=24)
                    continue
                dataset = Dataset(file, 'r')
                array = dataset.variables[f'RRS{band}'][:]
                array = np.ma.masked_invalid(array)
                min_v = np.ma.min(array)
                n_neg = 0
                if min_v < 0:
                    indices = np.where(np.logical_and(array.mask==False,array < 0))
                    n_neg = len(indices[0])
                line = f'{work_date.strftime("%Y-%m-%d")};{np.ma.min(array)};{n_neg}'
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

    if args.mode == 'image_stats':
        arguments = ['config_file', 'output', 'start_date', 'end_date']
        if not check_required_params(arguments):
            return
        if not check_output_file(args.output, 'csv'):
            return
        if not check_input_file(args.config_file,'ini'):
            return
        start_date, end_date = get_dates_from_arg()
        if start_date is None or end_date is None:
            return
        options_stats = get_options_stats()
        if options_stats is None:
            return

        make_stats(options_stats,start_date,end_date,args.output)

    if args.mode == 'compare_images':
        #make_image_comparison()
        make_chl_comparison()
def get_options_stats():
    options = configparser.ConfigParser()
    options.read(args.config_file)
    section = 'IMAGE_STATS'
    if not options.has_section(section):
        return None
    option_list ={
        'input_path':{
            'type':'directory_input'
        },
        'input_path_organization':{
            'type':'timeformat',
            'default': '%Y/%j'
        },
        'dataset_name_file':{
            'type':'str'
        },
        'dataset_name_format_date':{
            'type':'timeformat',
            'default': '%Y%m'
        },
        'var_list':{
            'type': 'strlist'
        },
        'alt_input_path': {
            'type': 'directory_input'
        },
        'alt_input_path_organization': {
            'type': 'timeformat',
            'default': '%Y/%j'
        },
        'alt_dataset_name_file': {
            'type': 'str'
        },
        'alt_dataset_name_format_date': {
            'type': 'timeformat',
            'default': '%Y%m'
        },
        'alt_var_list': {
            'type': 'strlist'
        },
        'correlation_stats_var': {
            'type': 'doublelist'
        },
        'alt_correlation_stats_var': {
            'type': 'doublelist'
        },
        'use_log':{
            'type': 'booleanlist',
            'default': [False]
        },
        'alt_use_log': {
            'type': 'booleanlist',
            'default': [False]
        }
    }

    options_dict = {}
    for opt in option_list:
        pvalues = option_list[opt]['potential_values'] if 'potential_values' in option_list[opt].keys() else None
        defaultv = option_list[opt]['default'] if 'default' in option_list[opt].keys() else None
        options_dict[opt] = get_value_param(options,section,opt,defaultv,option_list[opt]['type'],pvalues)
    option_list_l = list(option_list.keys())
    if options_dict['alt_input_path'] is not None:
        options_no_none = option_list_l[:10]
    else:
        options_no_none = option_list_l[:5]

    valid = True
    for opt in options_no_none:
        if options_dict[opt] is None:
            print(f'[ERROR] Option {opt} in section {section} is missing or not valid')
            valid = False

    if options_dict['correlation_stats_var'] is not None:
        for var_pair in options_dict['correlation_stats_var']:
            if len(var_pair)!=2:
                print(f'[ERROR] Option correlation_stats_var in section {section} should be a list of pairs of variables: var1;var2, var3;var4')
                valid=False
            if var_pair[0] not in options_dict['var_list']:
                print(f'[ERROR] {var_pair[0]} in correlation_stats_var should also be included in var_list')
                valid = False
            if var_pair[1] not in options_dict['var_list']:
                print(f'[ERROR] {var_pair[1]} in correlation_stats_var should also be included in var_list')
                valid = False

    if options_dict['alt_correlation_stats_var'] is not None:
        for var_pair in options_dict['alt_correlation_stats_var']:
            if len(var_pair)!=2:
                print(f'[ERROR] Option alt_correlation_stats_var in section {section} should be a list of pairs of variables: var1;var2, var3;var4')
                valid=False
            if var_pair[0] not in options_dict['var_list']:
                print(f'[ERROR] {var_pair[0]} in alt_correlation_stats_var should also be included in var_list')
                valid = False
            if var_pair[1] not in options_dict['alt_var_list']:
                print(f'[ERROR] {var_pair[1]} in alt_correlation_stats_var should also be included in alt_var_list')
                valid = False

    if not valid:
        return None
    return options_dict




def get_value(options, section, key):
    value = None
    if options.has_option(section, key):
        try:
            value = options[section][key]
        except:
            print(f'[ERROR] Parsing error in section {section} - {key}')
    return value

def get_value_param(options,section,key,default,type,potential_values):
    value = get_value(options,section, key)
    if value is None:
        return default
    if type == 'str':
        if potential_values is None:
            return value.strip()
        else:
            if value.strip().lower() in potential_values:
                return value
            else:
                print(
                    f'[ERROR] [{section}] {value} is not a valid  value for {key}. Valid values: {potential_values} ')
                return default


    if type == 'timeformat':
        value = value.strip()
        if value.upper()=='NONE':
            return 'NONE'
        dtcheck = dt.now()
        try:
            dtcheck.strftime(value)
        except:
            print(
                f'[ERROR] [{section}] {value} is not a valid  time format value for {key}.')
            return default
        return value

    if type == 'file':
        if not os.path.exists(value.strip()):
            return default
        else:
            return value.strip()
    if type.startswith('directory'):
        directory = value.strip()
        if os.path.isdir(directory):
            return directory
        if type=='directory_output':##create the new directory
            try:
                os.mkdir(directory)
                return directory
            except:
                return default
        else:
            return default

    if type == 'int':
        return int(value)
    if type == 'float':
        return float(value)
    if type == 'boolean':
        if value == '1' or value.upper() == 'TRUE':
            return True
        elif value == '0' or value.upper() == 'FALSE':
            return False
        else:
            return True
    if type == 'booleanlist':
        list = []
        for val in value.split(','):
            if val == '1' or val.upper() == 'TRUE':
                list.append(True)
            elif val == '0' or val.upper() == 'FALSE':
                list.append(False)
            else:
                list.append(False)
        return list
    if type == 'strlist':
        # list_str = value.split(',')
        # list = []
        # for vals in list_str:
        #     list.append(vals.strip())
        list = [x.strip() for x in value.split(',')]
        return list
    if type == 'doublelist':
        # list_str = value.split(',')
        list = []
        for vals in value.split(','):
            list.append([x.strip() for x in vals.split(';')])
        return list

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

def check_input_file(file,ext):
    if ext is not None and not file.endswith(ext):
        print(f'[ERROR] {file} should be a {ext} file')
        return False
    if not os.path.isfile(file):
        print(f'[ERROR] {file} in not a valid file or is not available')
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
