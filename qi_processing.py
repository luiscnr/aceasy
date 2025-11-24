import argparse,os,json
from qi.qi_comp import QI_PROCESSING
from datetime import datetime as dt
from datetime import timedelta
from netCDF4 import Dataset
import numpy as np

parser = argparse.ArgumentParser(description="QI processing launcher")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-m", "--mode", help="Mode.", choices=["make_pqd_2025","update_pqd_2025"])
parser.add_argument("-r", "--region", help="Region")
parser.add_argument('-sd', "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument('-ed', "--end_date", help="End date (yyyy-mm-dd")
parser.add_argument('-c', "--config_file", help="Configuration file (Default: qiprocessing.ini)")
parser.add_argument('-nrt', "--nrt_mode", help="NRT mode.", action="store_true")
parser.add_argument('-dt', "--dt_mode", help="DT mode (last two years).", action="store_true")
parser.add_argument('-af',"--append_files", help="Append QI files from start date to end date", action="store_true")
args = parser.parse_args()


def test():
    file_json = '/mnt/c/DATA_LUIS/OCTAC_WORK/QI/COPY_NEW/product_quality_nb-observations_Plankton_chl-sat_OCEANCOLOUR_BLK_BGC_L3_NRT_009_151_19970916-99999999.json'
    # file_json = '/mnt/c/DATA_LUIS/OCTAC_WORK/QI/COPY_NEW/product_quality_nb-observations_Plankton_chl-sat_OCEANCOLOUR_BLK_BGC_L3_NRT_009_151_19970916-99999999.json.bkp'
    import json
    print(os.path.exists(file_json))
    # js = json.loads(file_json)
    # print(type(js))
    with open(file_json, 'r') as j:
        js = json.loads(j.read())

    print(type(js))
    alldata = js['Blacksea']['all_sat']['data']
    for ad in alldata:
        print(ad)

    file_nc = '/mnt/c/DATA_LUIS/OCTAC_WORK/QI/2023/193/X2023193-chl-bs-hr.nc'
    from netCDF4 import Dataset
    import numpy as np
    dataset = Dataset(file_nc)
    varsm = 'SENSORMASK'
    smask = np.array(dataset.variables[varsm])
    fillvalue = dataset.variables[varsm].getncattr('_FillValue')
    arraysm = np.array(dataset.variables[varsm])
    arraysm = arraysm[arraysm != fillvalue]

    dataset.close()

    return True

def make_pqd_from_config_file(config_file):
    from config_reader import ConfigReader
    options = ConfigReader(config_file,None)
    if not options.check_options():
        return
    required_general = {
        'dir_base':{'type':'directory'},
        'start_date':{'type':'str'},
        'end_date':{'type':'str'}
    }
    general_options = options.retrieve_options('GENERAL',required_general,None)
    dir_base = general_options['dir_base']
    if dir_base is None:
        return
    if general_options['start_date'] is None or general_options['end_date'] is None:
        return
    try:
        start_date = dt.strptime(general_options['start_date'],'%Y-%m-%d')
    except:
        print(f'[ERROR] general/start_date value in the config file {general_options["start_date"]} is not in the valid format YYYY-mm-dd')
        return
    try:
        end_date = dt.strptime(general_options['end_date'],'%Y-%m-%d')
    except:
        print(f'[ERROR] general/start_date value in the config file {general_options["start_date"]} is not in the valid format YYYY-mm-dd')
        return

    required_parameters ={
        'region': {'type':'str'},
        'product_id': {'type':'str'},
        'dir_data': {'type':'directory_in'},
        'parameter': {'type':'str'},
        'metric': {'type':'str'},
        'resolution':{'type':'str'},
        'variable':{'type':'str'},
        'name_file_format':{'type':'str'}
    }

    for section in options.get_sections():
        if section=='GENERAL':
            continue
        options_here = options.retrieve_options(section,required_parameters,None)
        for param in required_parameters:
            if options_here[param] is None:
                print(f'[ERROR] Parameter {param} is not available or valid for section {section}')
                return
        region = options_here['region']
        parameter = options_here['parameter']
        product_id = options_here['product_id']
        resolution = options_here['resolution']
        dataset_info = f'{start_date.strftime("%Y%m%d")}_99999999'
        metric_base = options_here['metric']
        name_base = options_here['name_file_format']
        var_here = options_here['variable']
        dir_data = options_here['dir_data']
        if not metric_base.endswith('-'):
            metric_base = f'{metric_base}-'
        metric = f'{metric_base}SURF-D-NC-SAT-VALID-{region.upper()}'
        name_json = f'product_quality_nb-observations_{region}_{parameter}_{product_id}_{resolution}_{dataset_info}.json'
        file_json = os.path.join(dir_base, name_json)
        print(file_json)
        res_dict = start_dict(region, metric)

        work_date = start_date
        data = []

        while work_date <= end_date:
            yyyy = work_date.strftime('%Y')
            jjj = work_date.strftime('%j')
            name_date = name_base.replace('$DATE$',f'{yyyy}{jjj}')
            file_nc = os.path.join(dir_data, f'{yyyy}', f'{jjj}',name_date)
            if os.path.exists(file_nc):
                dset = Dataset(file_nc)
                var_array = dset.variables[var_here][:]
                value = int(np.ma.count(var_array))
                dset.close()
            else:
                value = -999
            data.append([work_date.strftime('%Y-%m-%d'), value])
            work_date = work_date + timedelta(hours=24)
        res_dict[region]["all_sat_valid"]["data"] = data

        with open(file_json, "w") as f:
            json.dump(res_dict, f, indent=2)


def update_pqd_from_config_file(config_file):
    from config_reader import ConfigReader
    options = ConfigReader(config_file, None)
    if not options.check_options():
        return
    required_general = {
        'dir_base': {'type': 'directory'},
        'start_date': {'type': 'str'},
        'ref_days': {'type': 'int'}
    }
    general_options = options.retrieve_options('GENERAL', required_general, None)
    dir_base = general_options['dir_base']
    if dir_base is None:
        return
    if general_options['start_date'] is None or general_options['end_date'] is None:
        return
    try:
        start_date = dt.strptime(general_options['start_date'], '%Y-%m-%d')
    except:
        print(
            f'[ERROR] general/start_date value in the config file {general_options["start_date"]} is not in the valid format YYYY-mm-dd')
        return
    ref_days = general_options['ref_days']
    if ref_days is None:
        return
    if ref_days<=0:
        print(f'[ERROR] ref_days {ref_days} is not valid. This parameter indicates the number of days ago to start the update, and it should be greater than one')
        return

    required_parameters = {
        'region': {'type': 'str'},
        'product_id': {'type': 'str'},
        'dir_data': {'type': 'directory_in'},
        'parameter': {'type': 'str'},
        'metric': {'type': 'str'},
        'resolution': {'type': 'str'},
        'variable': {'type': 'str'},
        'name_file_format': {'type': 'str'}
    }

    for section in options.get_sections():
        if section == 'GENERAL':
            continue
        options_here = options.retrieve_options(section, required_parameters, None)
        for param in required_parameters:
            if options_here[param] is None:
                print(f'[ERROR] Parameter {param} is not available or valid for section {section}')
                return
        region = options_here['region']
        parameter = options_here['parameter']
        product_id = options_here['product_id']
        resolution = options_here['resolution']
        dataset_info = f'{start_date.strftime("%Y%m%d")}_99999999'
        # metric_base = options_here['metric']
        name_base = options_here['name_file_format']
        var_here = options_here['variable']
        dir_data = options_here['dir_data']
        # if not metric_base.endswith('-'):
        #     metric_base = f'{metric_base}-'
        #metric = f'{metric_base}SURF-D-NC-SAT-VALID-{region.upper()}'
        name_json = f'product_quality_nb-observations_{region}_{parameter}_{product_id}_{resolution}_{dataset_info}.json'
        file_json = os.path.join(dir_base, name_json)
        if not os.path.exists(file_json):
            print(f'[WARNING] {file_json} could not be found. Skipping...')
            continue
        with open(file_json) as json_file:
            res_dict = json.load(json_file)
        data = res_dict[region]["all_sat_valid"]["data"]
        work_date = dt.now() - timedelta(days=ref_days)
        n_dates = len(data)
        index_ini = n_dates
        print(f'[INFO] Updating JSON file {json_file}')
        for idx in range(n_dates - 1, -1, -1):
            if data[idx][0] == work_date.strftime('%Y-%m-%d'):
                index_ini = idx
                break
        index_end = index_ini + ref_days

        for idx in range(index_ini, index_end):
            update_value = False
            add_value = False
            if idx < n_dates:
                if data[idx][0] == work_date.strftime('%Y-%m-%d'):
                    update_value = True
                else:
                    print(f'[WARNING] Inconsistency')
            else:
                add_value = True
            yyyy = work_date.strftime('%Y')
            jjj = work_date.strftime('%j')
            name_date = name_base.replace('$DATE$', f'{yyyy}{jjj}')
            file_nc = os.path.join(dir_data, f'{yyyy}', f'{jjj}', name_date)

            if os.path.exists(file_nc):
                dset = Dataset(file_nc)
                var_array = dset.variables[var_here][:]
                value = int(np.ma.count(var_array))
                dset.close()
            else:
                value = -999
            if update_value and value != data[idx][1]:
                data[idx][1] = value
            if add_value:
                data.append([work_date.strftime('%Y-%m-%d'), value])
            work_date = work_date + timedelta(hours=24)

        res_dict[region]["all_sat_valid"]["data"] = data

        with open(file_json, "w") as f:
            json.dump(res_dict, f, indent=2)


def make_pqd_2025():
    dir_base = '/store2/OC/QualityIndex/PQ-D_2025'
    #dir_base = '/mnt/c/DATA/TEMP'

    regions = ['med','blk']
    products_id = {
        'med': ['009_141']*8 + ['009_142'],
        'blk': ['009_151']*8 + ['009_152'],
    }
    start_date = dt(2023, 11, 15)
    end_date = dt(2025, 11, 16)
    dataset_info = f'{start_date.strftime("%Y%m%d")}_99999999'
    dir_data = '/store3/OC/MULTI/daily_v202311_x'
    variables = ['RRS412', 'RRS443', 'RRS490', 'RRS510', 'RRS555', 'RRS670', 'CHL', 'KD490', 'PP']
    parameters = ['rrs-412', 'rrs-443', 'rrs-490', 'rrs-510', 'rrs-555', 'rrs-670', 'chlorophyll-a', 'transparency',
                  'primary_production']
    metrics = ['RRS-'] * 6 + ["CHL-", "KD-", "PP-"]
    resolution = ['1km'] * 8 + ['4km']
    resolution_str = {
        '1km': 'hr',
        '4km': 'lr'
    }
    for region in regions:
        region_file = 'bs' if region=='blk' else region
        products_id_region = products_id[region]
        metric_suffix = f'SURF-D-NC-SAT-VALID-{region.upper()}'

        for ivar,variable in enumerate(variables):
            name_json = f'product_quality_nb-observations_{region}_{parameters[ivar]}_{products_id_region[ivar]}_{resolution[ivar]}_{dataset_info}.json'
            file_json = os.path.join(dir_base,name_json)
            metric = f'{metrics[ivar]}{metric_suffix}'
            res_dict = start_dict(region,metric)
            print(file_json)
            work_date = start_date
            data = []

            while work_date<=end_date:
                yyyy = work_date.strftime('%Y')
                jjj = work_date.strftime('%j')
                file_nc = os.path.join(dir_data,f'{yyyy}',f'{jjj}',f'X{yyyy}{jjj}-{variable.lower()}-{region_file}-{resolution_str[resolution[ivar]]}.nc')
                if os.path.exists(file_nc):
                    dset = Dataset(file_nc)
                    var_array = dset.variables[variable][:]
                    value = int(np.ma.count(var_array))
                    dset.close()
                else:
                    value = -999
                data.append([work_date.strftime('%Y-%m-%d'),value])
                work_date = work_date+timedelta(hours=24)
            res_dict[region]["all_sat_valid"]["data"] = data

            with open(file_json, "w") as f:
                json.dump(res_dict, f, indent=2)

    return True

def update_pqd_2025():
    dir_base = '/store2/OC/QualityIndex/PQ-D_2025'
    #dir_base = '/mnt/c/DATA/TEMP'

    regions = ['med', 'blk']
    products_id = {
        'med': ['009_141'] * 8 + ['009_142'],
        'blk': ['009_151'] * 8 + ['009_152'],
    }
    start_date = dt(2023, 11, 15)
    dataset_info = f'{start_date.strftime("%Y%m%d")}_99999999'
    dir_data = '/store3/OC/MULTI/daily_v202311_x'
    variables = ['RRS412', 'RRS443', 'RRS490', 'RRS510', 'RRS555', 'RRS670', 'CHL', 'KD490', 'PP']
    parameters = ['rrs-412', 'rrs-443', 'rrs-490', 'rrs-510', 'rrs-555', 'rrs-670', 'chlorophyll-a', 'transparency',
                  'primary_production']
    #metrics = ['RRS-'] * 6 + ["CHL-", "KD-", "PP-"]
    resolution = ['1km'] * 8 + ['4km']
    resolution_str = {
        '1km': 'hr',
        '4km': 'lr'
    }
    for region in regions:
        region_file = 'bs' if region == 'blk' else region
        products_id_region = products_id[region]
        #metric_suffix = f'SURF-D-NC-SAT-VALID-{region.upper()}'

        for ivar, variable in enumerate(variables):
            name_json = f'product_quality_nb-observations_{region}_{parameters[ivar]}_{products_id_region[ivar]}_{resolution[ivar]}_{dataset_info}.json'
            file_json = os.path.join(dir_base, name_json)
            if not os.path.exists(file_json):
                print(f'[WARNING] {file_json} could not be found. Skipping...')
                continue
            print(file_json)
            with open(file_json) as json_file:
                res_dict = json.load(json_file)
            data = res_dict[region]["all_sat_valid"]["data"]
            work_date = dt.now()-timedelta(days=8)
            n_dates = len(data)
            index_ini = n_dates
            print('n_dates: ',n_dates)
            for idx in range(n_dates-1,-1,-1):
                if data[idx][0]==work_date.strftime('%Y-%m-%d'):
                    index_ini = idx
                    break
            index_end = index_ini+8
            for idx in range(index_ini,index_end):
                update_value = False
                add_value = False
                if idx<n_dates:
                    if data[idx][0] == work_date.strftime('%Y-%m-%d'):
                        update_value = True
                    else:
                        print(f'[WARNING] Inconsistency')
                else:
                    add_value = True
                yyyy = work_date.strftime('%Y')
                jjj = work_date.strftime('%j')
                file_nc = os.path.join(dir_data, f'{yyyy}', f'{jjj}',f'X{yyyy}{jjj}-{variable.lower()}-{region_file}-{resolution_str[resolution[ivar]]}.nc')
                if os.path.exists(file_nc):
                    dset = Dataset(file_nc)
                    var_array = dset.variables[variable][:]
                    value = int(np.ma.count(var_array))
                    dset.close()
                else:
                    value = -999
                if update_value and value!=data[idx][1]:
                    data[idx][1] = value
                if add_value:
                    data.append([work_date.strftime('%Y-%m-%d'), value])
                work_date = work_date+timedelta(hours=24)

            res_dict[region]["all_sat_valid"]["data"] = data

            with open(file_json, "w") as f:
                json.dump(res_dict, f, indent=2)


def start_dict(region,metric):
    res = {
        region: {
            "all_sat_valid": {
                "metric_name": [metric],
                "data": []
            }
        }
    }
    return res

def main():
    if args.mode is None:
        return
    if args.mode=='make_pqd_2025':
        #make_pqd_2025()
        if not args.config_file:
            print(f'[ERROR] config file -c (--config_file) is required')
            return
        make_pqd_from_config_file(args.config_file)
        return

    if args.mode=='update_pqd_2025':
        update_pqd_2025()
        return
    # b = test()
    # if b:
    #     return
    print('[INFO] Started QI processing')
    fconfig = None
    name_config = 'qiprocessing.ini'
    if args.config_file:
        fconfig = args.config_file
    else:
        if os.path.exists(name_config):
            fconfig = name_config
        elif os.path.exists(os.path.join('aceasy', name_config)):
            fconfig = os.path.join('aceasy', name_config)
    if fconfig is None:
        print(f'[ERROR] Config file is required')
        exit(1)
    if not os.path.exists(fconfig):
        print(f'[ERROR] Config file: {fconfig} does not exist')
        exit(1)

    region = 'BAL'
    if args.region:
        region = args.region

    start_date, end_date = get_start_end_dates()

    if args.nrt_mode:
        qidate = start_date - timedelta(days=1)
        jsondate = start_date - timedelta(days=2)
        print(f'[INFO] TXT Date: {qidate}')
        print(f'[INFO] JSON Date: {jsondate}')
        qiproc = QI_PROCESSING(fconfig, args.verbose)
        if qiproc.start_region(region):
            qiproc.get_info_date(region, qidate)
            qiproc.update_json_file(region, jsondate)
        return

    qiproc = QI_PROCESSING(fconfig, args.verbose)

    if qiproc.start_region(region):
        date_proc = start_date
        while date_proc <= end_date:
            qiproc.get_info_date(region, date_proc)
            qiproc.update_json_file(region, date_proc)
            date_proc = date_proc + timedelta(hours=24)

    if args.dt_mode:
        start_date_abs = end_date - timedelta(days=730)
        qiproc.append_qi_files(start_date_abs,end_date)
    elif args.append_files:
        qiproc.append_qi_files(start_date,end_date)




def get_start_end_dates():
    start_date = (dt.utcnow()).replace(hour=0, minute=0, second=0, microsecond=0)
    if args.start_date:
        try:
            start_date = dt.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] Start date: {args.start_date} should be in format: yyyy-mm-dd')
            exit(1)
    if args.end_date:
        try:
            end_date = dt.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            print(f'[ERROR] End date: {args.end_date} should be in format: yyyy-mm-dd')
            exit(1)
    else:
        end_date = start_date

    if start_date > end_date:
        print(f'[ERROR] End date should be equal or greater than start date')
        exit(1)

    if args.verbose:
        print(f'[INFO] Start date: {start_date} End date: {end_date}')

    return start_date, end_date


if __name__ == '__main__':
    main()
