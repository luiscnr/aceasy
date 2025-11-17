import argparse,os,json
from qi.qi_comp import QI_PROCESSING
from datetime import datetime as dt
from datetime import timedelta
from netCDF4 import Dataset
import numpy as np

parser = argparse.ArgumentParser(description="QI processing launcher")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
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

def make_pqd_2025():
    dir_base = '/store2/OC/QualityIndex/PQ-D_2025'
    #dir_base = '/mnt/c/DATA/TEMP'

    #MED_MULTI
    region = 'med'
    product_id='009_141'
    dataset_info = '20231101_99999999'
    start_date = dt(2023, 11, 15)
    end_date = dt(2025, 11, 15)
    dir_data = '/store3/OC/MULTI/daily_v202311_x'
    variables = ['RRS412','RRS443','RRS490','RRS510','RRS555','RRS670','CHL','KD490','PP']
    parameters = ['rrs-412','rrs-443','rrs-490','rrs-510','rrs-555','rrs-670','chlorophyll-a','transparency','primary_production']
    metric_suffix = f'SURF-D-NC-SAT-VALID-{region.upper()}'
    metrics = ['RRS-']*6 + ["CHL-","KD-","PP-"]
    resolution = ['1km']*8 + ['4km']
    resolution_str = {
        '1km':'hr',
        '4km':'lr'
    }
    for ivar,variable in enumerate(variables):
        name_json = f'product_quality_nb-observations_{region}_{parameters[ivar]}_{product_id}_{resolution[ivar]}_{dataset_info}.json'
        file_json = os.path.join(dir_base,name_json)
        metric = f'{metrics[ivar]}{metric_suffix}'
        res_dict = start_dict(region,metric)
        print(file_json)
        work_date = start_date
        data = []
        while work_date<=end_date:
            yyyy = work_date.strftime('%Y')
            jjj = work_date.strftime('%j')
            file_nc = os.path.join(dir_data,f'{yyyy}',f'{jjj}',f'X{yyyy}{jjj}-{variable.lower()}-{region}-{resolution_str[resolution[ivar]]}.nc')
            if os.path.exists(file_nc):
                dset = Dataset(file_nc)
                var_array = dset.variables[variable][:]
                value = np.ma.count(var_array)
                dset.close()
            else:
                value = -999.0
            data.append([work_date.strftime('%Y-%m-%d'),value])
            work_date = work_date+timedelta(hours=24)
        res_dict[region]["all_sat_valid"]["data"] = data
        with open(file_json, "w") as f:
            json.dump(res_dict, f, indent=2)



    # res_412 = start_dict("med", "RRS-SURF-D-NC-SAT-VALID-MED")
    # res_443 = start_dict("med", "RRS-SURF-D-NC-SAT-VALID-MED")
    # res_490 = start_dict("med", "RRS-SURF-D-NC-SAT-VALID-MED")
    # res_510 = start_dict("med", "RRS-SURF-D-NC-SAT-VALID-MED")
    # res_555 = start_dict("med", "RRS-SURF-D-NC-SAT-VALID-MED")
    # res_670 = start_dict("med", "RRS-SURF-D-NC-SAT-VALID-MED")

    return True


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
    b = make_pqd_2025()
    if b:
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
