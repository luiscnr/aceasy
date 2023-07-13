import argparse
import os
from qi.qi_comp import QI_PROCESSING
from datetime import datetime as dt
from datetime import timedelta
parser = argparse.ArgumentParser(description="QI processing launcher")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-r", "--region", help="Region")
# parser.add_argument('-i', "--inputpath", help="Input directory")
# parser.add_argument('-o', "--outputpath", help="Output directory", required=True)
# parser.add_argument('-tp', "--temp_path", help="Temporary directory")
parser.add_argument('-sd', "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument('-ed', "--end_date", help="End date (yyyy-mm-dd")
# parser.add_argument('-wce', "--wce", help="Wild card expression")
parser.add_argument('-c', "--config_file", help="Configuration file (Default: qiprocessing.ini)")
parser.add_argument('-nrt',"--nrt_mode",help="NRT mode.", action="store_true")
# parser.add_argument('-ac', "--atm_correction", help="Atmospheric correction",
#                     choices=["C2RCC", "POLYMER", "FUB_CSIRO", "ACOLITE", "IDEPIX", "BALMLP", "BALALL","QI"], required=True)
args = parser.parse_args()

def test():
    file_json = '/mnt/c/DATA_LUIS/OCTAC_WORK/QI/COPY_NEW/product_quality_nb-observations_Plankton_chl-sat_OCEANCOLOUR_BLK_BGC_L3_NRT_009_151_19970916-99999999.json'
    #file_json = '/mnt/c/DATA_LUIS/OCTAC_WORK/QI/COPY_NEW/product_quality_nb-observations_Plankton_chl-sat_OCEANCOLOUR_BLK_BGC_L3_NRT_009_151_19970916-99999999.json.bkp'
    import json
    print(os.path.exists(file_json))
    #js = json.loads(file_json)
    #print(type(js))
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

def main():
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
        qidate = start_date-timedelta(days=1)
        jsondate = start_date-timedelta(days=2)
        print(f'[INFO] TXT Date: {qidate}')
        print(f'[INFO] JSON Date: {jsondate}')
        qiproc = QI_PROCESSING(fconfig, args.verbose)
        if qiproc.start_region(region):
            qiproc.get_info_date(region, qidate)
            qiproc.update_json_file(region, jsondate)
        return


    qiproc = QI_PROCESSING(fconfig,args.verbose)


    if qiproc.start_region(region):
        date_proc = start_date
        while date_proc<=end_date:
            qiproc.get_info_date(region,date_proc)
            qiproc.update_json_file(region,date_proc)
            date_proc = date_proc + timedelta(hours=24)




def get_start_end_dates():
    start_date = (dt.utcnow()).replace(hour=0,minute=0,second=0,microsecond=0)
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

    return start_date,end_date

if __name__ == '__main__':
    main()
