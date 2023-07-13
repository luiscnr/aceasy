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
# parser.add_argument('-ac', "--atm_correction", help="Atmospheric correction",
#                     choices=["C2RCC", "POLYMER", "FUB_CSIRO", "ACOLITE", "IDEPIX", "BALMLP", "BALALL","QI"], required=True)
args = parser.parse_args()


def main():
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

    qiproc = QI_PROCESSING(fconfig,args.verbose)
    start_date,end_date = get_start_end_dates()

    if qiproc.start_region(region):
        date_proc = start_date
        while date_proc<=end_date:
            qiproc.get_info_date(region,date_proc)
            date_proc = date_proc + timedelta(hours=24)




def get_start_end_dates():
    start_date = (dt.utcnow()-timedelta(hours=24)).replace(hour=0,minute=0,second=0,microsecond=0)
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
        print(f'[INFO] Start date: {args.start_date} End date: {args.end_date}')

    return start_date,end_date

if __name__ == '__main__':
    main()
