import argparse,os
import subprocess
from datetime import datetime as dt
from datetime import timedelta
parser = argparse.ArgumentParser(description="Algorithm launcher")
parser.add_argument('-sd', "--start_date", help="Start date (yyyy-mm-dd)")
parser.add_argument('-ed', "--end_date", help="End date (yyyy-mm-dd")
args = parser.parse_args()

def get_dates_from_arg():

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

def main():
    start_date, end_date = get_dates_from_arg()
    if start_date is None or end_date is None:
        return
    dir_orig = '/dst04-data1/OC/OLCI/daily_v202311_bc'
    dir_destiny = '/store3/OC/OLCI_BAL/dailyolci_202411'
    date_here = start_date
    while date_here<=end_date:
        yyyy = date_here.strftime('%Y')
        jjj = date_here.strftime('%j')
        dir_orig_date = os.path.join(dir_orig,yyyy,jjj)
        dir_destiny_date = os.path.join(dir_destiny,yyyy,jjj)
        if os.path.exists(dir_orig_date):
            if not os.path.exists(dir_destiny_date):
                os.mkdir(dir_destiny_date)
        cmd = f'chmod g+w {dir_destiny_date}'
        print(cmd)
        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
        out, err = prog.communicate()
        if err:
            print(err)
        cmd = f'cp {dir_orig_date}/O{yyyy}{jjj}-rrs*bal-fr.nc {dir_destiny_date}'
        print(cmd)
        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
        out, err = prog.communicate()
        if err:
            print(err)

        date_here = date_here + timedelta(hours=24)


if __name__ == '__main__':
    print('[INFO] Started')
    main()