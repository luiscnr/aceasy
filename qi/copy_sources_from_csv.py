import argparse
import os
from datetime import datetime as dt
import shutil

parser = argparse.ArgumentParser(description="Copy sources from CSV")
parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-r", "--region", help="Region (MED or BS)")
parser.add_argument('-d', "--date", help="Start date (yyyy-mm-dd)")
parser.add_argument('-s', "--sensor", help="Sensor (S3A or S3B)")
parser.add_argument('-res', "--resolution", help="Resolution (FR or RR)")
parser.add_argument('-o', "--output", help="Output directory")
args = parser.parse_args()


def main():


    print('kk')
    # cmd = 'cd /mnt/c/DATA_LUIS/TEMPORAL && zip -r 2023.zip 2023 && cd -'
    #
    # os.system(cmd)


    # print('[INFO] Copy sources from csv')
    # region = args.region.lower()
    # if region == 'bs':
    #     region = 'blk'
    # outputdir = args.output
    # dirsources = '/dst04-data1/OC/OLCI/sources_baseline_3.01/2023'
    # if args.resolution == 'FR':
    #     file_granules = f'/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/new_granules_{region}'
    # else:
    #     file_granules = f'/store/COP2-OC-TAC/OLCI_FTP_EUMETSAT/new_granules_{region}_rr'
    #
    # datehere = dt.strptime(args.date, '%Y-%m-%d')
    # jjj = datehere.strftime('%j')
    #
    # f1 = open(file_granules, 'r')
    # for line in f1:
    #     lines = line.split(';')
    #     jday = lines[0].strip()
    #     granule = lines[0].strip()
    #     if jday == jjj and granule.startswith(args.sensor):
    #         fgranule = os.path.join(dirsources, jjj, granule)
    #         fgranule_zip = os.path.join(outputdir, f'{granule}.zip')
    #         # fgranule_dest = os.path.join(outputdir, granule)
    #         print(f'Compressing {fgranule} to {fgranule_zip}')
    #         #os.symlink(fgranule, fgranule_dest)
    #         #shutil.make_archive(fgranule_zip,'zip',fgranule)
    #         cmd = f'cd {outputdir} && zip -r {granule}.zip {granule}'


if __name__ == '__main__':
    main()
