import json
import os.path
import configparser
from netCDF4 import Dataset
from datetime import timedelta
from datetime import datetime as dt
import numpy as np
import warnings

warnings.filterwarnings("ignore")


class QI_PROCESSING():

    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'qiprocessing.ini'
        self.options = None
        self.fconfig = fconfig
        if os.path.exists(fconfig):
            try:
                self.options = configparser.ConfigParser()
                self.options.read(fconfig)
            except:
                pass
        self.datasets = None
        self.dirbase = None
        self.sensor = None
        self.domaincoverage = None
        self.products = None
        self.namesoutput = None
        self.first_line = 'Dataset YearMonthDay DomainCoverage TheroticalCoverage ValidPixels -5.5STD-4.5 -4.5STD-3.5 -3.5STD-2.5 -2.5STD-1.5 -1.5STD-0.5 -0.5STD0.5 0.5STD1.5 1.5STD2.5 2.5STD3.5 3.5STD4.5 4.5STD5.5'
        self.info_sensor = {
            'MULTI': {
                'prefix': 'X',
                'resolution': 'hr'
            },
            'OLCI': {
                'prefix': 'O',
                'resolution': 'fr'
            },
            'MULTI_ARC': {
                'prefix': 'C',
                'resolution': '4km'
            }
        }

        self.json_file = None
        self.final_qi_file = None

    def append_qi_files(self, start_date, end_date):
        print(f'[INFO] Started appending qi files....')
        if not self.check_date_qi_file(start_date):
            return
        for idx,nameoutput in enumerate(self.namesoutput):
            dirbase_here = self.dirbase[idx] if len(self.dirbase)==len(self.namesoutput) else self.dirbase[0]
            self.append_qi_files_impl(start_date, end_date, nameoutput,dirbase_here)
        print(f'[INFO] Appending qi files completed')

    def check_date_qi_file(self, start_date):
        file_output = self.final_qi_file
        if os.path.exists(file_output):
            import pandas as pd
            df = pd.read_csv(file_output, sep=' ')
            last_date_str = str(df.iloc[-1].at['YearMonthDay'])
            last_date = dt.strptime(last_date_str, '%Y%m%d')
            if start_date <= last_date:
                start_date_str = start_date.strftime('%Y%m%d')
                print(
                    f'[WARNING] Appeding has not been done as the start date {start_date_str} is before the last date in file {last_date_str}')
                print(
                    f'[WARNING] For making the appending using this data range, please first remove or rename the output file: {file_output}')
                return False
        else:
            fout = open(file_output, 'w')
            fout.write(self.first_line)
            fout.close()
        return True

    def append_qi_files_impl(self, start_date, end_date, nameoutput, dirbase_here):

        file_output = self.final_qi_file
        proc_date = start_date
        fout = open(file_output, 'a')
        while proc_date <= end_date:
            yearstr = proc_date.strftime('%Y')
            jjjstr = proc_date.strftime('%j')
            folder_date = os.path.join(dirbase_here, yearstr, jjjstr)
            file_date = os.path.join(folder_date, nameoutput)
            if os.path.exists(file_date):
                fr = open(file_date, 'r')
                for line in fr:
                    lines = line.strip()
                    if lines.startswith(self.first_line):
                        continue
                    if len(lines) == 0:
                        continue
                    fout.write('\n')
                    fout.write(lines)
                fr.close()
            proc_date = proc_date + timedelta(hours=24)
        fout.close()

    def get_info_date(self, region, date_here):
        # from datetime import datetime as dt
        # date_here = dt(2022,10,5)
        date_here_str = date_here.strftime('%Y%m%d')
        yearstr = date_here.strftime('%Y')
        jjjstr = date_here.strftime('%j')

        #folder_date = os.path.join(self.dirbase, yearstr, jjjstr)
        folder_date_list = [''] * len(self.dirbase)
        for idx, dbase in enumerate(self.dirbase):
            folder_date_list[idx] = os.path.join(dbase, yearstr, jjjstr)
            if not os.path.exists(folder_date_list[idx]):
                print(f'[ERROR] No data directory for date {date_here_str}. Folder date: {folder_date_list[idx]} does not exist')
                return None



        nout = None
        f1 = None

        ##delete the previous files
        for idx,nameoutput in enumerate(self.namesoutput):
            folder_date = folder_date_list[idx] if len(self.datasets) == len(folder_date_list) else folder_date_list[0]
            fout = os.path.join(folder_date, nameoutput)
            if os.path.exists(fout):
                os.remove(fout)
            fouttemp = os.path.join(folder_date, 'product_quality_stats_OCEANCOLOUR_ARC_BGC_L3_MY_009_123.txt')
            if os.path.exists(fouttemp):
                os.remove(fouttemp)

        for idx,dataset in enumerate(self.datasets):
            folder_date = folder_date_list[idx] if len(self.datasets) == len(folder_date_list) else folder_date_list[0]
            var = self.datasets[dataset]['var']
            prefix = self.info_sensor[self.sensor]['prefix']
            res = self.info_sensor[self.sensor]['resolution']

            if self.sensor == 'MULTI_ARC':
                varh = var.lower()
                if var.startswith('RRS'):
                    varh = 'rrs'
                name_file = f'{prefix}{yearstr}{jjjstr}_{varh}-{region.lower()}-{res}.nc'
            else:
                name_file = f'{prefix}{yearstr}{jjjstr}-{var.lower()}-{region.lower()}-{res}.nc'
            file_data = os.path.join(folder_date, name_file)
            if not os.path.exists(file_data):
                print(
                    f'[WARING] {name_file} for variable {var} in date {date_here_str} does not exist. Continue anyway')
            else:
                if self.verbose:
                    print(f'[INFO] Getting info from file {name_file} for variable {var} (date: {date_here_str})')

                res = self.get_info_from_file(file_data, var)

                res_orig = [dataset, date_here_str] + res

                line = ' '.join(res_orig)

                prod = self.datasets[dataset]['prod']

                if prod in self.products:
                    index = self.products.index(prod)
                    name_out = self.namesoutput[index]
                    if nout is None or name_out != nout:
                        if f1 is not None:
                            f1.close()
                        file_out = os.path.join(folder_date, name_out)
                        f1 = open(file_out, 'a')
                        f1.write(self.first_line)
                        nout = name_out

                    f1.write('\n')
                    f1.write(line)

        if f1 is not None:
            f1.close()

    def get_info_from_file(self, file, var):
        try:
            dataset = Dataset(file)
        except:
            print(f'[ERROR] File: {file} is not a valid NetCDF dataset')
            return None

        varqi = 'QI'
        if varqi not in dataset.variables:
            varqi = f'QI_{var}'
        if varqi not in dataset.variables:
            print(f'[ERROR] Variable {var} is not available in file: {file}')
            dataset.close()
            return None

        res = [self.domaincoverage, 100]

        fillvalue = dataset.variables[varqi].getncattr('_FillValue')
        arrayqi = np.array(dataset.variables[varqi])
        arrayqi = arrayqi[arrayqi != fillvalue]
        hist, bin_edges = np.histogram(arrayqi, bins=11, range=(-5.5, 5.5))

        # print((np.sum(hist)/self.domaincoverage)*100)
        nhist = np.sum(hist)

        if nhist == 0:
            pvalid = 0.0
        else:
            pvalid = (nhist / self.domaincoverage) * 100

        res.append(pvalid)

        for h in hist:
            val = (h / nhist) * 100

            res.append(val)

        res_str = [str(x) for x in res]

        dataset.close()
        return res_str

    def update_json_file(self, region, date_here):
        date_here_str = date_here.strftime('%Y-%m-%d')
        yearstr = date_here.strftime('%Y')
        jjjstr = date_here.strftime('%j')

        #folder_date = os.path.join(self.dirbase, yearstr, jjjstr)

        folder_date_list = ['']*len(self.dirbase)
        for idx,dbase in enumerate(self.dirbase):
            folder_date_list[idx] = os.path.join(dbase, yearstr, jjjstr)
            if not os.path.exists(folder_date_list[idx]):
                print(f'[ERROR] No data directory for dir base {dbase} and date {date_here_str}')
                return None

        for idx,dataset in enumerate(self.datasets):
            folder_date = folder_date_list[idx] if len(self.datasets)==len(folder_date_list) else folder_date_list[0]
            if dataset.find('plankton') >= 0:  ##MULTI_ARC
                var = self.datasets[dataset]['var']
                prefix = self.info_sensor[self.sensor]['prefix']
                res = self.info_sensor[self.sensor]['resolution']
                name_file = f'{prefix}{yearstr}{jjjstr}_{var.lower()}-{region.lower()}-{res}.nc'
                file_data = os.path.join(folder_date, name_file)
                if not os.path.exists(file_data):
                    print(
                        f'[WARING] {name_file} for variable {var} in date {date_here_str} does not exist. Continue anyway')
                else:
                    if self.verbose:
                        print(f'[INFO] Getting json info from file {name_file} (date: {date_here_str})')

                    datasetf = Dataset(file_data)
                    varsm = 'CHL'
                    arraysm = np.array(datasetf.variables[varsm])
                    arraysm = arraysm.flatten()
                    indices = np.where(arraysm > 0)
                    ntotal = len(indices[0])
                    datasetf.close()

                    if not os.path.exists(self.json_file):
                        self.start_json_file_onlytotal(region, date_here_str, ntotal)
                    else:
                        with open(self.json_file, 'r') as j:
                            js = json.loads(j.read())

                        rs = region.capitalize()
                        data = js[rs]['all_sat']['data']
                        data.append([date_here_str, ntotal])
                        js[rs]['all_sat']['data'] = data

                        with open(self.json_file, "w", encoding='utf8') as outfile:
                            json.dump(js, outfile, indent=3, ensure_ascii=False)

            if dataset.find('chl') >= 0:
                var = self.datasets[dataset]['var']
                prefix = self.info_sensor[self.sensor]['prefix']
                res = self.info_sensor[self.sensor]['resolution']

                name_file = f'{prefix}{yearstr}{jjjstr}-{var.lower()}-{region.lower()}-{res}.nc'
                file_data = os.path.join(folder_date, name_file)
                if not os.path.exists(file_data):
                    print(
                        f'[WARING] {name_file} for variable {var} in date {date_here_str} does not exist. Continue anyway')
                else:
                    if self.verbose:
                        print(f'[INFO] Getting json info from file {name_file} (date: {date_here_str})')
                    datasetf = Dataset(file_data)
                    varsm = 'SENSORMASK'
                    arraysm = np.array(datasetf.variables[varsm])
                    arraysm = arraysm.flatten()

                    indices = np.where(arraysm > 0)
                    ntotal = len(indices[0])
                    indices = np.where(np.bitwise_or(arraysm == 1, arraysm == 3))
                    ns3a = len(indices[0])
                    indices = np.where(np.bitwise_or(arraysm == 2, arraysm == 3))
                    ns3b = len(indices[0])

                    if not os.path.exists(self.json_file):
                        self.start_json_file(region, date_here_str, ntotal, ns3a, ns3b)
                    else:
                        with open(self.json_file, 'r') as j:
                            js = json.loads(j.read())

                        rs = region.capitalize()
                        data = js[rs]['all_sat']['data']
                        data.append([date_here_str, ntotal])
                        js[rs]['all_sat']['data'] = data
                        data = js[rs]['OLCI_FR_Sentinel-3a']['data']
                        data.append([date_here_str, ns3a])
                        js[rs]['OLCI_FR_Sentinel-3a']['data'] = data
                        data = js[rs]['OLCI_FR_Sentinel-3b']['data']
                        data.append([date_here_str, ns3b])
                        js[rs]['OLCI_FR_Sentinel-3b']['data'] = data

                        with open(self.json_file, "w", encoding='utf8') as outfile:
                            json.dump(js, outfile, indent=3, ensure_ascii=False)

                    datasetf.close()

    def start_json_file_onlytotal(self, region, date_here_str, ntotal):
        rs = region.capitalize()
        js = {
            rs: {
                'all_sat': {
                    'data': [[date_here_str, ntotal]]
                }
            }
        }
        with open(self.json_file, "w", encoding='utf8') as outfile:
            json.dump(js, outfile, indent=3, ensure_ascii=False)

    def start_json_file(self, region, date_here_str, ntotal, ns3a, ns3b):
        rs = region.capitalize()
        # OLCI_FR_Sentinel-3a

        js = {
            rs: {
                'all_sat': {
                    'data': [[date_here_str, ntotal]]
                },
                'OLCI_FR_Sentinel-3a': {
                    'data': [[date_here_str, ns3a]]
                },
                'OLCI_FR_Sentinel-3b': {
                    'data': [[date_here_str, ns3b]]
                }
            }
        }

        with open(self.json_file, "w", encoding='utf8') as outfile:
            json.dump(js, outfile, indent=3, ensure_ascii=False)

    def start_region(self, region):

        if self.options is None:
            print(f'[ERROR] Error reading config file: {self.fconfig}')
            return False

        if not self.options.has_section(region):
            print(f'[ERROR] Region {region} is not defined in the config file: {self.fconfig}')
            return False

        if not self.options.has_option(region, 'BaseArch'):
            print(f'[ERROR] Option BaseArch is not defined in config file: {self.fconfig} for region {region}')
            return False
        self.dirbase = [x.strip() for x in self.options[region]['BaseArch'].split(';')]

        if not self.options.has_option(region, 'products'):
            print(f'[ERROR] Option products is not defined in config file: {self.fconfig} for region {region}')
            return False
        self.products = self.options[region]['products'].strip().split(';')
        self.products = [x.strip() for x in self.products]

        if not self.options.has_option(region, 'namesoutput'):
            print(f'[ERROR] Option namesoutput is not defined in config file: {self.fconfig} for region {region}')
            return False
        self.namesoutput = self.options[region]['namesoutput'].strip().split(';')
        self.namesoutput = [x.strip() for x in self.namesoutput]

        if not self.options.has_option(region, 'mySensor'):
            print(f'[ERROR] Option mySensor is not defined in config file: {self.fconfig} for region {region}')
            return False
        self.sensor = self.options[region]['mySensor'].strip()
        if self.sensor not in self.info_sensor.keys():
            print(
                f'[ERROR] Option mySensor in config file: {self.fconfig} for region {region} should be in the list: {self.info_sensor.keys()}')
            return False

        if not self.options.has_option(region, 'jsonfile'):
            print(f'[ERROR] Option jsonfile is not defined in config file: {self.fconfig} for region {region}')
            return False
        self.json_file = self.options[region]['jsonfile'].strip()

        if self.options.has_option(region, 'qifile'):
            self.final_qi_file = self.options[region]['qifile'].strip()
        else:
            folder_final = os.path.dirname(self.json_file)
            name_final = self.namesoutput[0].split('.')[0]
            try:
                name_final = name_final[0:name_final.rindex('_')]
                self.final_qi_file = os.path.join(folder_final, f'{name_final}.txt')
            except:
                print(
                    f'[ERROR] qi file can not be defined in the config file {self.fconfig}. The first namesoutput should have the format name_param.txt')
                return False

        if not self.options.has_option(region, 'DomainCoverage'):
            print(f'[ERROR] Option DomainCoverage is not defined in config file: {self.fconfig} for region {region}')
            return False
        try:
            self.domaincoverage = int(self.options[region]['DomainCoverage'].strip())
        except:
            print(
                f'[ERROR] Option DomainCoverage in config file: {self.fconfig} for region {region} must be an int number')
            return False

        if not self.options.has_option(region, 'ndatasets'):
            print(f'[ERROR] Option ndatasets is not defined in config file: {self.fconfig} for region {region}')
            return False
        try:
            ndatasets = int(self.options[region]['ndatasets'].strip())
        except:
            print(f'[ERROR] Option ndatasets in config file: {self.fconfig} for region {region} must be an int number')
            return False

        self.datasets = {}

        for idataset in range(ndatasets):
            option = f'dataset.{idataset}'
            if self.options.has_option(region, option):
                vals = self.options[region][option].strip().split(';')
                if len(vals) == 3:
                    self.datasets[vals[0].strip()] = {
                        'prod': vals[1].strip(),
                        'var': vals[2].strip()
                    }
                else:
                    print(
                        f'[ERROR] {option} in region {region} should be have 3 parts (dname,prod,var) separated by ;. Review config file {self.fconfig}')
                    return False
            else:
                print(f'[ERROR] {option} is not available in {region}. Review config file {self.fconfig}')
                return False

        if len(self.datasets) == ndatasets:
            return True
        else:
            return False
