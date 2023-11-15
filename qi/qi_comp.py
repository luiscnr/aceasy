import json
import os.path
import configparser
from netCDF4 import Dataset
import numpy as np
import numpy.ma as ma


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
            }
        }

        self.json_file = None

    def get_info_date(self, region, date_here):
        # from datetime import datetime as dt
        # date_here = dt(2022,10,5)
        date_here_str = date_here.strftime('%Y%m%d')
        yearstr = date_here.strftime('%Y')
        jjjstr = date_here.strftime('%j')
        folder_date = os.path.join(self.dirbase, yearstr, jjjstr)
        if not os.path.exists(folder_date):
            print(f'[ERROR] No data directory for date: {date_here_str}')
            return None

        nout = None
        f1 = None

        ##delete the previous files
        for nameoutput in self.namesoutput:
            fout = os.path.join(folder_date, nameoutput)
            if os.path.exists(fout):
                os.remove(fout)

        for dataset in self.datasets:
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
        if self.verbose:
            print(f'[INFO] NHist: {hist}')
            print(f'[INFO] Domain coverage: {self.domaincoverage}')
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
        folder_date = os.path.join(self.dirbase, yearstr, jjjstr)
        if not os.path.exists(folder_date):
            print(f'[ERROR] No data directory for date: {date_here_str}')
            return None
        for dataset in self.datasets:
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
                    dataset = Dataset(file_data)
                    varsm = 'SENSORMASK'
                    arraysm = np.array(dataset.variables[varsm])
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

                    dataset.close()

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
        self.dirbase = self.options[region]['BaseArch'].strip()

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
