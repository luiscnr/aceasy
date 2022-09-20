import os
import configparser
import sys


class POLYMER:
    def __init__(self, fconfig, verbose):
        '''
        Polymer options (From Polymer Main)
        - multiprocessing: number of threads to use for processing (int)
        N = 0: single thread (multiprocessing disactivated)
        N != 0: use multiple threads, with
        N < 0: use as many threads as there are CPUs on local machine
        - normalize: select water reflectance normalization
           * no geometry nor wavelength normalization (0)
           * apply normalization of the water reflectance at nadir-nadir (1)
           * apply wavelength normalization for MERIS and OLCI (2)
           * apply both (3)
        '''
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'aceasy_config.ini'

        self.polymer_path = None

        self.extraoptions = {
            'multiprocessing':{
                'value':0,
                'apply':False
            },
            'normalize':{
                'value':3,
                'apply': False
            },
            'bands_rw':{
                'value':[],
                'apply': False
            }
        }
        if os.path.exists(fconfig):
            options = configparser.ConfigParser()
            options.read(fconfig)
            if options.has_section('POLYMER'):
                if options.has_option('POLYMER', 'polymer_path'):
                    self.polymer_path = options['POLYMER']['polymer_path']
                if options.has_option('POLYMER','multiprocessing'):
                    self.extraoptions['multiprocessing']['value'] = int(options['POLYMER'] ['multiprocessing'])
                    self.extraoptions['multiprocessing']['apply'] = True
                if options.has_option('POLYMER','bands_rw'):
                    svalue = options['POLYMER']['bands_rw']
                    self.extraoptions['bands_rw']['value'] = [int(x.strip()) for x in svalue.strip().split(',')]
                    self.extraoptions['bands_rw']['apply'] = True
                if options.has_option('POLYMER','normalize'):
                    self.extraoptions['normalize']['value'] = int(options['POLYMER'] ['normalize'])
                    self.extraoptions['normalize']['apply'] = True



    def check_runac(self):
        if self.polymer_path is None:
            if self.verbose:
                print('[ERROR: POLYMER class can no be started. Polymer path is not available]')
            return False
        if not os.path.exists(self.polymer_path):
            if self.verbose:
                print('[ERROR: POLYMER class can no be started. Polymer path does not exist]')
            return False
        return True

    def run_process(self, prod_path, output_dir):
        prod_name = prod_path.split('/')[-1]
        if prod_name.endswith('.SEN3') and os.path.isdir(prod_path):
            if os.path.isdir(output_dir):
                output_name = prod_name[0:-5] + '_POLYMER.nc'
                output_path = os.path.join(output_dir, output_name)
            else:
                output_name = os.path.basename(output_dir)
                output_path = output_dir
        else:
            print(f'[ERROR] Product {prod_name} is not a correct *.SEN3 directory')
            return False
        if os.path.exists(output_path):
            print(f'[INFO] Output file {output_path} already exists. Skiping...')
            return True
        if self.verbose:
            print(f'[INFO] Input product: {prod_name}')

        sys.path.append(self.polymer_path)
        from polymer.main import run_atm_corr, Level1, Level2
        from polymer.level2_nc import Level2_NETCDF
        params = {}
        for key in self.extraoptions:
            if self.extraoptions[key]['apply']:
                params[key] = self.extraoptions[key]['value']

        #Level2('memory')  # store output in memory
        try:
            res = run_atm_corr(Level1(prod_path),Level2(filename = output_path,fmt = 'netcdf4'),**params)
        except:
            print(f'[ERROR] Polymer NOT completed for product: {prod_name}')
            return False
        #res = run_atm_corr(Level1(prod_path), Level2('memory'), **params)

        if isinstance(res,Level2_NETCDF) and os.path.exists(output_path):
            if self.verbose:
                print(f'[INFO] Polymer completed. Output file name: {output_name}')
            return True
        else:
            print(f'[ERROR] Polymer NOT completed for product: {prod_name}')
            return False
