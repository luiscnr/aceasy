import os
import configparser
import sys


class POLYMER:
    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'aceasy_config.ini'

        self.polymer_path = None
        if os.path.exists(fconfig):
            options = configparser.ConfigParser()
            options.read(fconfig)
            if options.has_section('POLYMER'):
                if options.has_option('POLYMER', 'polymer_path'):
                    self.polymer_path = options['POLYMER']['polymer_path']

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
            output_name = prod_name[0:-5] + '_POLYMER.nc'
            output_path = os.path.join(output_dir, output_name)
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
        res = run_atm_corr(Level1(prod_path),Level2(filename = output_path,fmt = 'netcdf4'))
        if isinstance(res,Level2_NETCDF) and os.path.exists(output_path):
            if self.verbose:
                print(f'[INFO] Polymer completed. Output file name: {output_name}')
            return True
        else:
            print(f'[ERROR] Polymer NOT completed for product: {prod_name}')
            return False
