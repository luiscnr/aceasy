import os
import configparser
import sys, stat


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
        self.name_ac = 'POLYMER'
        self.version = 4.14

        self.product_type = 's3_olci'

        if fconfig is None:
            fconfig = 'aceasy_config.ini'

        self.polymer_path = None
        self.ancillary_path = None

        self.extraoptions = {
            'multiprocessing': {
                'value': 0,
                'apply': False
            },
            'normalize': {
                'value': 3,
                'apply': False
            },
            'bands_rw': {
                'value': [],
                'apply': False
            }
        }

        if os.path.exists(fconfig):
            options = configparser.ConfigParser()
            options.read(fconfig)
            if options.has_section('POLYMER'):
                if options.has_option('POLYMER', 'polymer_path'):
                    self.polymer_path = options['POLYMER']['polymer_path']
                    if not self.polymer_path.strip() == 'PWD':
                        sys.path.append(self.polymer_path.strip())
                    if self.verbose:
                        print(f'[INFO] Polymer path: {self.polymer_path}')
                if options.has_option('POLYMER', 'version'):
                    self.version = float(options['POLYMER']['version'])
                    print(f'[INFO] Polymer version: {self.version}')
                if options.has_option('POLYMER', 'ancillary_path'):
                    self.ancillary_path = options['POLYMER']['ancillary_path']
                if options.has_option('POLYMER', 'multiprocessing'):
                    self.extraoptions['multiprocessing']['value'] = int(options['POLYMER']['multiprocessing'])
                    self.extraoptions['multiprocessing']['apply'] = True
                if options.has_option('POLYMER', 'bands_rw'):
                    svalue = options['POLYMER']['bands_rw']
                    self.extraoptions['bands_rw']['value'] = [int(x.strip()) for x in svalue.strip().split(',')]
                    self.extraoptions['bands_rw']['apply'] = True
                if options.has_option('POLYMER', 'normalize'):
                    self.extraoptions['normalize']['value'] = int(options['POLYMER']['normalize'])
                    self.extraoptions['normalize']['apply'] = True

    def allow_csv_test(self):
        return False

    def check_runac(self):
        if self.polymer_path is None:
            if self.verbose:
                print('[ERROR] POLYMER class can no be started. Polymer path is not available')
            return False
        if not self.polymer_path == 'PWD' and not os.path.exists(self.polymer_path):
            if self.verbose:
                print('[ERROR] POLYMER class can no be started. Polymer path does not exist')
            return False
        return True

    def check_product_path(self, prod_path, prod_name, output_dir):
        if self.verbose:
            print(f'[INFO] Checking {prod_path} for product type {self.product_type}')
        if self.product_type == 's3_olci':
            if prod_name.endswith('.SEN3') and os.path.isdir(prod_path):
                if os.path.isdir(output_dir):
                    output_name = prod_name[0:-5] + '_POLYMER.nc'
                    output_path = os.path.join(output_dir, output_name)
                else:
                    output_name = os.path.basename(output_dir)
                    output_path = output_dir
                return output_path, output_name
            else:
                print(f'[ERROR] Product {prod_name} is not a correct *.SEN3 directory')
                return None, None

        if self.product_type == 's2_msi':
            # if self.version==5.0:
            #     print(f'[ERROR] MSI products are not implemented for version 5')
            #     return None,None
            self.version = '5.0'
            if prod_name.startswith('S2') and prod_name.endswith('.SAFE') and os.path.isdir(prod_path):
                if os.path.isdir(output_dir):
                    output_name = prod_name[0:-5] + '_POLYMER.nc'
                    output_path = os.path.join(output_dir, output_name)
                else:
                    output_name = os.path.basename(output_dir)
                    output_path = output_dir
                return output_path, output_name
            else:
                print(f'[ERROR] Product {prod_name} is not a correct *.SAFE directory')
                return None, None

        if self.product_type == 'prisma':
            if self.version==5.0:
                print(f'[ERROR] PRISMA products are not implemented for version 5')
                return None,None
            if prod_name.endswith('.he5'):
                if os.path.isdir(output_dir):
                    output_name = prod_name[0:-4] + '_POLYMER.nc'
                    output_path = os.path.join(output_dir, output_name)
                else:
                    output_name = os.path.basename(output_dir)
                    output_path = output_dir
                return output_path, output_name
            else:
                print(f'[ERROR] Product {prod_name} is not a correct *.h5 format')
                return None, None

        return None,None

    def run_process(self, prod_path, output_dir):
        prod_name = os.path.basename(prod_path)
        output_path, output_name = self.check_product_path(prod_path, prod_name, output_dir)
        if output_path is None:##file error
            return False
        if os.path.exists(output_path):
            print(f'[INFO] Output file {output_path} already exists. Skipping...')
            return True
        if self.verbose:
            print(f'[INFO] Input product: {prod_name}')
            print(f'[INFO] Ouput path: {output_path}')

        if self.version == 4.14:
            from polymer.main import run_atm_corr, Level1, Level2
            from polymer.level2_nc import Level2_NETCDF


        if self.version == 4.17:
            from polymer.main import run_atm_corr
            from polymer.level1 import Level1
            from polymer.level1_prisma import Level1_PRISMA
            from polymer.level2 import Level2
            from polymer.level2_nc import Level2_NETCDF
            from polymer.ancillary import Ancillary_NASA

        if self.version == 4.0:
            ##version 4
            from polymer.main import run_atm_corr
            from polymer.level1 import Level1
            from polymer.level2 import Level2
            from polymer.level2_nc import Level2_NETCDF

        if self.version == 5.0:
            from polymer.main_v5 import run_polymer
            # from polymer.level1 import Level1
            # from polymer.level1_prisma import Level1_PRISMA
            # from polymer.level2 import Level2
            # from polymer.level2_nc import Level2_NETCDF
            # from polymer.ancillary import Ancillary_NASA



        params = {}
        for key in self.extraoptions:
            if self.extraoptions[key]['apply']:
                params[key] = self.extraoptions[key]['value']

        if self.product_type == 'prisma':
            if self.version!=4.17:##it shouldn't arrive here, type file is checked using check_product_path
                return False

            # ancillary_folder = os.path.join(os.path.dirname(prod_path),'ANCILLARY')
            # if not os.path.isdir(ancillary_folder):
            #     try:
            #         os.mkdir(ancillary_folder)
            #         os.chmod(ancillary_folder, 0o777)
            #
            #     except:
            #         print(f'[ERROR] Ancillary folder {ancillary_folder} could not be created, please review permissions')
            #         return False
            #
            # meteo_folder = os.path.join(ancillary_folder, 'METEO')
            # if not os.path.isdir(meteo_folder):
            #     try:
            #         os.mkdir(meteo_folder)
            #         os.chmod(meteo_folder, 0o777)
            #
            #     except:
            #         print(f'[ERROR] Meteo folder {meteo_folder} could not be created, please review permissions')
            #         return False



            try:
                ancillary_obj = Ancillary_NASA(directory=self.ancillary_path) if self.ancillary_path is not None else None
                res = run_atm_corr(Level1_PRISMA(prod_path,ancillary=ancillary_obj), Level2(filename=output_path, fmt='netcdf4'), **params)
            except Exception as error:
                print(f'[ERROR] Polymer WAS NOT completed for product: {prod_name}. ')
                print(f'[ERROR] {error}')
                return False
        else:

            if self.version==5:
                try:
                    res = run_polymer(prod_path,file_out=output_path,if_exists="overwrite")
                except Exception as error:
                    print(f'[ERROR] Polymer WAS NOT completed for product: {prod_name}. ')
                    print(f'[ERROR] {error}')
                    return False

            else:
                try:
                    res = run_atm_corr(Level1(prod_path), Level2(filename=output_path, fmt='netcdf4'), **params)
                except Exception as error:
                    print(f'[ERROR] Polymer WAS NOT completed for product: {prod_name}. ')
                    print(f'[ERROR] {error}')
                    return False
        # res = run_atm_corr(Level1(prod_path), Level2('memory'), **params)

        if self.version==5:
            from pathlib import Path
            if isinstance(res,Path) and os.path.exists(output_path):
                if self.verbose:
                    print(f'[INFO] Polymer completed. Output file name: {output_name}')
                return True
            else:
                print(f'[ERROR] Polymer NOT completed for product: {prod_name}')
                return False
        if isinstance(res, Level2_NETCDF) and os.path.exists(output_path):
            if self.verbose:
                print(f'[INFO] Polymer completed. Output file name: {output_name}')
            return True
        else:
            print(f'[ERROR] Polymer NOT completed for product: {prod_name}')
            return False
