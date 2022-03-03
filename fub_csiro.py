import configparser
import os
import subprocess


class FUB_CSIRO:
    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'aceasy_config.ini'
        self.gpt_path = None
        self.graph_file = None
        if os.path.exists(fconfig):
            options = configparser.ConfigParser()
            options.read(fconfig)
            if options.has_section('FUB_CSIRO'):
                if options.has_option('FUB_CSIRO', 'gpt_path'):
                    self.gpt_path = options['FUB_CSIRO']['gpt_path']
                if options.has_option('FUB_CSIRO', 'graph_file_default'):
                    self.graph_file = options['FUB_CSIRO']['graph_file_default']

    def check_runac(self):
        if self.gpt_path is None or self.graph_file is None:
            if self.verbose:
                print('[ERROR: FUB_CSIRO class can no be started. GPT and/or graph file paths are not available]')
            return False
        if not os.path.exists(self.gpt_path):
            if self.verbose:
                print('[ERROR: FUB_CSIRO class can no be started. GPT path does not exist]')
            return False
        if not os.path.exists(self.graph_file):
            if self.verbose:
                print('[ERROR: FUB_CSIRO class can no be started. Graph file path does not exist]')
            return False
        return True

    def run_process(self, prod_path, output_dir):
        prod_name = prod_path.split('/')[-1]
        if prod_name.endswith('.SEN3') and os.path.isdir(prod_path):
            output_name = prod_name[0:-5] + '_FUB.nc'
            output_path = os.path.join(output_dir, output_name)
        else:
            print(f'[ERROR] Product {prod_name} is not a correct *.SEN3 directory')
            return False

        if os.path.exists(output_path):
            print(f'[INFO] Output file {output_path} already exists. Skiping...')
            return True
        if self.verbose:
            print(f'[INFO] Input product: {prod_name}')

        cmd = f'{self.gpt_path} {self.graph_file} -f NetCDF4-CF -t {output_path} {prod_path}'
        if self.verbose:
            print(f'[INFO] Starting FUB-CSIRO processing...')
        proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = proc.communicate()
        if self.verbose:
            if outs:
                for l in outs.decode().split('\n'):
                    if l:
                        print(f'[INFO] GPT --> {l}')
            if errs:
                for l in errs.decode().split('\n'):
                    if l:
                        print(f'[WARNING] GPT --> {l}')

        if proc.returncode == 0:
            if self.verbose:
                print(f'[INFO] FUB-CSIRO completed. Output file name: {output_name}')
            return True
        else:
            print(f'[ERROR] FUB-CSIRO NOT completed for product: {prod_name}')
            return False
