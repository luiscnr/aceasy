import os
import configparser
import subprocess
from datetime import datetime as dt
from s3_lois import S3Product

class ACOLITE:

    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'aceasy_config.ini'
        self.acolite_path = None
        self.python_call = None
        if os.path.exists(fconfig):
            options = configparser.ConfigParser()
            options.read(fconfig)
            if options.has_section('ACOLITE'):
                if options.has_option('ACOLITE', 'acolite_path'):
                    self.acolite_path = options['ACOLITE']['acolite_path']
                if options.has_option('ACOLITE', 'python_call'):
                    self.python_call = options['ACOLITE']['python_call']

    def check_runac(self):
        if self.acolite_path is None:
            if self.verbose:
                print('[ERROR]: ACOLITE class can no be started. Acolite path is not available')
            return False
        if self.python_call is None:
            if self.verbose:
                print('[ERROR]: ACOLITE class can no be started. Python call is not available')
            return False
        acolite_py = os.path.join(self.acolite_path,'launch_acolite.py')
        if not os.path.exists(self.acolite_path) or not os.path.exists(acolite_py):
            if self.verbose:
                print('[ERROR]: ACOLITE class can no be started. Acolite path does not exist')
            return False

        cmd = f'{self.python_call} --version'
        proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = proc.communicate()
        if not outs and errs:
            print(f'[ERROR] Python call command: {self.python_call} is not found')
            return False

        return True

    def run_process(self, prod_path, output_dir):
        prod_name = prod_path.split('/')[-1]
        if prod_name.endswith('.SEN3') and os.path.isdir(prod_path):
            output_name = prod_name[0:-5] + '_ACOLITE'
            output_path = os.path.join(output_dir, output_name)
        else:
            print(f'[ERROR] Product {prod_name} is not a correct *.SEN3 directory')
            return False

        sprod = S3Product(prod_path)
        foutput = os.path.join(output_path,sprod.get_acolite_filename_output())

        if os.path.exists(output_path) and os.path.isdir(output_path) and os.path.exists(foutput):
            print(f'[INFO] Output file {output_path} already exists. Skiping...')
            return True
        if self.verbose:
            print(f'[INFO] Input product: {prod_name}')

        file_settings = os.path.join(self.acolite_path, 'acolite_settings.txt')
        lines = ['## ACOLITE settings']
        datenow = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        lines.append(f'## Written at {datenow}')
        lines.append(f'inputfile={prod_path}')
        lines.append(f'output={output_path}')
        lines.append('polygon=')
        lines.append('l2w_parameters=None')
        lines.append('rgb_rhot=True')
        lines.append('rgb_rhos=True')
        lines.append('map_l2w=True')
        with open(file_settings,'w') as fs:
            for line in lines:
                fs.write(line)
                fs.write('\n')
        if self.verbose:
            print(f'[INFO] Settings file: {file_settings}')
        acolite_py = os.path.join(self.acolite_path, 'launch_acolite.py')
        cmd=f'{self.python_call} {acolite_py} --settings {file_settings} --cli'
        print(cmd)
        proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = proc.communicate()
        if self.verbose:
            if outs:
                for l in outs.decode().split('\n'):
                    if l:
                        print(f'[INFO] ACOLITE --> {l}')
            if errs:
                for l in errs.decode().split('\n'):
                    if l:
                        print(f'[WARNING] ACOLITE --> {l}')

        if proc.returncode == 0:
            if self.verbose:
                print(f'[INFO] ACOLITE completed. Output file name: {output_name}')
            return True
        else:
            print(f'[ERROR] ACOLITE  NOT completed for product: {prod_name}')
            return False

