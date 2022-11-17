import os
import subprocess
import configparser
from datetime import datetime as dt

class BALTIC_ALL():
    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'aceasy_config.ini'
        self.codepath = '/home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/'#default
        self.onlyreformat = False
        self.doresampling = True
        self.domosaic = True
        self.dosplit = True
        if os.path.exists(fconfig):
            options = configparser.ConfigParser()
            options.read(fconfig)
            if options.has_section('BALALL'):
                if options.has_option('BALALL', 'code_path'):
                    self.codepath = options['BALALL']['code_path']
                    if not self.codepath.endswith('/'):
                        self.codepath = f'{self.codepath}/'

                if options.has_option('BALALL', 'only_reformat'):
                    onlyr = options['BALALL']['only_reformat']
                    if onlyr.lower()=='true':
                        self.onlyreformat = True

                if options.has_option('BALALL', 'do_resampling'):
                    onlyr = options['BALALL']['do_resampling']
                    if onlyr.lower()=='false':
                        self.doresampling = False

                if options.has_option('BALALL', 'do_mosaic'):
                    onlyr = options['BALALL']['do_mosaic']
                    if onlyr.lower()=='false':
                        self.domosaic = False

                if options.has_option('BALALL', 'do_split'):
                    onlyr = options['BALALL']['do_split']
                    if onlyr.lower()=='false':
                        self.dosplit = False

    def check_runac(self):
        # NO IMPLEMENTED
        return True

    def run_process(self, prod_path, output_dir):
        if self.onlyreformat:
            self.run_reformat(self,prod_path,output_dir)
            return
        ##Resampling
        if self.doresampling:
            for name in os.listdir(prod_path):
                if name.endswith('POLYMER_MLP.nc'):
                    sat_time = self.get_sat_time_from_fname(name)
                    sat_time_str = sat_time.strftime('%Y%j%H%M%S')
                    if name.startswith('S3A'):
                        expected_oname = f'Oa{sat_time_str}--bal-fr.nc'
                    elif name.startswith('S3B'):
                        expected_oname = f'Ob{sat_time_str}--bal-fr.nc'
                    expected_ofile = os.path.join(output_dir,expected_oname)
                    if os.path.exists(expected_ofile):
                        print(f'[INFO] Output file {expected_ofile} already exist. Continue...')
                        continue
                    fname = os.path.join(prod_path, name)
                    cmd = f'python {self.codepath}s3olci_bal_reproject_202211.py {fname} -c {self.codepath}s3olci_202211.yaml -od {output_dir} -v'
                    self.launch_cmd(cmd)
        # Mosacking
        if self.domosaic:
            yearstr = prod_path.split('/')[-2]
            jjjstr = prod_path.split('/')[-1]
            namea = f'Oa{yearstr}{jjjstr}'
            expected_ofile = os.path.join(output_dir,f'{namea}--bal-fr.nc')
            if os.path.exists(expected_ofile):
                print(f'[INFO] Output file {expected_ofile} already exist. Continue...')
            else:
                cmd = f'python {self.codepath}s3olcimosaic_202211.py -v {output_dir}/{namea}??????--bal-fr.nc -od {output_dir} -of {namea}--bal-fr.nc'
                self.launch_cmd(cmd)

        # Check splitting
        if self.dosplit:
            name_main_end = f'O{yearstr}{jjjstr}--bal-fr.nc'
            prename_end = f'O{yearstr}{jjjstr}-'
            nfiles = 0
            for name in os.listdir(output_dir):
                if name==name_main_end:
                    continue
                if name.startswith(prename_end):
                    nfiles = nfiles +1
            if nfiles==28:
                print('[INFO] Files are already available. Completed')
                return

            # Splitting
            cmd = f'python {self.codepath}s3olcisplit_202211.py -v --qi --lowerleft {output_dir}/{namea}--bal-fr.nc -c {self.codepath}s3olci_202211.yaml -od {output_dir}'
            self.launch_cmd(cmd)

            # Changing name
            name_main = f'{namea}--bal-fr.nc'
            prename = f'{namea}-'
            for name in os.listdir(output_dir):
                if name==name_main:
                    continue
                if name.startswith(prename):
                    name_out = name.replace('Oa','O')
                    fin = os.path.join(output_dir,name)
                    fout = os.path.join(output_dir,name_out)
                    cmd = f'mv {fin} {fout}'
                    self.launch_cmd(cmd)

    def run_reformat(self,prod_path,output_dir):
        yearstr = prod_path.split('/')[-2]
        jjjstr = prod_path.split('/')[-1]
        name_main_end = f'O{yearstr}{jjjstr}--bal-fr.nc'
        prename_end = f'O{yearstr}{jjjstr}-'
        nfiles = 0
        for name in os.listdir(output_dir):
            if name == name_main_end:
                continue
            if name.startswith(prename_end):
                nfiles = nfiles + 1
        if nfiles < 28:
            print('[INFO] Files are not available. Completed')
            return

        cmd = f'/home/gosuser/Processing/OC_PROC_EIS202211/uploaddu/reformatting_file_cmems2_202211.sh -res FR -m NRT -r BAL -f D -p reflectance -path {output_dir}'
        self.launch_cmd(cmd)
        cmd = f'/home/gosuser/Processing/OC_PROC_EIS202211/uploaddu/reformatting_file_cmems2_202211.sh -res FR -m NRT -r BAL -f D -p plankton -path {output_dir}'
        self.launch_cmd(cmd)
        cmd = f'/home/gosuser/Processing/OC_PROC_EIS202211/uploaddu/reformatting_file_cmems2_202211.sh -res FR -m NRT -r BAL -f D -p optics -path {output_dir}'
        self.launch_cmd(cmd)
        cmd = f'/home/gosuser/Processing/OC_PROC_EIS202211/uploaddu/reformatting_file_cmems2_202211.sh -res FR -m NRT -r BAL -f D -p transp -path {output_dir}'
        self.launch_cmd(cmd)

    def launch_cmd(self,cmd):
        if self.verbose:
            print(f'CMD: {cmd}')
        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
        out, err = prog.communicate()
        if err:
            print(err)

    def get_sat_time_from_fname(self,fname):
        val_list = fname.split('_')
        sat_time = None
        for v in val_list:
            try:
                sat_time = dt.strptime(v, '%Y%m%dT%H%M%S')
                break
            except ValueError:
                continue
        return sat_time
