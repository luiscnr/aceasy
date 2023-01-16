import os, stat
import subprocess
import configparser
from datetime import datetime as dt


class BALTIC_ALL():
    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'aceasy_config.ini'
        self.codepath = '/home/gosuser/Processing/OC_PROC_EIS202207_NRTNASA_EDS/s3olciProcessing/'  # default
        self.onlyreformat = False
        self.onlycopymonth = False
        self.domerge = False
        self.doresampling = True
        self.domosaic = True
        self.dosplit = True
        self.dos3b = True

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
                    if onlyr.lower() == 'true':
                        self.onlyreformat = True

                if options.has_option('BALALL', 'do_resampling'):
                    onlyr = options['BALALL']['do_resampling']
                    if onlyr.lower() == 'false':
                        self.doresampling = False

                if options.has_option('BALALL', 'do_mosaic'):
                    onlyr = options['BALALL']['do_mosaic']
                    if onlyr.lower() == 'false':
                        self.domosaic = False

                if options.has_option('BALALL', 'do_split'):
                    onlyr = options['BALALL']['do_split']
                    if onlyr.lower() == 'false':
                        self.dosplit = False

                if options.has_option('BALALL', 'do_s3b'):
                    onlyr = options['BALALL']['do_s3b']
                    if onlyr.lower() == 'false':
                        self.dos3b = False

                if options.has_option('BALALL', 'do_merge'):
                    onlyr = options['BALALL']['do_merge']
                    if onlyr.lower() == 'true':
                        self.domerge = True

                if options.has_option('BALALL', 'copy_formonth'):
                    onlyr = options['BALALL']['copy_formonth']
                    if onlyr.lower() == 'true':
                        self.onlycopymonth = True

    def check_runac(self):
        # NO IMPLEMENTED
        return True

    def run_process(self, prod_path, output_dir):

        # Only copy before processing month
        if self.onlycopymonth:
            self.copy_month(prod_path, output_dir)
            return
        # Only reformat
        if self.onlyreformat:
            if self.domerge:
                self.run_merge(prod_path, output_dir)
            self.run_reformat(prod_path, output_dir)
            return
        # Merge
        if self.domerge:
            self.run_merge(prod_path, output_dir)
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
                    expected_ofile = os.path.join(output_dir, expected_oname)
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
            expected_ofile = os.path.join(output_dir, f'{namea}--bal-fr.nc')
            if os.path.exists(expected_ofile):
                print(f'[INFO] Output file {expected_ofile} already exist. Continue...')
            else:
                cmd = f'python {self.codepath}s3olcimosaic_202211.py -v {output_dir}/{namea}??????--bal-fr.nc -od {output_dir} -of {namea}--bal-fr.nc'
                self.launch_cmd(cmd)

            if self.dos3b:
                yearstr = prod_path.split('/')[-2]
                jjjstr = prod_path.split('/')[-1]
                nameb = f'Ob{yearstr}{jjjstr}'
                expected_ofile = os.path.join(output_dir, f'{nameb}--bal-fr.nc')
                if os.path.exists(expected_ofile):
                    print(f'[INFO] Output file {expected_ofile} already exist. Continue...')
                else:
                    cmd = f'python {self.codepath}s3olcimosaic_202211.py -v {output_dir}/{nameb}??????--bal-fr.nc -od {output_dir} -of {nameb}--bal-fr.nc'
                    self.launch_cmd(cmd)

        # Check splitting
        if self.dosplit:
            name_main_end = f'O{yearstr}{jjjstr}--bal-fr.nc'
            prename_end = f'O{yearstr}{jjjstr}-'
            nfiles = 0
            for name in os.listdir(output_dir):
                if name == name_main_end:
                    continue
                if name.startswith(prename_end):
                    nfiles = nfiles + 1
            if nfiles == 28:
                print('[INFO] Files are already available. Completed')
                return

            # Splitting
            cmd = f'python {self.codepath}s3olcisplit_202211.py -v --qi --lowerleft {output_dir}/{namea}--bal-fr.nc -c {self.codepath}s3olci_202211.yaml -od {output_dir}'
            self.launch_cmd(cmd)
            if self.dos3b:
                cmd = f'python {self.codepath}s3olcisplit_202211.py -v --qi --lowerleft {output_dir}/{nameb}--bal-fr.nc -c {self.codepath}s3olci_202211.yaml -od {output_dir}'
                self.launch_cmd(cmd)

            # Changing name
            if not self.dos3b:
                name_main = f'{namea}--bal-fr.nc'
                prename = f'{namea}-'
                for name in os.listdir(output_dir):
                    if name == name_main:
                        continue
                    if name.startswith(prename):
                        name_out = name.replace('Oa', 'O')
                        fin = os.path.join(output_dir, name)
                        fout = os.path.join(output_dir, name_out)
                        cmd = f'mv {fin} {fout}'
                        self.launch_cmd(cmd)

    def run_reformat(self, prod_path, output_dir):
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
        if nfiles < 27:
            print('[INFO] Files are not available. Completed')
            return

        cmd = f'{self.codepath}reformatting_file_cmems2_202211.sh -res FR -m MY -r BAL -f D -p reflectance -path {output_dir}'
        self.launch_cmd(cmd)
        cmd = f'{self.codepath}reformatting_file_cmems2_202211.sh -res FR -m MY -r BAL -f D -p plankton -path {output_dir}'
        self.launch_cmd(cmd)
        cmd = f'{self.codepath}reformatting_file_cmems2_202211.sh -res FR -m MY -r BAL -f D -p optics -path {output_dir}'
        self.launch_cmd(cmd)
        cmd = f'{self.codepath}reformatting_file_cmems2_202211.sh -res FR -m MY -r BAL -f D -p transp -path {output_dir}'
        self.launch_cmd(cmd)

    def run_merge(self, prod_path, output_dir):
        yearstr = prod_path.split('/')[-2]
        jjjstr = prod_path.split('/')[-1]
        filesa, nfilesa = self.check_nfiles('Oa', prod_path, output_dir)
        filesb, nfilesb = self.check_nfiles('Ob', prod_path, output_dir)

        if nfilesa == 28 and nfilesb == 0:
            for filea in filesa:
                filea_o = filea.replace('Oa20', 'O20')
                cmd = f'cp -a {filea} {filea_o}'
                self.launch_cmd(cmd)
            return
        if nfilesb == 28 and nfilesa == 0:
            for fileb in filesb:
                fileb_o = fileb.replace('Ob20', 'O20')
                cmd = f'cp -a {fileb} {fileb_o}'
                self.launch_cmd(cmd)
            return

        if nfilesa < 28:
            print('[INFO] Files S3A are not available. Skyping merge...')
            return
        if nfilesb < 28:
            print('[INFO] Files S3B are not available. Skyping merge...')
            return

        # output dir for making merge
        dirbase = '/DataArchive/OC/OLCI/dailybal202211'
        output_dir_base = self.get_output_directory(dirbase, yearstr, jjjstr)
        for filea in filesa:
            cmd = f'cp -a {filea} {output_dir_base}'
            self.launch_cmd(cmd)
        for fileb in filesb:
            cmd = f'cp -a {fileb} {output_dir_base}'
            self.launch_cmd(cmd)
        # mergin
        datehere = dt.strptime(f'{yearstr}{jjjstr}', '%Y%j')
        dateheres = datehere.strftime('%Y-%m-%d')
        cmd = f'sh {self.codepath}make_merge_olci_202211.sh -d {dateheres} -a bal -r fr -v'
        self.launch_cmd(cmd)
        # copying again
        files, nfiles = self.check_nfiles('O', prod_path, output_dir_base)
        print(nfiles)
        if nfiles < 27:
            print('[INFO] Number of merged files lower than 27. Skypping merge')
            return
        for file in files:
            cmd = f'cp -a {file} {output_dir}'
            self.launch_cmd(cmd)
        print('COMPLETED')

    def get_output_directory(self, output_path, year_str, day_str):
        output_path_year = os.path.join(output_path, year_str)
        if not os.path.exists(output_path_year):
            st = os.stat(output_path)
            os.chmod(output_path, st.st_mode | stat.S_IWOTH | stat.S_IWGRP)
            os.mkdir(output_path_year)

        output_path_jday = os.path.join(output_path_year, day_str)
        if not os.path.exists(output_path_jday):
            st = os.stat(output_path_year)
            os.chmod(output_path_year, st.st_mode | stat.S_IWOTH | stat.S_IWGRP)
            os.mkdir(output_path_jday)
            st = os.stat(output_path_jday)
            os.chmod(output_path_jday, st.st_mode | stat.S_IWOTH | stat.S_IWGRP)

        return output_path_jday

    def copy_month(self, input_path, output_path):
        jjj = input_path.split('/')[-1]
        yyyy = input_path.split('/')[-2]

        namechl = f'O{yyyy}{jjj}-chl-bal-fr.nc'
        filechl = os.path.join(input_path, namechl)
        filechlout = os.path.join(output_path, namechl)
        if os.path.exists(filechl):
            cmd = f'cp -a {filechl} {filechlout}'
            self.launch_cmd(cmd)

        namekd = f'O{yyyy}{jjj}-kd490-bal-fr.nc'
        filekd = os.path.join(input_path, namekd)
        filekdout = os.path.join(output_path, namekd)
        if os.path.exists(filekd):
            cmd = f'cp -a {filekd} {filekdout}'
            self.launch_cmd(cmd)

    def check_nfiles(self, ref, prod_path, output_dir):
        yearstr = prod_path.split('/')[-2]
        jjjstr = prod_path.split('/')[-1]
        name_main_end = f'{ref}{yearstr}{jjjstr}--bal-fr.nc'
        prename_end = f'{ref}{yearstr}{jjjstr}-'
        files = []
        nfiles = 0
        for name in os.listdir(output_dir):
            if name == name_main_end:
                continue
            if name.startswith(prename_end):
                files.append(os.path.join(output_dir, name))
                nfiles = nfiles + 1
        return files, nfiles

    def launch_cmd(self, cmd):
        if self.verbose:
            print(f'CMD: {cmd}')
        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
        out, err = prog.communicate()
        if err:
            print(err)

    def get_sat_time_from_fname(self, fname):
        val_list = fname.split('_')
        sat_time = None
        for v in val_list:
            try:
                sat_time = dt.strptime(v, '%Y%m%dT%H%M%S')
                break
            except ValueError:
                continue
        return sat_time
