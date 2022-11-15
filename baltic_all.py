import os
import subprocess

class BALTIC_ALL():
    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        self.codepath = '/home/gosuser/Processing/OC_PROC_EIS202211/s3olciProcessing/'

    def check_runac(self):
        # NO IMPLEMENTED
        return True

    def run_process(self, prod_path, output_dir):
        ##Resampling
        for name in os.listdir(prod_path):
            if name.endswith('POLYMER_MLP.nc'):
                fname = os.path.join(prod_path, name)
                cmd = f'python {self.codepath}s3olci_bal_reproject_202211.py {fname} -c {self.codepath}s3olci_202211.yaml -od {output_dir} -v'
                self.launch_cmd(cmd)
        # Mosacking
        yearstr = prod_path.split('/')[-2]
        jjjstr = prod_path.split('/')[-1]
        namea = f'Oa{yearstr}{jjjstr}'
        cmd = f'python {self.codepath}s3olcimosaic_202211.py -v {output_dir}/{namea}??????--bal-fr.nc -od {output_dir} -of {namea}--bal-fr.nc'
        self.launch_cmd(cmd)

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

    def launch_cmd(self,cmd):
        if self.verbose:
            print(f'CMD: {cmd}')
        prog = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
        out, err = prog.communicate()
        if err:
            print(err)


