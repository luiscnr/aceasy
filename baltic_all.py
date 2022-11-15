import os


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
                print(cmd)
        # Mosacking
        yearstr = prod_path.split('/')[-2]
        jjjstr = prod_path.split('/')[-1]
        namea = f'Oa{yearstr}{jjjstr}'
        cmd = f'python {self.codepath}s3olcimosaic_202211.py -v {output_dir}/{namea}??????--bal-fr.nc -od {output_dir} -of {namea}--bal-fr.nc'
        print(cmd)

        # Splitting
        cmd = f'python {self.codepath}s3olcisplit_202211.py -v --qi --lowerleft {output_dir}/{namea}--bal-fr.nc -c {self.codepath}s3olci_202211.yaml -od {output_dir}'
        print(cmd)
