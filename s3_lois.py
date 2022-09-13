import os
from datetime import datetime as dt


class S3Product:
    def __init__(self, path_prod):
        self.path_prod = path_prod
        self.prod_name = path_prod.split('/')[-1]
        if self.prod_name.startswith('S3') and os.path.isdir(self.path_prod):
            lvalues = self.prod_name.split('_')
            dformat = '%Y%m%dT%H%M%S'
            for l in lvalues:
                try:
                    self.date = dt.strptime(l.strip(), dformat)
                    break
                except ValueError:
                    continue
            self.platform = lvalues[0]
            self.instrument = 'OLCI'

    def get_acolite_filename_output(self):
        dates = self.date.strftime('%Y_%m_%d_%H_%M_%S')
        fname = f'{self.platform}_{self.instrument}_{dates}_L2R.nc'
        return fname
