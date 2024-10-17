from netCDF4 import Dataset
import os
import numpy as np

class QI_ADD():
    def __init__(self,nc_input,input_path):
        self.nc_input = nc_input
        if self.nc_input is None and os.path.exists(input_path):
            self.nc_input = Dataset(input_path)

        # climatologies ...
        self.clima_paths = {
            'med':{
                'chl':'/store2/data/s_climatology/hr_new',
                'kd':'/store2/data/s_climatology/hr',
                'other':'/DataArchive/OC/SEAWIFS/SEAWIFS_DAILY_CLIMATOLOGY_HR'
            },
            'bs':{
                'chl':'/store2/data/s_climatology/hr_new',
                'other':'/DataArchive/OC/SEAWIFS/SEAWIFS_DAILY_CLIMATOLOGY_HR'
            },
            'bal':{
                'all': '/DataArchive/OC/MODISA/Climatology_BAL/climatology_OC_CNR-Bal-v01.nc'
            }

        }
        if not os.path.exists(self.clima_paths['bal']['all']):  ##LOCAT TEST
            self.clima_paths['bal']['all'] = '/mnt/c/DATA_LUIS/OCTAC_WORK/POLYMER_PROCESSING/climatology_OC_CNR-Bal-v01.nc'



    def add_qi_bal(self):
        file_clima = self.clima_paths['bal']['all']
        if not os.path.exists(file_clima):
            print(f'[WARNING] Climatology file is not available. QI band was not added')
        climafile = Dataset(file_clima)
        lons_in = self.nc_input.variables['lon'][:]
        lats_in = self.nc_input.variables['lat'][::-1]
        lons_cl = climafile.variables['lon'][:]
        lats_cl = climafile.variables['lat'][:]
        lons_sub, lats_sub = np.meshgrid(lons_in, lats_in)

        clim_bands = []
        for name_var in climafile.variables:
            if name_var.endswith('_median'):
                clim_bands.append(name_var[0:name_var.find('_median')])

        ny = len(self.nc_input.dimensions['lat'])
        nx = len(self.nc_input_dimensions['lon'])

        # flags to be used if --lowerleft is specified
        flip_latlon = {
            'lat': True if self.nc_input.variables['lat'][0] > self.nc_input.variables['lat'][ny - 1] else False,
            'lon': True if self.nc_input.variables['lon'][0] > self.nc_input.variables['lon'][nx - 1] else False
        }
        lons_in = self.nc_input.variables['lon'][:]
        if flip_latlon['lon']: lons_in = np.flipud(lons_in)
        lats_in = self.nc_input.variables['lat'][:]
        if flip_latlon['lat']:lats_in = np.flipud(lats_in)

        var_list = list(self.nc_input.variables.keys())
        for vname in var_list:
            if not vname in clim_bands:
                continue
            fillvalue = -999.0
            if '_FillValue' in self.nc_input.variables[vname].ncattrs():
                fillvalue = self.nc_input.variables[vname].getncattr('_FillValue')
            qi_var = self.nc_input.createVariable('QI', 'f4',self.nc_input.dimensions, fill_value=fillvalue, zlib=True, complevel=6)
