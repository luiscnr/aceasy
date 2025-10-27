from netCDF4 import Dataset
import os
import numpy as np
from datetime import datetime as dt
from datetime import timedelta

class QI_ADD():
    def __init__(self,nc_input,input_path,date_image):
        self.nc_input = nc_input
        if self.nc_input is None and os.path.exists(input_path):
            self.nc_input = Dataset(input_path)

        if date_image is None and 'time' in self.nc_input.variables:
            ts = self.nc_input.variables['time'][0]
            date_image = dt(1981,1,1,0,0,0)+timedelta(seconds=int(ts))

        if date_image is None and 'start_date' in self.nc_input.ncattrs():
            date_image = dt.strptime(self.nc_input.start_date,'%Y-%m-%d')

        self.date_image = date_image

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

        # common attrs
        self.common_attrs = {
            'comment': 'QI=(log(DailyData)-ClimatologyMedianData))/ClimatologyStandardDeviation',
            'type': 'surface',
            'units': '1',
            'missing_value': -999.0,
            'valid_min': -5.0,
            'valid_max': 5.0,
        }



    def close_input_dataset(self):
        self.nc_input.close()

    def add_qi_bal(self,var_list):
        if self.date_image is None:
            print(f'[WARNING] Date is not available. QI band was not added')
            return None
        file_clima = self.clima_paths['bal']['all']
        if not os.path.exists(file_clima):
            print(f'[WARNING] Climatology file is not available. QI band was not added')
            return None
        climafile = Dataset(file_clima)
        lons_in = self.nc_input.variables['lon'][:]
        lats_in = self.nc_input.variables['lat'][::-1]
        lons_cl = climafile.variables['lon'][:]
        lats_cl = climafile.variables['lat'][:]

        first_day = climafile.variables['day'][:].min()  # 40!
        last_day = climafile.variables['day'][:].max()  # 320
        jday = int(self.date_image.strftime('%j'))
        clim_bands = []
        for name_var in climafile.variables:
            if name_var.endswith('_median'):
                clim_bands.append(name_var[0:name_var.find('_median')])

        ny = len(self.nc_input.dimensions['lat'])
        nx = len(self.nc_input.dimensions['lon'])

        # flags to be used if --lowerleft is specified
        flip_latlon = {
            'lat': True if self.nc_input.variables['lat'][0] > self.nc_input.variables['lat'][ny - 1] else False,
            'lon': True if self.nc_input.variables['lon'][0] > self.nc_input.variables['lon'][nx - 1] else False
        }
        lons_in = self.nc_input.variables['lon'][:]
        if flip_latlon['lon']: lons_in = np.fliplr(lons_in)
        lats_in = self.nc_input.variables['lat'][:]
        if flip_latlon['lat']:lats_in = np.flipud(lats_in)
        lons_sub, lats_sub = np.meshgrid(lons_in, lats_in)


        if var_list is None:
            var_list = list(self.nc_input.variables.keys())

        qi_bands_done = []
        print('-->',clim_bands)
        for vname in var_list:
            if not vname.upper() in clim_bands:
                print('=',vname, ' no tiene qui')
                continue
            valid_min = self.nc_input.variables[vname].valid_min if hasattr(self.nc_input.variables[vname], 'valid_min') else None
            valid_max = self.nc_input.variables[vname].valid_max if hasattr(self.nc_input.variables[vname], 'valid_max') else None
            if valid_min is None or valid_max is None:
                continue
            fillvalue = -999.0
            if '_FillValue' in self.nc_input.variables[vname].ncattrs():
                fillvalue = self.nc_input.variables[vname].getncattr('_FillValue')
            name_qi = f'QI_{vname}'
            qi_var = self.nc_input.createVariable(name_qi, 'f4',self.nc_input.variables[vname].dimensions, fill_value=fillvalue, zlib=True, complevel=6)
            if jday < first_day or jday > last_day:
                qi_var[0, :, :] = np.ma.masked_all((ny, nx))
            else:
                cvar_median_str = f'{vname.upper()}_median'
                cvar_std_str = f'{vname.upper()}_median'
                cvar_median = climafile.variables[cvar_median_str][jday - first_day, :, :]
                cvar_std = climafile.variables[cvar_std_str][jday - first_day, :, :]
                cvar_median_i = self.Interp(cvar_median, lons_cl, lats_cl, lons_sub, lats_sub)
                cvar_median_i = np.ma.masked_less(cvar_median_i, valid_min)
                cvar_median_i = np.ma.masked_greater(cvar_median_i, valid_max)
                cvar_std_i = self.Interp(cvar_std, lons_cl, lats_cl, lons_sub, lats_sub)
                data = np.ma.squeeze(self.nc_input.variables[vname][:])
                qi_var[0, :, :] = (data - cvar_median_i) / cvar_std_i

            ##atrs
            for at in self.common_attrs:
                qi_var.setncattr(at,self.common_attrs[at])

            stringa = self.nc_input.variables[vname].long_name if hasattr(self.nc_input.variables[vname], 'long_name') else vname.upper()
            qi_var.setncattr('long_name', f'Quality Index for OLCI {stringa}')
            qi_var.setncattr('climatology_file', os.path.basename(file_clima))
            qi_bands_done.append(name_qi)

        if len(qi_bands_done)==0:
            return None
        else:
            return qi_bands_done

    def Interp(self,datain, xin, yin, xout, yout, interpolation='NearestNeighbour'):
        """
           Interpolates a 2D array onto a new grid (only works for linear grids),
           with the Lat/Lon inputs of the old and new grid. Can perfom nearest
           neighbour interpolation or bilinear interpolation (of order 1)'

           This is an extract from the basemap module (truncated)
        """

        # Mesh Coordinates so that they are both 2D arrays
        # xout,yout = np.meshgrid(xout,yout)

        # compute grid coordinates of output grid.
        delx = xin[1:] - xin[0:-1]
        dely = yin[1:] - yin[0:-1]

        xcoords = (len(xin) - 1) * (xout - xin[0]) / (xin[-1] - xin[0])
        ycoords = (len(yin) - 1) * (yout - yin[0]) / (yin[-1] - yin[0])

        xcoords = np.clip(xcoords, 0, len(xin) - 1)
        ycoords = np.clip(ycoords, 0, len(yin) - 1)

        # Interpolate to output grid using nearest neighbour
        if interpolation == 'NearestNeighbour':
            xcoordsi = np.around(xcoords).astype(np.int32)
            ycoordsi = np.around(ycoords).astype(np.int32)
            dataout = datain[ycoordsi, xcoordsi]

        # Interpolate to output grid using bilinear interpolation.
        elif interpolation == 'Bilinear':
            xi = xcoords.astype(np.int32)
            yi = ycoords.astype(np.int32)
            xip1 = xi + 1
            yip1 = yi + 1
            xip1 = np.clip(xip1, 0, len(xin) - 1)
            yip1 = np.clip(yip1, 0, len(yin) - 1)
            delx = xcoords - xi.astype(np.float32)
            dely = ycoords - yi.astype(np.float32)
            dataout = (1. - delx) * (1. - dely) * datain[yi, xi] + \
                      delx * dely * datain[yip1, xip1] + \
                      (1. - delx) * dely * datain[yip1, xi] + \
                      delx * (1. - dely) * datain[yi, xip1]

        return dataout