import configparser
import json
import os.path

from netCDF4 import Dataset
from balticmlp import balmlpensemble
from balticmlp import polymerflag
from balticmlp import baloutputfile
import numpy as np
import BSC_QAA.bsc_qaa_EUMETSAT as bsc_qaa


class BALTIC_MLP():
    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'aceasy_config.ini'
        path_par = None
        if os.path.exists(fconfig):
            options = configparser.ConfigParser()
            options.read(fconfig)
            if options.has_section('BALMLP'):
                if options.has_option('BALMLP', 'par_path'):
                    path_par = options['BALMLP']['par_path']

        self.balmlp = balmlpensemble.BalMLP(path_par)

        self.rrsbands = {
            'Rw400': 'RRS400',
            'Rw412': 'RRS412_5',
            'Rw443': 'RRS442_5',
            'Rw490': 'RRS490',
            'Rw510': 'RRS510',
            'Rw560': 'RRS560',
            'Rw620': 'RRS620',
            'Rw665': 'RRS665',
            'Rw674': 'RRS673_75',
            'Rw681': 'RRS681_25',
            'Rw709': 'RRS708_75',
            'Rw754': 'RRS753_75',
            'Rw779': 'RRS778_75',
            'Rw865': 'RRS865'
        }

        # for retrieving RRS
        self.central_wavelength = {}

        # for chla algorithm
        self.wlbands_chla = ['Rw443', 'Rw490', 'Rw510', 'Rw560', 'Rw665']
        self.wl_chla = [443, 490, 510, 560, 665]
        self.central_wl_chla = []
        self.applyBandShifting = True

        # IOP
        self.iop_var = ['ADG443', 'APH443', 'BBP443']

        # PSC
        self.psc_var = ['MICRO', 'NANO', 'PICO']

        # PFT
        self.pft_var = ['DIATO', 'DINO', 'GREEN', 'CRYPTO', 'PROKAR']

        # BAL LIMITS
        self.geo_limits = [53.25, 65.85, 9.25, 30.25]  # latmin,latmax,lonmin,lonmax

        # ALL VARIABLES
        self.all_var = ['CHL', 'ADG443', 'APH443', 'BBP443', 'KD490', 'MICRO', 'NANO', 'PICO', 'DIATO', 'DINO', 'GREEN',
                        'CRYPTO', 'PROKAR']

        self.varattr = None
        sdir = os.path.abspath(os.path.dirname(__file__))
        foptions = os.path.join(sdir, 'varat.json')
        if not os.path.exists(foptions):
            path2info = os.path.join(os.path.dirname(sdir))
            foptions = os.path.join(path2info, 'varat.json')
        if os.path.exists(foptions):
            f = open(foptions, "r")
            self.varattr = json.load(f)
            f.close()

    def check_runac(self):
        # NO IMPLEMENTED
        return True

    def run_process(self, prod_path, output_dir):

        fileout = self.get_file_out(prod_path, output_dir)
        if os.path.exists(fileout):
            print(f'[WARNING] Output file {fileout} already exits. Skipping...')
            return
        if self.verbose:
            print(f'[INFO] Starting water processing')
        ncpolymer = Dataset(prod_path)

        # info bands
        self.retrive_info_wlbands(ncpolymer)

        # flag object
        flag_band = ncpolymer.variables['bitmask']
        flagging = polymerflag.Class_Flags_Polymer(flag_band)

        # image limits
        if prod_path.split('/')[-1].lower().find('trim') > 0:
            startX = 0
            startY = 0
            endX = ncpolymer.dimensions['width'].size - 1
            endY = ncpolymer.dimensions['height'].size - 1
        else:
            startY, endY, startX, endX = self.get_geo_limits(ncpolymer)
            if self.verbose:
                print(f'[INFO] Trimming y->{startY}:{endY} x->{startX}:{endX}')
        ny = (endY - startY) + 1
        nx = (endX - startX) + 1
        if self.verbose:
            print(f'[INFO] Image dimensions {ny}x{nx}')

        # print(startY,endY,startX,endX)
        # latArray,lonArray = self.get_lat_lon_arrays(ncpolymer,startY,endY+1,startX,endX+1)
        # print(latArray.shape)
        # print(lonArray.shape)

        # defining output arrays
        all_arrays = {}
        for var in self.all_var:
            array = np.empty((ny, nx))
            array[:] = np.NaN
            all_arrays[var] = array

        # defining tile sizes
        tileX = 500
        tileY = 500

        # computing chla for each tile
        for y in range(startY, endY, tileY):
            ycheck = y - startY
            if self.verbose and (ycheck == 0 or ((ycheck % tileY) == 0)):
                print(f'[INFO] Processing line {ycheck}/{ny}')
            for x in range(startX, endX, tileX):
                yini = y
                yend = y + tileY
                if yend > endY:
                    yend = endY + 1
                xini = x
                xend = x + tileX
                if xend > endX:
                    xend = endX + 1
                nvalid, valid_mask = self.get_valid_mask(flagging, ncpolymer, yini, yend, xini, xend)

                # chla, iop, psc and kd estimation, only if the tile includes valid pixels
                if nvalid > 0:
                    rrs_data, iop = self.get_valid_rrs(ncpolymer, valid_mask, nvalid, yini, yend, xini, xend)
                    # chl
                    chla_res = self.balmlp.compute_chla_ensemble_3bands(rrs_data)
                    chla_here = np.empty(valid_mask.shape)
                    chla_here[:] = np.NaN
                    chla_here[valid_mask] = chla_res
                    all_arrays['CHL'][yini - startY:yend - startY, xini - startX:xend - startX] = chla_here[:, :]
                    # iop
                    if iop is None:
                        continue
                    for n in range(3):
                        iop_here = np.empty(valid_mask.shape)
                        iop_here[:] = np.NaN
                        iop_here[valid_mask] = iop[:, n]
                        all_arrays[self.iop_var[n]][yini - startY:yend - startY,
                        xini - startX:xend - startX] = iop_here[:, :]
                    # kd, using 490 and 555 bands
                    kd_res = self.compute_kd(rrs_data[:, 1], rrs_data[:, 3])
                    kd_here = np.empty(valid_mask.shape)
                    kd_here[:] = np.NaN
                    kd_here[valid_mask] = kd_res[:]
                    all_arrays['KD490'][yini - startY:yend - startY, xini - startX:xend - startX] = kd_here[:, :]

                    # psc and pft
                    psc, pft = self.compute_psc_pft(chla_res)
                    for var in self.psc_var:
                        psc_here = np.empty(valid_mask.shape)
                        psc_here[:] = np.NaN
                        psc_here[valid_mask] = psc[var][:]
                        all_arrays[var][yini - startY:yend - startY, xini - startX:xend - startX] = psc_here[:, :]
                    for var in self.pft_var:
                        pft_here = np.empty(valid_mask.shape)
                        pft_here[:] = np.NaN
                        pft_here[valid_mask] = pft[var][:]
                        all_arrays[var][yini - startY:yend - startY, xini - startX:xend - startX] = pft_here[:, :]

        if self.verbose:
            print(f'[INFO] Water processing completed')
            print(f'[INFO] Output file: {fileout}')

        self.create_file(fileout, ncpolymer, all_arrays, startY, endY + 1, startX, endX + 1)

    def get_geo_limits(self, ncpolymer):
        array_lat = np.array(ncpolymer.variables['latitude'][:, :])
        array_lon = np.array(ncpolymer.variables['longitude'][:, :])
        width = ncpolymer.dimensions['width'].size
        height = ncpolymer.dimensions['height'].size
        geovalid = np.zeros(array_lat.shape, dtype=np.bool)
        for r in range(height):
            for c in range(width):
                if self.geo_limits[0] <= array_lat[r, c] <= self.geo_limits[1] and self.geo_limits[2] <= array_lon[
                    r, c] <= self.geo_limits[3]:
                    geovalid[r, c] = True
        r, c = np.where(geovalid)
        startY = r.min()
        endY = r.max()
        startX = c.min()
        endX = c.max()

        return startY, endY, startX, endX

    def find_row_column_from_lat_lon(self, lat, lon, lat0, lon0):
        # % closest squared distance
        # % lat and lon are arrays of MxN
        # % lat0 and lon0 is the coordinates of one point
        if self.contain_location(lat, lon, lat0, lon0):
            dist_squared = (lat - lat0) ** 2 + (lon - lon0) ** 2
            r, c = np.unravel_index(np.argmin(dist_squared),
                                    lon.shape)  # index to the closest in the latitude and longitude arrays
        else:
            # print('Warning: Location not contained in the file!!!')
            r = np.nan
            c = np.nan
        return r, c

    def contain_location(self, lat, lon, in_situ_lat, in_situ_lon):
        if lat.min() <= in_situ_lat <= lat.max() and lon.min() <= in_situ_lon <= lon.max():
            contain_flag = 1
        else:
            contain_flag = 0

        return contain_flag

    def retrive_info_wlbands(self, ncpolymer):
        if 'central_wavelength' in ncpolymer.ncattrs():
            cws = ncpolymer.central_wavelength.replace('{', '')
            cws = cws.replace('}', '')
            cws = cws.split(',')
            for cw in cws:
                cwhere = cw.split(':')
                band = f'Rw{cwhere[0].strip()}'
                self.central_wavelength[band] = float(cwhere[1].strip())
            for wlband in self.wlbands_chla:
                self.central_wl_chla.append(self.central_wavelength[wlband])
        else:
            self.applyBandShifting = False

    def create_file(self, fileout, ncpolymer, all_arrays, yini, yend, xini, xend):
        if self.verbose:
            print(f'[INFO] Writting output file: {fileout}')
        ncoutput = baloutputfile.BalOutputFile(fileout)
        if not ncoutput.FILE_CREATED:
            print(f'[ERROR] File {fileout} could not be created. Please check permissions')
            return False

        ncoutput.set_global_attributes(ncpolymer)
        array_chl = all_arrays['CHL']
        ny = array_chl.shape[0]
        nx = array_chl.shape[1]
        ncoutput.create_dimensions(ny, nx)

        # latitude, longitude
        if self.verbose:
            print(f'[INFO]    Adding latitude/longitude...')
        array_lat = np.array(ncpolymer.variables['latitude'][yini:yend, xini:xend])
        array_lon = np.array(ncpolymer.variables['longitude'][yini:yend, xini:xend])
        ncoutput.create_lat_long_variables(array_lat, array_lon)

        # rrs
        if self.verbose:
            print(f'[INFO]    Adding rrs:')
        for rrsvar in self.rrsbands.keys():
            namevar = self.rrsbands[rrsvar]
            if self.verbose:
                print(f'[INFO]     {rrsvar}->{namevar}')
            if not rrsvar in ncpolymer.variables:
                print(f'[WARNING] Band {rrsvar} is not available in the Polymer file')
                continue
            array = np.ma.array(ncpolymer.variables[rrsvar][yini:yend, xini:xend])
            array[array.mask] = -999
            array[~array.mask] = array[~array.mask] / np.pi
            wl = self.central_wavelength[rrsvar]
            ncoutput.create_rrs_variable(array, namevar, wl, self.varattr)

        #chl
        if self.verbose:
            print(f'[INFO]    Adding chla...')
        #ncoutput.create_chla_variable(array_chl)
        ncoutput.create_var_general(array_chl,'CHL',self.varattr)

        #IOP
        if self.verbose:
            print(f'[INFO]    Adding IOPs...')
        for var in self.iop_var:
            if self.verbose:
                print(f'[INFO]     {var}')
            #ncoutput.create_iop_variable(all_arrays[var], var)
            ncoutput.create_var_general(all_arrays[var],var,self.varattr)

        #KD490
        if self.verbose:
            print(f'[INFO]    Adding KD490...')
        #ncoutput.create_kd_variable(all_arrays['KD490'], 'KD490')
        ncoutput.create_var_general(all_arrays['KD490'],'KD490',self.varattr)

        if self.verbose:
            print(f'[INFO]    Adding PSC...')
        for var in self.psc_var:
            if self.verbose:
                print(f'[INFO]     {var}')
            ncoutput.create_var_general(all_arrays[var],var,self.varattr)

        if self.verbose:
            print(f'[INFO]    Adding PFT...')
        for var in self.pft_var:
            if self.verbose:
                print(f'[INFO]     {var}')
            ncoutput.create_var_general(all_arrays[var],var,self.varattr)

        ncoutput.close_file()
        if self.verbose:
            print(f'[INFO]    File {fileout} was created')

    def get_file_out(self, prod_path, output_dir):
        name = prod_path.split('/')[-1]
        nameout = name[:-3] + '_MLP.nc'
        if nameout.find('OL_1_EFR') > 0:
            nameout = nameout.replace('OL_1_EFR', 'OL_2_WFR')
        fileout = os.path.join(output_dir, nameout)
        return fileout

    def get_valid_mask(self, flagging, ncpolymer, yini, yend, xini, xend):
        satellite_flag_band = np.array(ncpolymer.variables['bitmask'][yini:yend, xini:xend])
        flag_mask = flagging.MaskGeneral(satellite_flag_band)
        valid_mask = flag_mask == 0
        nvalid = valid_mask.sum()
        return nvalid, valid_mask

    def get_valid_rrs(self, ncpolymer, valid_mask, nvalid, yini, yend, xini, xend):
        # 443_490_510_555_670
        wlbands = ['Rw443', 'Rw490', 'Rw510', 'Rw560', 'Rw665']
        # rrsdata = np.zeros([nvalid, 5])
        rrsdata = np.zeros([5, nvalid])
        for iband in range(5):
            wlband = wlbands[iband]
            band = np.ma.array(ncpolymer.variables[wlband][yini:yend, xini:xend])
            valid_mask[band.mask] = False
            # rrsdata[:, iband] = band[valid_mask]
            rrsdata[iband, :] = band[valid_mask]
        rrsdata = rrsdata / np.pi

        iop = None

        if self.applyBandShifting:
            # rrsdata_out = rrsdata.transpose()
            rrsdata_out, iop = bsc_qaa.bsc_qaa(rrsdata, self.central_wl_chla, self.wl_chla)
            rrsdata = rrsdata_out.transpose()
        else:
            rrsdata = rrsdata.transpose()

        return rrsdata, iop

    def compute_kd(self, rrs490, rrs555):
        r = np.log10(rrs490 / rrs555)
        a = [0.0166, -0.8515, -1.8263, 1.8714, -2.4414, -1.0690]
        val = a[1] + r * (a[2] + r * (a[3] + r * (a[4] + r * a[5])))
        out = a[0] + np.power(10, val)
        return out
        # IDL CODE
        # r490 = input(*, 0) & r555 = input(*, 1)
        #
        # r = ALOG10(r490(good) / r555(good))
        #
        # a = [0.0166, -0.8515, -1.8263, 1.8714, -2.4414, -1.0690];
        # kd490
        # standard
        # out = a(0) + 10.0 ^ (a(1) + r * (a(2) + r * (a(3) + r * (a(4) + r * a(5)))))

    def compute_psc_pft(self, chl):

        x_log = np.log10(chl)

        psc = {}
        pft = {}

        # PSC - pico
        a = 0.261
        b = 1.870
        psc['PICO'] = a * np.exp(b * x_log)

        # PSC - nano
        a = 0.324
        b = 2.412
        psc['NANO'] = a * np.exp(b * x_log)

        # PSC - micro
        psc['MICRO'] = chl - psc['PICO'] - psc['NANO']

        ##PFT - Dino
        a = 0.050
        b = 2.313
        pft['DINO'] = a * np.exp(b * x_log)

        ##PFT - diato
        pft['DIATO'] = psc['MICRO'] - pft['DINO']

        ## PFT - Green algae & Prochlorophytes
        a = 0.119
        b = 2.181
        pft['GREEN'] = a * np.exp(b * x_log)

        ## PFT - Cryptophytes
        pft['CRYPTO'] = psc['NANO'] - (0.5 * pft['GREEN'])

        ## PFT - Prokaryotes
        pft['PROKAR'] = psc['PICO'] - (0.5 * pft['GREEN'])

        for var in self.psc_var:
            psc[var][chl < 0.13] = -999
            psc[var][chl > 25.5] = -999

        for var in self.pft_var:
            pft[var][chl < 0.13] = -999
            pft[var][chl > 25.5] = -999

        return psc, pft

    # def get_lat_lon_arrays(self,ncpolymer,yini,yend,xini,xend):
    #     array_lat = np.array(ncpolymer.variables['latitude'][yini:yend, xini:xend])
    #     array_lon = np.array(ncpolymer.variables['longitude'][yini:yend, xini:xend])
    #     return array_lat, array_lon
