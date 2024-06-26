import os.path
from baltic202411.bal_202411 import BALTIC_202411
from csv_lois import CSV_FILE
from netCDF4 import Dataset
import numpy as np
import BSC_QAA.bsc_qaa_EUMETSAT as bsc_qaa
import json

class BALTIC_202411_PROCESSOR():

    def __init__(self, fconfig, verbose):
        self.verbose = verbose
        if fconfig is None:
            fconfig = 'aceasy_config.ini'
            if not os.path.exists(fconfig): fconfig = os.path.join(os.path.dirname(__file__), 'aceasy_config.ini')
        self.bal_proc = BALTIC_202411()


        # for retrieving RRS from nc products
        self.central_wavelength = {}
        self.central_wl_chla = []
        self.wlbands_chla_polymer = ['Rw443', 'Rw490', 'Rw510', 'Rw560', 'Rw665']
        self.wlbands_chla_cci = ['Rrs_443', 'Rrs_490', 'Rrs_510', 'Rrs_560', 'Rrs_665']

        ##input bands
        # self.wl_chla = [443, 490, 510, 560, 665]
        self.wl_chla = self.bal_proc.input_bands

        self.applyBandShifting = True

        self.product_type = 'cci'

        # ALL VARIABLES
        self.all_var = ['CHL', 'ADG443', 'APH443', 'BBP443', 'KD490', 'MICRO', 'NANO', 'PICO', 'DIATO', 'DINO', 'GREEN',
                        'CRYPTO', 'PROKAR']

        self.processing_var = ['CHL','CDF_AVG','CDF_MLP3B','CDF_MLP4B','CDF_MLP5B','CHL_MLP3B','CHL_MLP4B','CHL_MLP5B']

        # defining tile sizes
        self.tileX = 1500
        self.tileY = 1500

        ##defining attributes
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
        return self.bal_proc.VALID

    def run_process(self, prod_path, output_dir):
        fileout = self.get_file_out(prod_path, output_dir)
        if os.path.exists(fileout):
            print(f'[WARNING] Output file {fileout} already exits. Skipping...')
            return
        if self.verbose:
            print(f'[INFO] Starting water processing')

        ncinput = Dataset(prod_path)

        # info bands
        if self.product_type == 'polymer':
            self.retrieve_info_wlbands_polymer(ncinput)
        elif self.product_type == 'cci':
            self.retrieve_info_wlbands_cci(ncinput)
        if self.applyBandShifting:
            print(f'[INFO] Band shifting is activated:')
            print(f'[INFO]   Input bands: {self.central_wl_chla}')
            print(f'[INFO]   Output bands: {self.wl_chla}')
        else:
            print(f'[INFO] Band shifting is not activated')


        # flag object
        if self.product_type == 'polymer':
            flag_band = ncinput.variables['bitmask']
            # flagging = polymerflag.Class_Flags_Polymer(flag_band) TO IMPLEMENT

        # image limits
        if self.product_type == 'polymer':
            if prod_path.split('/')[-1].lower().find('trim') > 0:
                startX = 0
                startY = 0
                endX = ncinput.dimensions['width'].size - 1
                endY = ncinput.dimensions['height'].size - 1
            else:
                startY, endY, startX, endX = self.get_geo_limits(ncinput)
                if self.verbose:
                    print(f'[INFO] Trimming y->{startY}:{endY} x->{startX}:{endX}')
        elif self.product_type == 'cci':
            startX = 0
            startY = 0
            endX = ncinput.dimensions['longitude'].size - 1
            endY = ncinput.dimensions['latitude'].size - 1

        ny = (endY - startY) + 1
        nx = (endX - startX) + 1
        if self.verbose:
            print(f'[INFO] Image dimensions {ny}x{nx}')



        # defining output arrays
        all_arrays = {}
        for var in self.processing_var:
            array = np.ma.masked_all((ny, nx))
            #array[:] = np.NaN
            all_arrays[var] = array

        # computing chla and other variables for each tile
        for y in range(startY, endY, self.tileY):
            ycheck = y - startY
            if self.verbose and (ycheck == 0 or ((ycheck % self.tileY) == 0)):
                print(f'[INFO] Processing line {ycheck}/{ny}')
            for x in range(startX, endX, self.tileX):
                yini = y
                yend = y + self.tileY
                if yend > endY: yend = endY + 1
                xini = x
                xend = x + self.tileX
                if xend > endX: xend = endX + 1
                if self.product_type=='cci':
                    nvalid, valid_mask = self.get_valid_cci_mask(ncinput, yini, yend, xini, xend)

                if nvalid > 0:
                    input_rrs, iop = self.get_valid_rrs_cci(ncinput,valid_mask,nvalid,yini,yend,xini,xend)
                    res_algorithm = self.bal_proc.compute_ensemble(input_rrs)

                    for key in res_algorithm.keys():
                        if key.upper() in all_arrays.keys():
                            array_here = np.ma.masked_all(valid_mask.shape)
                            array_here[valid_mask==1] = res_algorithm[key]
                            all_arrays[key.upper()][yini - startY:yend - startY, xini - startX:xend - startX] = array_here[:,:]


        if self.verbose:
            print(f'[INFO] Water processing completed')
            print(f'[INFO] Generating output file: {fileout}')




        self.create_file(fileout, ncinput, all_arrays, startY, endY + 1, startX, endX + 1)

    def allow_csv_test(self):
        return True

    def run_from_csv_file(self, path_csv):
        if not self.check_runac():
            print(f'[ERROR] Error starting Baltic 202411 chl-a algorithm')
            return
        if not os.path.exists(path_csv):  ##should be checked before
            return
        csv_lois = CSV_FILE(path_csv)
        spectra = csv_lois.get_rrs_spectra(self.bal_proc.input_bands, self.apply_band_shifting)


        res_all = self.bal_proc.compute_ensemble(spectra)
        # for key in res_all:
        #     print(key, '->', res_all[key].shape)

    def get_file_out(self, prod_path, output_dir):
        name = prod_path.split('/')[-1]
        nameout = name[:-3] + '_BAL202411.nc'
        if nameout.find('OL_1_EFR') > 0:
            nameout = nameout.replace('OL_1_EFR', 'OL_2_WFR')
        fileout = os.path.join(output_dir, nameout)
        return fileout

    def retrieve_info_wlbands_polymer(self, ncpolymer):
        if 'central_wavelength' in ncpolymer.ncattrs():
            cws = ncpolymer.central_wavelength.replace('{', '')
            cws = cws.replace('}', '')
            cws = cws.split(',')
            for cw in cws:
                cwhere = cw.split(':')
                band = f'Rw{cwhere[0].strip()}'
                self.central_wavelength[band] = float(cwhere[1].strip())
            for wlband in self.wlbands_chla_polymer:
                self.central_wl_chla.append(self.central_wavelength[wlband])
        else:
            self.applyBandShifting = False

    def retrieve_info_wlbands_cci(self, nccci):
        for band in self.wlbands_chla_cci:
            if band in nccci.variables:
                self.central_wavelength[band] = float(band.split('_')[1])
                self.central_wl_chla.append(self.central_wavelength[band])
            else:
                self.central_wavelength = {}
                self.central_wl_chla = []

    def get_valid_cci_mask(self,ncinfo,yini,yend,xini,xend):
        mask_bands = ['Rrs_490', 'Rrs_510', 'Rrs_560']
        array_mask = None
        for band in mask_bands:
            array_mask_here = ncinfo.variables[band][0, yini:yend, xini:xend]
            #print('-->',yini,yend,xini,xend,array_mask_here.shape,np.ma.count_masked(array_mask_here))
            if array_mask is None:
                array_mask = np.where(array_mask_here.mask,0,1)
                #print('==>,',array_mask_here.shape,array_mask.shape)
            else:
                array_mask[array_mask_here.mask] = 0
        nvalid = array_mask.sum()

        return nvalid, array_mask

    def get_valid_rrs_cci(self, ncinput, valid_mask, nvalid, yini, yend, xini, xend):

        nbands = len(self.wl_chla)
        rrsdata = np.zeros([nbands, nvalid])
        for iband,wlband in enumerate(self.wlbands_chla_cci):
            band = ncinput.variables[wlband][0,yini:yend, xini:xend]
            band_valid  = band[valid_mask==1]
            rrsdata[iband, :] = band[valid_mask==1].flatten()

        iop = None
        if self.applyBandShifting:
            rrsdata_out, iop = bsc_qaa.bsc_qaa(rrsdata, self.central_wl_chla, self.wl_chla)
            rrsdata = rrsdata_out.transpose()
        else:
            rrsdata = rrsdata.transpose()

        return rrsdata, iop

    def create_file(self, fileout, ncinput, all_arrays, yini, yend, xini, xend):
        if self.verbose:
            print(f'[INFO] Writting output file: {fileout}')

        from baltic_mlp import baloutputfile
        ncoutput = baloutputfile.BalOutputFile(fileout)
        if not ncoutput.FILE_CREATED:
            print(f'[ERROR] File {fileout} could not be created. Please check permissions')
            return False

        if self.product_type=='polymer':
            ncoutput.set_global_attributes(ncinput)
        if self.product_type=='cci':
            ncoutput.set_global_attributes_cci(ncinput,None)

        array_chl = all_arrays['CHL']
        ny = array_chl.shape[0]
        nx = array_chl.shape[1]
        ncoutput.create_dimensions(ny, nx)

        # latitude, longitude
        if self.verbose:
            print(f'[INFO]    Adding latitude/longitude...')
        var_lat_name = 'latitude'
        var_lon_name = 'longitude'
        if len(ncinput.variables[var_lat_name].dimensions)==2:
            array_lat = np.array(ncinput.variables[var_lat_name][yini:yend, xini:xend])
        elif len(ncinput.variables[var_lat_name].dimensions)==1:
            array_lat = np.array(ncinput.variables[var_lat_name][yini:yend])
        if len(ncinput.variables[var_lon_name].dimensions) == 2:
            array_lon = np.array(ncinput.variables[var_lon_name][yini:yend, xini:xend])
        elif len(ncinput.variables[var_lon_name].dimensions) == 1:
            array_lon = np.array(ncinput.variables[var_lon_name][xini:xend])
        ncoutput.create_lat_long_variables(array_lat, array_lon)

        # rrs
        rrsbands = {}
        for band in self.wlbands_chla_cci:
            wl = self.central_wavelength[band]
            name_band = f'RRS{wl:.2f}'
            name_band = name_band.replace('.', '_')
            if name_band.endswith('_00'): name_band = name_band[:-3]
            if name_band.endswith('_50'): name_band = name_band[:-1]
            rrsbands[band] = name_band

        if self.verbose:
            print(f'[INFO]    Adding rrs:')
        for rrsvar in rrsbands.keys():
            namevar = rrsbands[rrsvar]
            if self.verbose:
                print(f'[INFO]     {rrsvar}->{namevar}')
            if not rrsvar in ncinput.variables:
                print(f'[WARNING] Band {rrsvar} is not available in the input file')
                continue
            array = np.ma.array(ncinput.variables[rrsvar][yini:yend, xini:xend])
            array[array.mask] = -999
            array[~array.mask] = array[~array.mask] / np.pi
            wl = self.central_wavelength[rrsvar]
            ncoutput.create_rrs_variable(array, namevar, wl, self.varattr)

        #chl
        if self.verbose:
            print(f'[INFO]    Adding chla...')
        ncoutput.create_var_general(array_chl,'CHL',self.varattr)


        #other variables
        done_variables = ['CHL']
        for ovar in all_arrays.keys():
            if ovar in done_variables:
                continue
            if self.verbose:
                print(f'[INFO]    Adding extra variable {ovar}....')
            array_here = all_arrays[ovar]
            ncoutput.create_var_general(array_here,ovar,None)

        ncoutput.close_file()
        if self.verbose:
            print(f'[INFO]    File {fileout} was created')