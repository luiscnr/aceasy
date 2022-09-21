import configparser
import math
import os.path

from netCDF4 import Dataset
from balticmlp import balmlpensemble
from balticmlp import polymerflag
from balticmlp import baloutputfile
import numpy as np


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
            'Rw400': 'rrs400',
            'Rw412': 'rrs412_5',
            'Rw443': 'rrs442_5',
            'Rw490': 'rrs490',
            'Rw510': 'rrs510',
            'Rw560': 'rrs560',
            'Rw620': 'rrs620',
            'Rw665': 'rrs665',
            'Rw674': 'rrs673_75',
            'Rw681': 'rrs681_25',
            'Rw709': 'rrs708_75',
            'Rw754': 'rrs753_75',
            'Rw779': 'rrs778_75',
            'Rw865': 'rrs400'
        }

    def check_runac(self):
        # NO IMPLEMENTED
        return True

    def run_process(self, prod_path, output_dir):
        fileout = self.get_file_out(prod_path, output_dir)
        if os.path.exists(fileout):
            print(f'[WARNING] Output file {fileout} already exits. Skipping...')
            return
        if self.verbose:
            print(f'[INFO] Starting chla processing')
        ncpolymer = Dataset(prod_path)
        # flag object
        flag_band = ncpolymer.variables['bitmask']
        flagging = polymerflag.Class_Flags_Polymer(flag_band)

        # image limits
        startX = 0
        startY = 0
        endX = ncpolymer.dimensions['width'].size - 1
        endY = ncpolymer.dimensions['height'].size - 1
        ny = (endY - startY) + 1
        nx = (endX - startX) + 1
        if self.verbose:
            print(f'[INFO] Image dimensions {ny}x{nx}')

        #print(startY,endY,startX,endX)
        latArray,lonArray = self.get_lat_lon_arrays(ncpolymer,startY,endY+1,startX,endX+1)
        #print(latArray.shape)
        #print(lonArray.shape)

        # defining output array
        array_chl = np.empty((ny, nx))
        array_chl[:] = np.NaN

        # defining tile sizes
        tileX = 250
        tileY = 250

        # computing chla for each tile
        for y in range(startY, endY, tileY):
            if self.verbose and (y == 0 or ((y % 100) == 0)):
                print(f'[INFO] Processing line {y}/{ny}')
            for x in range(startX, endX, tileX):
                yini = y
                yend = y + tileY
                if yend > endY:
                    yend = endY +1
                xini = x
                xend = x + tileX
                if xend > endX:
                    xend = endX + 1
                nvalid, valid_mask = self.get_valid_mask(flagging, ncpolymer, yini, yend, xini, xend)

                # chla estimation, only if the tile includes valid pixels
                if nvalid > 0:
                    rrs_data = self.get_valid_rrs(ncpolymer, valid_mask, nvalid, yini, yend, xini, xend)
                    chla_res = self.balmlp.compute_chla_ensemble_3bands(rrs_data)
                    chla_here = np.empty(valid_mask.shape)
                    chla_here[:] = np.NaN
                    chla_here[valid_mask] = chla_res
                    array_chl[yini:yend, xini:xend] = chla_here[:, :]

        if self.verbose:
            print(f'[INFO] Chla processing completed')
            print(f'[INFO] Output file: {fileout}')
        self.create_file(fileout,ncpolymer,array_chl,latArray,lonArray)

    def create_file(self,fileout,ncpolymer,array_chl,latArray,lonArray):

        ncoutput = baloutputfile.BalOutputFile(fileout)
        if not ncoutput.FILE_CREATED:
            print(f'[ERROR] File {fileout} could not be created. Please check permissions')
            return False
        ncoutput.set_global_attributes()
        ny = array_chl.shape[0]
        nx = array_chl.shape[1]
        ncoutput.create_dimensions(ny,nx)
        print(latArray.shape,lonArray.shape,array_chl.shape,'============================')
        ncoutput.create_lat_long_variables(latArray,lonArray)
        ncoutput.create_chla_variable(array_chl)
        ncoutput.close_file()

    def get_file_out(self,prod_path,output_dir):
        name = prod_path.split('/')[-1]
        nameout = name[:-3] + '_MLP.nc'
        if nameout.find('OL_1_EFR')>0:
            nameout = nameout.replace('OL_1_EFR','OL_2_WFR')
        fileout = os.path.join(output_dir,nameout)
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
        rrsdata = np.zeros([nvalid, 5])
        for iband in range(5):
            wlband = wlbands[iband]
            band = np.array(ncpolymer.variables[wlband][yini:yend, xini:xend])
            rrsdata[:, iband] = band[valid_mask]
        rrsdata = rrsdata / np.pi
        return rrsdata

    def get_lat_lon_arrays(self,ncpolymer,yini,yend,xini,xend):
        array_lat = np.array(ncpolymer.variables['latitude'][yini:yend, xini:xend])
        array_lon = np.array(ncpolymer.variables['longitude'][yini:yend, xini:xend])
        return array_lat, array_lon
