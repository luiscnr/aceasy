import datetime
import os.path

import pytz

from baltic202411.bal_202411 import BALTIC_202411
from baltic_mlp import BALTIC_MLP
from csv_lois import CSV_FILE
from netCDF4 import Dataset
import numpy as np
import BSC_QAA.bsc_qaa_EUMETSAT as bsc_qaa
from balticmlp import polymerflag
import json

from split_202411 import Splitter


class BALTIC_202411_PROCESSOR():

    def __init__(self, fconfig, verbose):

        self.verbose = verbose
        self.bal_proc = BALTIC_202411()
        self.bal_proc_old = BALTIC_MLP(fconfig, verbose)

        # for retrieving RRS from nc products
        self.only_rrs = False
        self.central_wavelength = {}
        self.central_wl_chla = []
        self.wlbands_chla_polymer = ['Rw443', 'Rw490', 'Rw510', 'Rw560', 'Rw665']
        self.wlbands_chla_cci = ['Rrs_443', 'Rrs_490', 'Rrs_510', 'Rrs_560', 'Rrs_665']
        self.wlbands_chla_olci_l3 = ['rrs442_5', 'rrs490', 'rrs510', 'rrs560', 'rrs665']

        self.rrsbands_polymer = self.bal_proc_old.rrsbands
        self.geo_limits = self.bal_proc_old.geo_limits

        ##input bands
        # 443, 490, 510, 555, 670
        self.wl_chla = self.bal_proc.input_bands
        self.th_cyano_555 = 4.25e-3
        self.th_cyano_670 = 1.22e-3

        self.applyBandShifting = True

        self.product_type = 'cci'

        # ALL VARIABLES
        # self.all_var = ['CHL', 'ADG443', 'APH443', 'BBP443', 'KD490', 'MICRO', 'NANO', 'PICO', 'DIATO', 'DINO', 'GREEN',
        #                 'CRYPTO', 'PROKAR']

        self.rrs_bands_cci = ['Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_510', 'Rrs_560', 'Rrs_665']
        self.processing_var = ['CHL', 'CDF_AVG', 'CDF_MLP3B', 'CDF_MLP4B', 'CDF_MLP5B', 'CHL_MLP3B', 'CHL_MLP4B',
                               'CHL_MLP5B',
                               'CDF_FLAG_MULTIPLE', 'WEIGHT_MLP3B', 'WEIGHT_MLP4B', 'WEIGHT_MLP5B',
                               'RRS555', 'RRS670', 'CYANOBLOOM', 'FLAG_CDF', 'ADG443', 'APH443', 'BBP443', 'KD490',
                               'MICRO', 'NANO', 'PICO', 'DIATO', 'DINO', 'GREEN', 'CRYPTO', 'PROKAR']
        self.iop_bands = ['ADG443', 'APH443', 'BBP443']

        # defining tile sizes
        self.tileX = 1500
        self.tileY = 1500

        ##defining attributes
        self.varattr = None
        sdir = os.path.abspath(os.path.dirname(__file__))
        foptions = os.path.join(sdir, 'varat_cci.json')
        if not os.path.exists(foptions):
            path2info = os.path.join(os.path.dirname(sdir))
            foptions = os.path.join(path2info, 'varat_cci.json')
        if os.path.exists(foptions):
            f = open(foptions, "r")
            self.varattr = json.load(f)
            f.close()

        ##defining splits for l3_olci
        self.splits = {
            'chl': ['CHL', 'CYANOBLOOM', 'SENSORMASK'],
            'pft': ['PICO', 'NANO', 'MICRO', 'CRYPTO', 'GREEN', 'DIATO', 'DINO', 'PROKAR', 'SENSORMASK'],
            'kd490': ['KD490', 'SENSORMASK'],
            'adg443': ['ADG443', 'SENSORMASK'],
            'aph443': ['APH443', 'SENSORMASK'],
            'bbp443': ['BBP443', 'SENSORMASK']
        }
        ##defining rrs file for applying mask
        self.rrs_l3_olci = ['rrs400', 'rrs412_5', 'rrs442_5', 'rrs490', 'rrs510', 'rrs560', 'rrs620', 'rrs665',
                            'rrs673_75', 'rrs681_25',
                            'rrs708_75', 'rrs753_75', 'rrs778_75', 'rrs865']

        self.mask_file = None
        self.mask_var = None

        if fconfig is not None:
            self.start_balmlp_options(fconfig)

    def start_balmlp_options(self, fconfig):
        import configparser
        options = configparser.ConfigParser()
        options.read(fconfig)
        if not options.has_section('BALMLP'):
            return

        if options.has_option('BALMLP', 'tile_x'):
            try:
                tile_x = int(options['BALMLP']['tile_x'])
            except:
                print(
                    f'[WARNING] Not valid option {options["BALMLP"]["tile_x"]} for tile_x in configuration file. It shoud be a int value greater than zero')
            if tile_x > 0:
                self.tileX = tile_x
            else:
                print(
                    f'[WARNING] Not valid option {options["BALMLP"]["TILE_X"]} for TILE_X in configuration file. It shoud be a int value greater than zero')

        if options.has_option('BALMLP', 'tile_y'):
            try:
                tile_y = int(options['BALMLP']['tile_y'])
            except:
                print(
                    f'[WARNING] Not valid option {options["BALMLP"]["tyle_y"]} for tyle_y in configuration file. It shoud be a int value greater than zero')
            if tile_y > 0:
                self.tileY = tile_y
            else:
                print(
                    f'[WARNING] Not valid option {options["BALMLP"]["TILE_X"]} for TILE_X in configuration file. It shoud be a int value greater than zero')

        if options.has_option('BALMLP', 'mask'):
            mask_s = options['BALMLP']['mask']
            mask_l = [x.strip() for x in mask_s.split(';')]
            if os.path.exists(mask_l[0]):
                self.mask_file = mask_l[0]
                self.mask_var = 'Land_Mask'
                if len(mask_l) == 2:
                    self.mask_var = mask_l[1]
                dmask = Dataset(self.mask_file)
                if self.mask_var not in dmask.variables:
                    print(
                        f'[WARNING] Variable {self.mask_var} is not available in {self.mask_file}. Mask will no be implemented')
                    self.mask_file = None
                    self.mask_var = None
                else:
                    print(f'[INFO] Mask set to variable {self.mask_var} in file {self.mask_file}')
                dmask.close()
            else:
                print(f'[WARNING] File mask: {mask_l} does not exist. Mask will no be implemented')

    def check_runac(self):
        return self.bal_proc.VALID

    def update_attrs_l3_olci(self, date_olci):
        timeliness = self.product_type.split('_')[2].upper()
        self.varattr['GLOBAL']['start_date'] = date_olci.strftime('%Y-%m-%d')
        self.varattr['GLOBAL']['stop_date'] = date_olci.strftime('%Y-%m-%d')
        self.varattr['GLOBAL']['timeliness'] = timeliness
        if timeliness == 'NR':
            product_name = 'OCEANCOLOUR_BAL_BGC_L3_NRT_009_131'
            dataset_name = 'cmems_obs-oc_bal_bgc-plankton_nrt_l3-olci-300m_P1D'
        else:
            product_name = 'OCEANCOLOUR_BAL_BGC_L3_MY_009_133'
            dataset_name = 'cmems_obs-oc_bal_bgc-plankton_my_l3-olci-300m_P1D'
        self.varattr['GLOBAL']['cmems_product_id'] = product_name
        self.varattr['GLOBAL']['title'] = dataset_name

    def set_product_type(self, product_type):
        self.product_type = product_type
        sdir = os.path.abspath(os.path.dirname(__file__))
        if self.product_type == 'polymer':
            name_json = 'varat_polymer.json'
        elif self.product_type.startswith('l3_olci_'):
            name_json = 'varat_l3_olci.json'
        else:
            name_json = 'varat_cci.json'
        foptions = os.path.join(sdir, name_json)
        if not os.path.exists(foptions):
            path2info = os.path.join(os.path.dirname(sdir))
            foptions = os.path.join(path2info, name_json)
        if os.path.exists(foptions):
            f = open(foptions, "r")
            self.varattr = json.load(f)
            f.close()

    def run_process_multiple_files(self, prod_path, output_dir):
        if not self.product_type.startswith('l3_olci_'):
            print('Only OLCI L3 is implemented')
            return

        # self.tileY = 250
        # self.tileX = 250

        from datetime import datetime as dt

        ##retrieving date
        olci_date_str = None
        olci_date = None
        for name in os.listdir(prod_path):
            if name.endswith(f'{self.wlbands_chla_olci_l3[0]}-bal-fr.nc') and name.startswith('O'):
                if name.startswith('Oa') or name.startswith('Ob'): continue
                olci_date_str = name[1:8]
                try:
                    olci_date = dt.strptime(olci_date_str, '%Y%j')
                except:
                    pass
        if olci_date is None:
            print(f'[ERROR] OLCI date could not be retrieved')
            return
        fileout = os.path.join(output_dir, f'O{olci_date_str}-bal-fr_BAL202411.nc')

        if os.path.exists(fileout):
            splitter = Splitter(fileout, olci_date)
            splitter.mask_array = self.get_mask_array()
            splitter.make_multiple_split(os.path.dirname(fileout), self.splits)
            self.apply_mask_to_olci_l3_rrs(prod_path, olci_date_str)
            return

        if self.verbose:
            print(f'[INFO] Date: {olci_date.strftime("%Y-%m-%d")}')
        self.retrieve_info_wlbands_olci_l3(prod_path, olci_date_str)


        if self.applyBandShifting:
            print(f'[INFO] Band shifting is activated')
            print(f'[INFO]   Input bands: {self.central_wl_chla}')
            print(f'[INFO]   Output bands: {self.wl_chla}')
        else:
            print(f'[INFO] Band shifting is not activated')

        startX = 0
        startY = 0
        try:
            input_file = list(self.central_wavelength.keys())[0]
            ncinput = Dataset(input_file)
            endX = ncinput.dimensions['lon'].size - 1
            endY = ncinput.dimensions['lat'].size - 1
            ncinput.close()
        except:
            return
        ny = (endY - startY) + 1
        nx = (endX - startX) + 1
        if self.verbose:
            print(f'[INFO] Image dimensions {ny}x{nx}')

        # defining output arrays
        all_arrays = {}
        for var in self.processing_var:
            array = np.ma.masked_all((ny, nx))
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
                if self.product_type.startswith('l3_olci_'):
                    nvalid, valid_mask = self.get_valid_olci_l3_mask(yini, yend, xini, xend)
                if nvalid > 1:
                    print(f'[INFO] Processing {nvalid} valid pixels...')
                    input_rrs, iop = self.get_valid_rrs_olci_l3(valid_mask, nvalid, yini, yend, xini, xend)

                    for iop_index, iop_band in enumerate(self.iop_bands):
                        array_here = np.ma.masked_all(valid_mask.shape)
                        array_here[valid_mask == 1] = iop[:, iop_index]
                        all_arrays[iop_band][yini - startY:yend - startY, xini - startX:xend - startX] = array_here[:,
                                                                                                         :]

                    cyano_info = self.get_cyano_info_olci_l3(input_rrs)
                    for cyano_band in cyano_info:
                        array_here = np.ma.masked_all(valid_mask.shape)
                        array_cyano = cyano_info[cyano_band]
                        array_here[valid_mask == 1] = array_cyano[:]
                        all_arrays[cyano_band][yini - startY:yend - startY, xini - startX:xend - startX] = array_here[:,
                                                                                                           :]

                    res_algorithm = self.bal_proc.compute_ensemble(input_rrs)

                    for key in res_algorithm.keys():
                        if key.upper() in all_arrays.keys():
                            # print('-->',key.upper(),yini-startY,yend-startY,xini-startX,xend-startX)
                            array_here = np.ma.masked_all(valid_mask.shape)
                            array_here[valid_mask == 1] = res_algorithm[key]
                            all_arrays[key.upper()][yini - startY:yend - startY,
                            xini - startX:xend - startX] = array_here[:, :]

                    # kd, using 490 and 555 bands,
                    kd_res = self.bal_proc_old.compute_kd(input_rrs[:, 1], input_rrs[:, 3])
                    kd_here = np.ma.masked_all(valid_mask.shape)
                    kd_here[valid_mask == 1] = kd_res[:]
                    all_arrays['KD490'][yini - startY:yend - startY, xini - startX:xend - startX] = kd_here[:, :]

                    psc, pft = self.bal_proc_old.compute_psc_pft(res_algorithm['chl'])
                    for var in self.bal_proc_old.psc_var:
                        psc_here = np.ma.masked_all(valid_mask.shape)
                        psc_here[valid_mask == 1] = psc[var][:]
                        all_arrays[var][yini - startY:yend - startY, xini - startX:xend - startX] = psc_here[:, :]
                    for var in self.bal_proc_old.pft_var:
                        pft_here = np.ma.masked_all(valid_mask.shape)
                        pft_here[valid_mask == 1] = pft[var][:]
                        all_arrays[var][yini - startY:yend - startY, xini - startX:xend - startX] = pft_here[:, :]

        if self.verbose:
            print(f'[INFO] Water processing completed')
            print(f'[INFO] Generating output file: {fileout}')

        self.create_file(fileout, prod_path, all_arrays, startY, endY + 1, startX, endX + 1, olci_date)

        ##SPLIT
        splitter = Splitter(fileout, olci_date)
        splitter.mask_array = self.get_mask_array()
        splitter.make_multiple_split(os.path.dirname(fileout), self.splits)

    def run_cci_split(self,fileout):
        if self.verbose:
            print(f'[INFO] Starting CCI splitting and masking...')
        from datetime import datetime as dt
        ncref = Dataset(fileout)
        variables = ['lat', 'lon', 'CHL', 'CYANOBLOOM']
        try:
            date_file = dt.strptime(os.path.basename(fileout)[1:8],'%Y%j').replace(tzinfo=pytz.utc)
        except:
            print(f'[ERROR] Date file could not be set.')
            return
        output_file_chla = os.path.join(os.path.dirname(fileout), f'C{date_file.strftime("%Y%j")}-chl-bal-hr.nc')
        if self.verbose:
            print(f'[INFO] Creating CHL file: {output_file_chla}')
        self.create_copy_final_file(ncref, variables, date_file, output_file_chla)
        variables = ['lat', 'lon', 'MICRO', 'NANO', 'PICO', 'CRYPTO', 'DIATO', 'DINO', 'GREEN', 'PROKAR']
        output_file_pft = os.path.join(os.path.dirname(fileout), f'C{date_file.strftime("%Y%j")}-pft-bal-hr.nc')
        if self.verbose:
            print(f'[INFO] Creating PFT file: {output_file_pft}')
        self.create_copy_final_file(ncref, variables, date_file, output_file_pft)
        ncref.close()

    def run_process(self, prod_path, output_dir):
        if self.product_type.startswith('l3_olci_') and os.path.isdir(prod_path):
            self.run_process_multiple_files(prod_path, output_dir)
            return
        fileout = self.get_file_out(prod_path, output_dir)
        if self.product_type=='cci_split' and os.path.exists(fileout):
            print('')
            self.run_cci_split(fileout)
            return

        if os.path.exists(fileout):
            print(f'[WARNING] Output file {fileout} already exits. Skipping...')
            return
        if self.verbose:
            print(f'[INFO] Starting water processing')

        ncinput = Dataset(prod_path)
        from datetime import datetime as dt
        date_file = None
        if self.product_type == 'polymer':
            self.tileX = 500
            self.tileY = 500
            if 'start_time' in ncinput.ncattrs():
                start_time_str = ncinput.start_time
                try:
                    date_file = dt.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                except:
                    start_time_str = os.path.basename(prod_path).split('_')[7]
                    try:
                        date_file = dt.strptime(start_time_str, '%Y%m%dT%H%M%S')
                    except:
                        pass

        if self.product_type == 'cci':
            #date_file = dt.utcfromtimestamp(ncinput.variables['time'][0]).replace(tzinfo=pytz.UTC)
            date_file = dt.fromtimestamp(ncinput.variables['time'][0]).astimezone(pytz.UTC)

        if date_file is None:
            print(f'[ERROR] Date file could not be set.')
            return
        else:
            print(f'[INFO] Date file: {date_file.strftime("%Y-%m-%d")}')
            print(f'[INFO] Tile size: {self.tileY} x {self.tileX}')

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
        flagging  = None
        if self.product_type == 'polymer':
            flag_band = ncinput.variables['bitmask']
            flagging = polymerflag.Class_Flags_Polymer(flag_band)

        # image limits
        startX,startY,endX,endY = -1,-1,-1,-1

        if self.product_type == 'polymer':
            if prod_path.split('/')[-1].lower().find('trim') > 0:
                startX = 0
                startY = 0
                endX = ncinput.dimensions['width'].size - 1
                endY = ncinput.dimensions['height'].size - 1
            else:
                startY, endY, startX, endX = self.get_geo_limits(ncinput)
                if startY == -1 and endY == -1 and startX == -1 and endX == -1:
                    print(f'[WARNING] Image is not covering the Baltic Sea. Skipping...')
                    return
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

        if self.only_rrs and self.product_type== 'polymer':
            if self.verbose:
                print(f'[INFO] No water processing implemented, only rrs are generated...')
                print(f'[INFO] Generating output file: {fileout}')
            self.create_file(fileout, ncinput, None, startY, endY + 1, startX, endX + 1, date_file)
            return

        # if only_rrs is false, compute chl-a and other variables
        all_arrays = {}
        for var in self.processing_var:
            array = np.ma.masked_all((ny, nx))
            # array[:] = np.NaN
            all_arrays[var] = array

        # computing chla and other variables for each tile
        for y in range(startY, endY, self.tileY):
            ycheck = y - startY
            for x in range(startX, endX, self.tileX):
                xcheck = x - startX
                print(f'[INFO] Processing tile {ycheck}/{ny} - {xcheck}/{nx}')
                yini = y
                yend = y + self.tileY
                if yend > endY: yend = endY + 1
                xini = x
                xend = x + self.tileX
                if xend > endX: xend = endX + 1
                nvalid = 0
                valid_mask = None
                if self.product_type == 'cci':
                    nvalid, valid_mask = self.get_valid_cci_mask(ncinput, yini, yend, xini, xend)
                if self.product_type == 'polymer':
                    nvalid, valid_mask = self.get_valid_polymer_mask(flagging, ncinput, yini, yend, xini, xend)

                if nvalid > 0:
                    input_rrs = None
                    if self.product_type == 'cci':
                        input_rrs, iop, cyano_info = self.get_valid_rrs_cci(ncinput, valid_mask, nvalid, yini, yend,xini, xend)

                    if self.product_type == 'polymer':
                        input_rrs, iop, cyano_info = self.get_valid_rrs_polymer(ncinput, valid_mask, nvalid, yini, yend,xini, xend)

                    if input_rrs is None:
                        continue

                    res_algorithm = self.bal_proc.compute_ensemble(input_rrs)

                    for key in cyano_info.keys():
                        if key.upper() in all_arrays.keys():
                            array_here = np.ma.masked_all(valid_mask.shape)
                            array_here[valid_mask == 1] = cyano_info[key]
                            all_arrays[key.upper()][yini - startY:yend - startY,
                            xini - startX:xend - startX] = array_here[:, :]
                    for key in res_algorithm.keys():
                        if key.upper() in all_arrays.keys():
                            array_here = np.ma.masked_all(valid_mask.shape)
                            array_here[valid_mask == 1] = res_algorithm[key]
                            all_arrays[key.upper()][yini - startY:yend - startY,
                            xini - startX:xend - startX] = array_here[:, :]

                    ##ATTENTION: CODE FOR OLCI (OK FOR MULTI?)
                    if iop is None:
                        continue

                    ##iop
                    for n in range(3):
                        iop_here = np.ma.masked_all(valid_mask.shape)
                        iop_here[valid_mask == 1] = iop[:, n]
                        all_arrays[self.bal_proc_old.iop_var[n]][yini - startY:yend - startY,
                        xini - startX:xend - startX] = iop_here[:, :]

                    # kd, using 490 and 555 bands,
                    kd_res = self.bal_proc_old.compute_kd(input_rrs[:, 1], input_rrs[:, 3])
                    kd_here = np.ma.masked_all(valid_mask.shape)
                    kd_here[valid_mask == 1] = kd_res[:]
                    all_arrays['KD490'][yini - startY:yend - startY, xini - startX:xend - startX] = kd_here[:, :]

                    # psc and pft
                    chla_res = res_algorithm['chl']
                    psc, pft = self.bal_proc_old.compute_psc_pft(chla_res)
                    for var in self.bal_proc_old.psc_var:
                        psc_here = np.ma.masked_all(valid_mask.shape)
                        psc_here[valid_mask == 1] = psc[var][:]
                        all_arrays[var][yini - startY:yend - startY, xini - startX:xend - startX] = psc_here[:, :]
                    for var in self.bal_proc_old.pft_var:
                        pft_here = np.ma.masked_all(valid_mask.shape)
                        pft_here[valid_mask == 1] = pft[var][:]
                        all_arrays[var][yini - startY:yend - startY, xini - startX:xend - startX] = pft_here[:, :]

        if self.verbose:
            print(f'[INFO] Water processing completed')
            print(f'[INFO] Generating output file: {fileout}')

        # print('chla')
        # array_chl = all_arrays['CHL']
        # print(array_chl.shape)
        # dataset_out = Dataset(fileout,'w')
        # dataset_out.createDimension('x',)
        # dataset_out.close()
        self.create_file(fileout, ncinput, all_arrays, startY, endY + 1, startX, endX + 1, date_file)

    def allow_csv_test(self):
        return True

    def run_from_mdb_file(self, path_mdb, output_path):
        if not self.check_runac():
            print(f'[ERROR] Error starting Baltic 202411 chl-a algorithm')
            return
        if not os.path.exists(path_mdb):  ##should be checked before
            return
        print(f'[INFO] Input MDB FILE: {path_mdb}')

        variables_bal202411 = {
            'satellite_CHL_202411': 'chl',
            'satellite_CDF_AVG': 'cdf_avg',
            'satellite_CDF_FLAG_MULTIPLE': 'cdf_flag_multiple',
            'satellite_CDF_MLP3B': 'cdf_mlp3b',
            'satellite_CDF_MLP4B': 'cdf_mlp4b',
            'satellite_CDF_MLP5B': 'cdf_mlp5b',
            'satellite_CHL_MLP3B': 'chl_mlp3b',
            'satellite_CHL_MLP4B': 'chl_mlp4b',
            'satellite_CHL_MLP5B': 'chl_mlp5b',
            'satellite_WEIGHT_MLP3B': 'weight_mlp3b',
            'satellite_WEIGHT_MLP4B': 'weight_mlp4b',
            'satellite_WEIGHT_MLP5B': 'weight_mlp5b'
        }
        variable_list = list(variables_bal202411.keys())

        dataset_w = self.start_output_dataset(path_mdb, output_path, variable_list)
        rrs = dataset_w.variables['satellite_Rrs'][:]
        wl_bands = dataset_w.variables['satellite_bands'][:]

        n_mu = rrs.shape[0]
        n_rows = rrs.shape[2]
        n_cols = rrs.shape[3]
        print(f'[INFO] Number of match-ups: {n_mu}')

        for imu in range(n_mu):
            if imu==0 or imu%100==0:
                print(f'[INFO] Processing data for match-up: {imu}')

            spectra = self.get_rrs_spectra_from_mdb(np.ma.squeeze(rrs[imu, :, :, :]), wl_bands)
            res_all = self.bal_proc.compute_ensemble(spectra)
            for name_var in variables_bal202411:
                key = variables_bal202411[name_var]
                array = res_all[key]
                array = np.ma.reshape(array, (n_rows, n_cols))
                dataset_w[name_var][imu, :, :] = array[:, :]

        dataset_w.close()
        print(f'[INFO] Completed')

    def start_output_dataset(self, input_file, output_file, new_variables):
        print(f'[INFO] Making copy from {input_file} to {output_file}')
        input_dataset = Dataset(input_file)
        ncout = Dataset(output_file, 'w', format='NETCDF4')

        # copy global attributes all at once via dictionary
        ncout.setncatts(input_dataset.__dict__)

        # copy dimensions
        for name, dimension in input_dataset.dimensions.items():
            ncout.createDimension(
                name, (len(dimension) if not dimension.isunlimited() else None))

        for name, variable in input_dataset.variables.items():
            fill_value = None
            if '_FillValue' in list(variable.ncattrs()):
                fill_value = variable._FillValue
            ncout.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                                 complevel=6)
            # copy variable attributes all at once via dictionary
            ncout[name].setncatts(input_dataset[name].__dict__)
            ncout[name][:] = input_dataset[name][:]

        for name in new_variables:
            ncout.createVariable(name, 'f4', ('satellite_id', 'rows', 'columns'), fill_value=-999.0, zlib=True,
                                 complevel=6)

        return ncout

    def get_rrs_spectra_from_mdb(self, rrs, wl_bands):
        n_bands = rrs.shape[0]
        n_data = rrs.shape[1] * rrs.shape[2]
        all_spectra = np.transpose(np.ma.reshape(rrs, (n_bands, n_data)))

        ##to get original
        ##all_spectra_cual = np.ma.reshape(np.transpose(all_spectra_tal),rrs.shape)

        wl_output = self.bal_proc.input_bands
        n_output = len(wl_output)
        out_spectra = np.ma.masked_all((n_data, n_output))

        for iwl, wl in enumerate(wl_output):
            index_wl = np.argmin(np.abs(wl - wl_bands))
            diff_wl = abs(wl_bands[index_wl] - wl)
            if diff_wl == 0.0:
                out_spectra[:, iwl] = all_spectra[:, index_wl]
            else:  ##band shifting
                all_spectra_bs = np.transpose(all_spectra)
                rrsdata_out = bsc_qaa.bsc_qaa(all_spectra_bs, wl_bands, [wl])
                out_spectra[:, iwl] = np.squeeze(rrsdata_out)

        return out_spectra

    def run_from_csv_file(self, path_csv, output_path):
        if not self.check_runac():
            print(f'[ERROR] Error starting Baltic 202411 chl-a algorithm')
            return
        if not os.path.exists(path_csv):  ##should be checked before
            return
        print(f'[INFO] Input CSV file: {path_csv}')
        print(f'[INFO] Output CSV file: {output_path}')
        csv_lois = CSV_FILE(path_csv)
        print(f'[INFO] Getting spectra...')
        spectra = csv_lois.get_rrs_spectra(self.bal_proc.input_bands, self.applyBandShifting)
        print(f'[INFO] Processing data...')
        res_all = self.bal_proc.compute_ensemble(spectra)

        print(f'[INFO] Adding data to output CSV...')
        csv_lois.start_copy_output()
        for key in res_all:
            # print(key, '->', res_all[key].shape)
            print(f'[INFO] -> {key}')
            csv_lois.add_column_to_output(key, res_all[key])
        print(f'[INFO] Saving...')
        csv_lois.save_output(output_path)
        print(f'[INFO] Completed')

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

    def apply_mask_to_olci_l3_rrs(self, prod_path, olci_date_str):
        mask_array = self.get_mask_array()
        if mask_array is None:
            return
        for band in self.rrs_l3_olci:
            input_file = os.path.join(prod_path, f'O{olci_date_str}-{band}-bal-fr.nc')
            if os.path.exists(input_file):
                print(f'[INFO] Applying mask to file: {input_file}')
                temp_file = os.path.join(prod_path, f'O{olci_date_str}-{band}-bal-fr_temp.nc')
                mask_applied = self.apply_mask_impl(input_file, mask_array, temp_file)
                if mask_applied:
                    os.rename(temp_file, input_file)
                else:
                    if os.path.exists(temp_file): os.remove(temp_file)

    def apply_mask_impl(self, input_file, mask, output_file):
        mask_applied = False
        from netCDF4 import Dataset
        nc_input = Dataset(input_file, 'r')
        nc_out = Dataset(output_file, 'w')

        # copy global attributes all at once via dictionary
        nc_out.setncatts(nc_input.__dict__)

        # copy dimensions
        for name, dimension in nc_input.dimensions.items():
            nc_out.createDimension(
                name, (len(dimension) if not dimension.isunlimited() else None))

        for name, variable in nc_input.variables.items():
            fill_value = -999.0
            if '_FillValue' in list(variable.ncattrs()):
                fill_value = variable._FillValue

            nc_out.createVariable(name, variable.datatype, variable.dimensions, fill_value=fill_value, zlib=True,
                                  complevel=6)

            # copy variable attributes all at once via dictionary
            nc_out[name].setncatts(nc_input[name].__dict__)

            if len(variable.dimensions) == 3 and nc_input.variables[name].shape[1] == mask.shape[0] and \
                    nc_input.variables[name].shape[2] == mask.shape[1]:
                print(f'[INFO] Applying mask to variable: {name}')
                array = np.squeeze(nc_input[name][:])
                array[mask == 1] = fill_value
                nc_out[name][0, :, :] = array[:, :]
                mask_applied = True
            else:
                nc_out[name][:] = nc_input[name][:]

        nc_input.close()
        nc_out.close()
        return mask_applied

    def retrieve_info_wlbands_olci_l3(self, prod_path, olci_date_str):
        for band in self.wlbands_chla_olci_l3:
            input_file = os.path.join(prod_path, f'O{olci_date_str}-{band}-bal-fr.nc')
            if os.path.exists(input_file):
                self.central_wavelength[input_file] = float(band[3:].replace('_', '.'))
                self.central_wl_chla.append(self.central_wavelength[input_file])
            else:
                self.central_wavelength = {}
                self.central_wl_chla = []

    def get_geo_limits(self, ncpolymer):
        array_lat = np.array(ncpolymer.variables['latitude'][:, :])
        array_lon = np.array(ncpolymer.variables['longitude'][:, :])
        geovalid = np.logical_and(np.logical_and(array_lat >= self.geo_limits[0], array_lat <= self.geo_limits[1]),
                                  np.logical_and(array_lon >= self.geo_limits[2], array_lon <= self.geo_limits[3]))
        if np.count_nonzero(geovalid) > 0:
            r, c = np.where(geovalid)
            startY = r.min()
            endY = r.max()
            startX = c.min()
            endX = c.max()
        else:
            startY = -1
            endY = -1
            startX = -1
            endX = -1

        return startY, endY, startX, endX

    def get_valid_olci_l3_mask(self, yini, yend, xini, xend):
        array_mask = None
        input_files = list(self.central_wavelength.keys())
        for idx in range(1, 4):
            input_file = input_files[idx]
            ncinfo = Dataset(input_file)
            var = input_file[input_file.find('rrs'):input_file.find('-bal-fr.nc')].upper()
            array_mask_here = ncinfo.variables[var][0, yini:yend, xini:xend]
            if array_mask is None:
                array_mask = np.where(array_mask_here.mask, 0, 1)
            else:
                array_mask[array_mask_here.mask] = 0
            ncinfo.close()
        nvalid = array_mask.sum()
        return nvalid, array_mask

    def get_valid_cci_mask(self, ncinfo, yini, yend, xini, xend):
        mask_bands = ['Rrs_490', 'Rrs_510', 'Rrs_560']
        array_mask = None
        for band in mask_bands:
            array_mask_here = ncinfo.variables[band][0, yini:yend, xini:xend]
            # print('-->',yini,yend,xini,xend,array_mask_here.shape,np.ma.count_masked(array_mask_here))
            if array_mask is None:
                array_mask = np.where(array_mask_here.mask, 0, 1)
                # print('==>,',array_mask_here.shape,array_mask.shape)
            else:
                array_mask[array_mask_here.mask] = 0
        nvalid = array_mask.sum()

        return nvalid, array_mask

    def get_valid_polymer_mask(self, flagging, ncpolymer, yini, yend, xini, xend):
        satellite_flag_band = np.array(ncpolymer.variables['bitmask'][yini:yend, xini:xend])
        flag_mask = flagging.MaskGeneralV5(satellite_flag_band)
        valid_mask = np.array(flag_mask == 0).astype(np.byte)
        for iband in range(5):
            wlband = self.wlbands_chla_polymer[iband]
            band = np.ma.array(ncpolymer.variables[wlband][yini:yend, xini:xend])
            valid_mask[band.mask] = 0

        nvalid = valid_mask.sum()
        return nvalid, valid_mask

    def get_valid_rrs_cci(self, ncinput, valid_mask, nvalid, yini, yend, xini, xend):

        nbands = len(self.wl_chla)
        rrsdata = np.zeros([nbands, nvalid])
        for iband, wlband in enumerate(self.wlbands_chla_cci):
            band = ncinput.variables[wlband][0, yini:yend, xini:xend]
            # band_valid  = band[valid_mask==1]
            rrsdata[iband, :] = band[valid_mask == 1].flatten()

        iop = None
        if self.applyBandShifting:
            rrsdata_out, iop = bsc_qaa.bsc_qaa(rrsdata, self.central_wl_chla, self.wl_chla)
            rrsdata = rrsdata_out.transpose()
        else:
            rrsdata = rrsdata.transpose()

        ##ciano_mask
        # 'RRS555', 'RRS670', 'FLAG_CYANO'
        index555 = self.wl_chla.index(555)  # index555 es 3
        index670 = self.wl_chla.index(670)  # index670 es 4
        rrs555 = rrsdata[:, index555]
        rrs670 = rrsdata[:, index670]
        flag_cyano = np.zeros(rrs555.shape)
        flag_cyano[rrs555 >= self.th_cyano_555] = flag_cyano[rrs555 >= self.th_cyano_555] + 1
        flag_cyano[rrs670 >= self.th_cyano_670] = flag_cyano[rrs670 >= self.th_cyano_670] + 2
        cyano_info = {
            'rrs555': rrs555,
            'rrs670': rrs670,
            'cyanobloom': flag_cyano
        }

        return rrsdata, iop, cyano_info

    def get_valid_rrs_polymer(self, ncpolymer, valid_mask, nvalid, yini, yend, xini, xend):
        # 443_490_510_555_670
        # wlbands = ['Rw443', 'Rw490', 'Rw510', 'Rw560', 'Rw665']
        # rrsdata = np.zeros([nvalid, 5])
        rrsdata = np.zeros([5, nvalid])
        for iband in range(5):
            wlband = self.wlbands_chla_polymer[iband]
            band = np.ma.array(ncpolymer.variables[wlband][yini:yend, xini:xend])
            rrsdata[iband, :] = band[valid_mask==1]
        rrsdata = rrsdata / np.pi

        iop = None
        if self.applyBandShifting:
            rrsdata_out, iop = bsc_qaa.bsc_qaa(rrsdata, self.central_wl_chla, self.wl_chla)
            rrsdata = rrsdata_out.transpose()
        else:
            rrsdata = rrsdata.transpose()

        ##ciano_mask
        # 'RRS555', 'RRS670', 'FLAG_CYANO'
        index555 = self.wl_chla.index(555)  # index555 es 3
        index670 = self.wl_chla.index(670)  # index670 es 4
        rrs555 = rrsdata[:, index555]
        rrs670 = rrsdata[:, index670]
        flag_cyano = np.zeros(rrs555.shape)
        flag_cyano[rrs555 >= self.th_cyano_555] = flag_cyano[rrs555 >= self.th_cyano_555] + 1
        flag_cyano[rrs670 >= self.th_cyano_670] = flag_cyano[rrs670 >= self.th_cyano_670] + 2
        cyano_info = {
            'rrs555': rrs555,
            'rrs670': rrs670,
            'cyanobloom': flag_cyano
        }

        return rrsdata, iop, cyano_info

    def get_valid_rrs_olci_l3(self, valid_mask, nvalid, yini, yend, xini, xend):

        nbands = len(self.wl_chla)
        rrsdata = np.zeros([nbands, nvalid])
        input_files = list(self.central_wavelength.keys())

        for iband, input_file in enumerate(input_files):
            ncinput = Dataset(input_file)
            var = input_file[input_file.find('rrs'):input_file.find('-bal-fr.nc')].upper()
            band = ncinput.variables[var][0, yini:yend, xini:xend]
            # band_valid  = band[valid_mask==1]
            rrsdata[iband, :] = band[valid_mask == 1].flatten()
            ncinput.close()
        iop = None
        if self.applyBandShifting:
            rrsdata_out, iop = bsc_qaa.bsc_qaa(rrsdata, self.central_wl_chla, self.wl_chla)
            rrsdata = rrsdata_out.transpose()
        else:
            rrsdata = rrsdata.transpose()

        return rrsdata, iop

    def get_cyano_info_olci_l3(self, input_rrs):
        index555 = self.wl_chla.index(555)  # index555 es 3
        index670 = self.wl_chla.index(670)  # index670 es 4
        rrs555 = input_rrs[:, index555]
        rrs670 = input_rrs[:, index670]
        flag_cyano = np.zeros(rrs555.shape)
        flag_cyano[rrs555 >= self.th_cyano_555] = flag_cyano[rrs555 >= self.th_cyano_555] + 1
        flag_cyano[rrs670 >= self.th_cyano_670] = flag_cyano[rrs670 >= self.th_cyano_670] + 2
        cyano_info = {
            'RRS555': rrs555,
            'RRS670': rrs670,
            'CYANOBLOOM': flag_cyano
        }
        return cyano_info

    def get_mask_array(self):
        mask_array = None
        if self.mask_file is not None and self.mask_var is not None:
            dmask = Dataset(self.mask_file)
            mask_array = dmask.variables[self.mask_var][:]
            dmask.close()
        return mask_array

    def create_file(self, fileout, ncinput, all_arrays, yini, yend, xini, xend, date_file):

        if self.verbose:
            print(f'[INFO] Writting output file: {fileout}')

        if self.product_type.startswith('l3_olci_'):
            input_dir = ncinput

        from baltic_mlp import baloutputfile
        ncoutput = baloutputfile.BalOutputFile(fileout)
        if not ncoutput.FILE_CREATED:
            print(f'[ERROR] File {fileout} could not be created. Please check permissions')
            return False

        if self.product_type == 'polymer':
            ncoutput.set_global_attributes(ncinput)
        if self.product_type == 'cci':
            ncoutput.set_global_attributes_from_dict(self.varattr)
        if self.product_type.startswith('l3_olci_'):
            self.update_attrs_l3_olci(date_file)
            ncoutput.set_global_attributes_from_dict(self.varattr)

        if all_arrays is None:
            ny = yend - yini
            nx = xend - xini
        else:
            array_chl = all_arrays['CHL']
            ny = array_chl.shape[0]
            nx = array_chl.shape[1]
            if self.product_type == 'cci':
                array_chl = np.flipud(array_chl)

        ncoutput.create_dimensions(ny, nx)

        # latitude, longitude
        if self.verbose:
            print(f'[INFO]    Adding latitude/longitude...')
        var_lat_name = 'latitude'
        var_lon_name = 'longitude'
        if self.product_type.startswith('l3_olci_'):
            var_lat_name = 'lat'
            var_lon_name = 'lon'
            ncinput = Dataset(list(self.central_wavelength.keys())[0])
        if len(ncinput.variables[var_lat_name].dimensions) == 2:
            array_lat = np.array(ncinput.variables[var_lat_name][yini:yend, xini:xend])
        elif len(ncinput.variables[var_lat_name].dimensions) == 1:
            array_lat = np.array(ncinput.variables[var_lat_name][yini:yend])
        if len(ncinput.variables[var_lon_name].dimensions) == 2:
            array_lon = np.array(ncinput.variables[var_lon_name][yini:yend, xini:xend])
        elif len(ncinput.variables[var_lon_name].dimensions) == 1:
            array_lon = np.array(ncinput.variables[var_lon_name][xini:xend])

        if self.product_type == 'cci':
            array_lat = np.flip(array_lat)

        ncoutput.create_lat_long_variables(array_lat, array_lon)
        if self.product_type.startswith('l3_olci_'):
            ncinput.close()

        # rrs
        if self.verbose:
            print(f'[INFO]    Adding rrs:')
        ##cci
        if self.product_type == 'cci':
            for rrsvar in self.rrs_bands_cci:
                wl = float(rrsvar.split('_')[1])
                name_band = f'RRS{wl:.2f}'
                name_band = name_band.replace('.', '_')
                if name_band.endswith('_00'): name_band = name_band[:-3]
                if name_band.endswith('_50'): name_band = name_band[:-1]
                if self.verbose:
                    print(f'[INFO]     {rrsvar}->{name_band}')
                array = ncinput.variables[rrsvar][yini:yend, xini:xend]
                array = np.ma.squeeze(array)
                array = np.flipud(array)
                ncoutput.create_rrs_variable(array, name_band, wl, self.varattr, self.product_type)

        if self.product_type == 'polymer':

            flag_band = ncinput.variables['bitmask']
            flagging = polymerflag.Class_Flags_Polymer(flag_band)
            mask_here = self.get_valid_polymer_mask(flagging,ncinput,yini,yend,xini,xend)

            for rrsvar in self.rrsbands_polymer.keys():
                namevar = self.rrsbands_polymer[rrsvar]
                if self.verbose:
                    print(f'[INFO]     {rrsvar}->{namevar}')
                if not rrsvar in ncinput.variables:
                    print(f'[WARNING] Band {rrsvar} is not available in the Polymer file')
                    continue
                array = np.ma.array(ncinput.variables[rrsvar][yini:yend, xini:xend])
                #array[array.mask] = -999
                array[mask_here==0] = -999
                array[~array.mask] = array[~array.mask] / np.pi

                wl = self.central_wavelength[rrsvar]
                ncoutput.create_rrs_variable(array, namevar, wl, self.varattr, self.product_type)

        ##l3_olci
        if self.product_type.startswith('l3_olci_'):
            added_sensor_mask = False
            for name in os.listdir(input_dir):
                if name.startswith('Oa') or name.startswith('Ob'): continue
                if name.startswith('O') and name.endswith('bal-fr.nc') and name.find('rrs') > 0:
                    input_file = os.path.join(input_dir, name)
                    nchere = Dataset(input_file)
                    if not added_sensor_mask and 'SENSORMASK' in nchere.variables:
                        var_sensor_mask = nchere.variables['SENSORMASK']
                        ncoutput.copy_var('SENSORMASK', var_sensor_mask)
                        added_sensor_mask = True
                    rrsvar = input_file[input_file.find('rrs'):input_file.find('-bal-fr.nc')].upper()
                    array = np.ma.array(nchere.variables[rrsvar][0, yini:yend, xini:xend])
                    wl = float(rrsvar[3:].replace('_', '.'))
                    ncoutput.create_rrs_variable(array, rrsvar, wl, self.varattr, self.product_type)
                    nchere.close()

        if all_arrays is not None:
            # chl
            if self.verbose:
                print(f'[INFO]    Adding chla...')
            ncoutput.create_var_general(array_chl, 'CHL', self.varattr)

            # other variables
            done_variables = ['CHL']
            for ovar in all_arrays.keys():
                if ovar in done_variables:
                    continue
                if self.verbose:
                    print(f'[INFO]    Adding extra variable {ovar}....')
                array_here = all_arrays[ovar]
                if self.product_type == 'cci':
                    array_here = np.flipud(array_here)
                if ovar.startswith('FLAG'):
                    ncoutput.create_var_flag_general(array_here, ovar, self.varattr)
                else:
                    ncoutput.create_var_general(array_here, ovar, self.varattr)
        ncoutput.close_file()

        if self.verbose:
            print(f'[INFO]    File {fileout} was created')

        if self.product_type == 'cci':
            ncref = Dataset(fileout)
            variables = ['lat', 'lon', 'CHL', 'CYANOBLOOM']
            output_file_chla = os.path.join(os.path.dirname(fileout), f'C{date_file.strftime("%Y%j")}-chl-bal-hr.nc')
            if self.verbose:
                print(f'[INFO] Creating CHL file: {output_file_chla}')
            self.create_copy_final_file(ncref, variables, date_file, output_file_chla)
            variables = ['lat', 'lon', 'MICRO', 'NANO', 'PICO', 'CRYPTO', 'DIATO', 'DINO', 'GREEN', 'PROKAR']
            output_file_pft = os.path.join(os.path.dirname(fileout), f'C{date_file.strftime("%Y%j")}-pft-bal-hr.nc')
            if self.verbose:
                print(f'[INFO] Creating PFT file: {output_file_pft}')
            self.create_copy_final_file(ncref, variables, date_file, output_file_pft)
            ncref.close()

    def create_copy_final_file(self, ncref, variables, date_file, output_file):
        ncout = Dataset(output_file, 'w', format='NETCDF4')

        # copy global attributes all at once via dictionary
        ncout.setncatts(ncref.__dict__)

        # copy dimensions
        for name, dimension in ncref.dimensions.items():
            ncout.createDimension(name, (len(dimension) if not dimension.isunlimited() else None))

        # create time variable
        time_var = ncout.createVariable('time', 'i4', ('time',), zlib=True, complevel=6)
        time_var.long_name = "reference time"
        time_var.standard_name = "time"
        time_var.axis = "T"
        time_var.calendar = "Gregorian"
        time_var.units = "seconds since 1981-01-01 00:00:00"
        epoch_copernicus = datetime.datetime(1981, 1, 1, 0, 0, 0).replace(tzinfo=pytz.utc)
        time_var[0] = int((date_file - epoch_copernicus).total_seconds())

        # other variables
        # copy variables
        for name, variable in ncref.variables.items():
            if name not in variables:
                continue
            fill_value = None
            if '_FillValue' in list(variable.ncattrs()):
                fill_value = variable._FillValue

            datatype = variable.datatype
            if name == 'CYANOBLOOM':
                datatype = 'i4'
                fill_value = -999
            # print(name, datatype, fill_value)

            if name == 'lat' or name == 'lon':
                dimensions = variable.dimensions
            else:
                dimensions = ('time', 'lat', 'lon')

            ncout.createVariable(name, datatype, dimensions, fill_value=fill_value, zlib=True, complevel=6)

            # copy variable attributes
            for at in variable.ncattrs():
                if at == '_FillValue': continue
                ncout[name].setncattr(at, variable.getncattr(at))

            # copy variable data
            if name == 'lat' or name == 'lon':
                ncout[name][:] = ncref[name][:]
            elif name == 'CYANOBLOOM':
                ncout[name][0, :, :] = np.int32(ncref[name][:, :])
            else:
                ncout[name][0, :, :] = ncref[name][:, :]

        ncout.close()
