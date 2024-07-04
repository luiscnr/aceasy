import os
from scipy.io import loadmat
from baltic202411 import bsc_module as bsc
import numpy as np


class BALTIC_202411:

    def __init__(self):
        self.VALID = True
        self.thresh_cdf = 0.001
        self.input_bands = [443, 490, 510, 555, 670]
        dirmodels = os.path.join(os.path.dirname(__file__), 'models')

        par_file = os.path.join(dirmodels, 'blts_rrs490-rrs510-rrs555-mlp-1-of-1-test-1-of-1.par')
        if os.path.exists(par_file):
            self.mlp_3 = loadmat(par_file, squeeze_me=True, struct_as_record=False)
        else:
            print(f'[ERROR] Error starting BALTIC_202411 algorithm. mpl_3 {par_file} file was not found')

        par_file = os.path.join(dirmodels, 'blts_rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par')
        if os.path.exists(par_file):
            self.mlp_4 = loadmat(par_file, squeeze_me=True, struct_as_record=False)
        else:
            print(f'[ERROR] Error starting BALTIC_202411 algorithm. mlp_4 {par_file} file was not found')

        par_file = os.path.join(dirmodels, 'blts_rrs443-rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par')
        if os.path.exists(par_file):
            self.mlp_5 = loadmat(par_file, squeeze_me=True, struct_as_record=False)
        else:
            print(f'[ERROR] Error starting BALTIC_202411 algorithm. mlp_5 {par_file} file was not found')

    ##rrs_in: 490, 510, 555
    def mlp_three_bands(self, rrs_in):
        return bsc.mlp_chl(self.mlp_3, rrs_in) if self.VALID else None

    ##rrs_in: 490, 510, 555, 670
    def mlp_four_bands(self, rrs_in):
        return bsc.mlp_chl(self.mlp_4, rrs_in) if self.VALID else None

    ##rrs_in: 443, 490, 510, 555, 670
    def mlp_five_band(self, rrs_in):
        return bsc.mlp_chl(self.mlp_5, rrs_in) if self.VALID else None

    ##rrs_in: n x 5 with band: 443, 490, 510, 555, 670
    def compute_ensemble(self, rrs_in):
        if not self.VALID:
            return None

        # MLP 2018 Table4 uses 3 bands (490, 510, 555)
        bsc_3 = bsc.mlp_chl(self.mlp_3, rrs_in[:, 1:4])

        # MLP 2018 Table3 uses 4 bands (490, 510, 555 and 670)
        bsc_4 = bsc.mlp_chl(self.mlp_4, rrs_in[:, 1:5])

        # MLP 2018 5 bands (443, 490, 510, 555, 670)
        bsc_5 = bsc.mlp_chl(self.mlp_5, rrs_in[:, 0:5])

        # Arrays with three ensembles
        chl = np.array([bsc_3['chl'], bsc_4['chl'], bsc_5['chl']]).transpose()
        cdf = np.array([bsc_3['cdf'], bsc_4['cdf'], bsc_5['cdf']]).transpose()

        # Masking values lower or equal than given thershold
        cdf_masked = np.ma.masked_where(cdf<=self.thresh_cdf,cdf)

        # dot product
        chl_cdf_inner = np.ma.sum(np.ma.multiply(chl,cdf_masked),axis=1)

        # cdf sum
        cdf_sum = np.ma.sum(cdf_masked,axis=1)

        # chl-a computation
        chl_cdf = np.ma.divide(chl_cdf_inner,cdf_sum)

        # average cdf
        cdf_ens = np.ma.mean(cdf_masked,axis=1)

        # using bsc_3 if all the cdf values are lower than thershold (masked values)
        chl_cdf[chl_cdf.mask] = bsc_3['chl'][chl_cdf.mask]
        cdf_ens[cdf_ens.mask] = -999

        #flag_cdf
        cdf = np.ma.masked_where(cdf<0,cdf)
        cdf = np.ma.masked_invalid(cdf)
        cdf_mask_mlp3b = np.ma.where(cdf[:,0] >= 0.001,2,0)
        cdf_mask_mlp4b = np.ma.where(cdf[:,1] >= 0.001,4,0)
        cdf_mask_mlp5b = np.ma.where(cdf[:,2] >= 0.001,8,0)
        cdf_flag_multiple = np.ma.filled(cdf_mask_mlp3b,0)+np.ma.filled(cdf_mask_mlp4b,0)+np.ma.filled(cdf_mask_mlp5b,0)
        cdf_flag_multiple[cdf_flag_multiple == 0] = 1
        cdf_flag_multiple = np.ma.array(cdf_flag_multiple,mask=(cdf[:,0].mask*cdf[:,1].mask*cdf[:,2].mask))

        cdf_mlp3b = np.ma.masked_less(cdf[:,0], 0.001)
        cdf_mlp4b = np.ma.masked_less(cdf[:,1], 0.001)
        cdf_mlp5b = np.ma.masked_less(cdf[:,2], 0.001)
        cdf_sum = np.ma.filled(cdf_mlp3b,0) + np.ma.filled(cdf_mlp4b,0) + np.ma.filled(cdf_mlp5b,0)
        weight_mlp3b = np.ma.divide(cdf_mlp3b, cdf_sum)
        weight_mlp4b = np.ma.divide(cdf_mlp4b, cdf_sum)
        weight_mlp5b = np.ma.divide(cdf_mlp5b, cdf_sum)

        res = {
            'chl': chl_cdf,
            'cdf_avg': cdf_ens,
            'cdf_flag_multiple': cdf_flag_multiple,
            'cdf_mlp3b': cdf[:, 0],
            'cdf_mlp4b': cdf[:, 1],
            'cdf_mlp5b': cdf[:, 2],
            'chl_mlp3b': chl[:, 0],
            'chl_mlp4b': chl[:, 1],
            'chl_mlp5b': chl[:, 2],
            'weight_mlp3b': weight_mlp3b,
            'weight_mlp4b': weight_mlp4b,
            'weight_mlp5b': weight_mlp5b
        }

        return res
