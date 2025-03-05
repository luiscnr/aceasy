import numpy as np
from scipy.io import loadmat
import pandas as pd
import os

pd.set_option('display.precision', 4)


class BalMLP():
    def __init__(self, path_file):

        if path_file is None:
            path_file = os.path.dirname(__file__)
            if not os.path.exists(path_file) or not os.path.isdir(path_file):
                path_file = None

        # Six bands: Table 2
        parFile = "blts_rrs412-rrs443-rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file, parFile)
        self.mlp_412_443_490_510_555_670 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # Five bands (412 incl.) Table 2
        parFile = "blts_rrs412-rrs443-rrs490-rrs510-rrs555-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file, parFile)
        self.mlp_412_443_490_510_555 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # Five bands (670 incl.): Table 2
        parFile = "blts_rrs443-rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file, parFile)
        self.mlp_443_490_510_555_670 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # Four bands: Table 3
        parFile = "blts_rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file, parFile)
        self.mlp_490_510_555_670 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # Three bands: Table 4
        parFile = "blts_rrs490-rrs510-rrs555-mlp-1-of-1-test-1-of-1.par"
        if path_file is not None:
            parFile = os.path.join(path_file, parFile)
        self.mlp_490_510_555 = loadmat(parFile, squeeze_me=True, struct_as_record=False)

        # RESULTS
        self.wgt_3 = None
        self.chl_3 = None
        self.chl_4 = None
        self.wgt_4 = None
        self.wgt_5_670 = None
        self.chl_5_670 = None
        self.wgt_5_412 = None
        self.chl_5_412 = None
        self.chl_6 = None
        self.ens4 = None
        self.ens3 = None

    # Compute Chl
    def mlpChl(self, mlpPar, rrs):
        # Data pre-processing

        l = np.log10(rrs)
        x = (l - mlpPar['model'].muIn) / mlpPar['model'].stdIn

        # MLP forward
        z = np.tanh(x.dot(mlpPar['model'].par.w1) + mlpPar['model'].par.b1)
        y = z.dot(mlpPar['model'].par.w2) + mlpPar['model'].par.b2

        # Data post-processing
        chl = 10 ** ((y * mlpPar['model'].stdOut) + mlpPar['model'].muOut)

        # Novelty
        xi = np.inner((l - mlpPar['model'].muIn), mlpPar['model'].pcvecs.T)
        zeta = xi / np.sqrt(mlpPar['model'].pcvals)
        eta = np.transpose(np.linalg.norm(zeta, axis=1, keepdims=True) / l.shape[1])

        # Weight
        wgt = 1 / eta

        wgt = wgt[0]

        return chl, wgt

    def mlp_6_bands(self, rrs):
        return self.mlpChl(self.mlp_412_443_490_510_555_670, rrs)

    def mlp_5_bands_412(self, rrs):
        return self.mlpChl(self.mlp_412_443_490_510_555, rrs)

    def mlp_5_bands_670(self, rrs):
        return self.mlpChl(self.mlp_443_490_510_555_670, rrs)

    def mlp_4_bands(self, rrs):
        return self.mlpChl(self.mlp_490_510_555_670, rrs)

    def mlp_3_bands(self, rrs):
        return self.mlpChl(self.mlp_490_510_555, rrs)

    def compute_chla_ensemble(self, rrs):
        # Chla and weight  (one for each spectral configuration)
        self.chl_6, self.wgt_6 = self.mlp_6_bands(rrs[:, [0, 1, 2, 3, 4, 5]])
        self.chl_5_412, self.wgt_5_412 = self.mlp_5_bands_412(rrs[:, [0, 1, 2, 3, 4]])
        self.chl_5_670, self.wgt_5_670 = self.mlp_5_bands_670(rrs[:, [1, 2, 3, 4, 5]])
        self.chl_4, self.wgt_4 = self.mlp_4_bands(rrs[:, [2, 3, 4, 5]])
        self.chl_3, self.wgt_3 = self.mlp_3_bands(rrs[:, [2, 3, 4]])

        # Ensembles
        tmp = np.multiply(self.chl_6, self.wgt_6) + np.multiply(self.chl_5_670, self.wgt_5_670) + np.multiply(
            self.chl_4, self.wgt_4) + np.multiply(self.chl_3, self.wgt_3)
        self.ens4 = np.divide(tmp, self.wgt_6 + self.wgt_5_670 + self.wgt_4 + self.wgt_3)

        tmp = np.multiply(self.chl_5_670, self.wgt_5_670) + np.multiply(self.chl_4, self.wgt_4) + np.multiply(
            self.chl_3, self.wgt_3)

        self.ens3 = np.divide(tmp, self.wgt_5_670 + self.wgt_4 + self.wgt_3)

    ##rrs with 5 bands:443_490_510_555_670
    def compute_chla_ensemble_3bands(self, rrs):
        self.chl_5_670, self.wgt_5_670 = self.mlp_5_bands_670(rrs[:, [0, 1, 2, 3, 4]])
        self.chl_4, self.wgt_4 = self.mlp_4_bands(rrs[:, [1, 2, 3, 4]])
        self.chl_3, self.wgt_3 = self.mlp_3_bands(rrs[:, [1, 2, 3]])

        tmp = np.multiply(self.chl_5_670, self.wgt_5_670) + np.multiply(self.chl_4, self.wgt_4) + np.multiply(
            self.chl_3, self.wgt_3)

        self.ens3 = np.divide(tmp, self.wgt_5_670 + self.wgt_4 + self.wgt_3)
        return self.ens3

    def get_values(self):
        values = [self.wgt_3[[0]], self.wgt_4[[0]], self.wgt_5_412[[0]], self.wgt_5_670[[0]], self.wgt_6[[0]],
                  self.chl_3[[0]], self.chl_4[[0]], self.chl_5_412[[0]], self.chl_5_670[[0]], self.chl_6[[0]],
                  self.ens3[[0]], self.ens4[[0]]]
        for idx in range(len(values)):
            values[idx] = float(values[idx])
        return values
