######################################################
#
# Service contract: Activities in the framework of the
# COPERNICUS MARINE - PRODUCTION PROVISION OF OCEAN
# OBSERVATION PRODUCTS THEMATIC ASSEMBLY CENTRE (TAC).
#
# Authors
#  ISMAR-CNR Net: https://www.ismar.cnr.it/en/
#  AEQUORA Net: http://aequora.org/
#
######################################################

# Import modules
import numpy as np
from scipy.io import loadmat
from scipy.io import readsav
import pandas as pd
import matplotlib.pyplot as plt
import bsc_module as bsc

# Processing flags
plot_flag = True
printmlp_flag = False
printdata_flag = False

# Processing options
thresh_cdf = 0.001
sav_file = 'dset[olci]ver[all]bloom[4]'
# sav_file = 'dset[final]ver[all]bloom[4]'

# Load sav data
data_directory = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CODE_DAVIDE_2024/ens_update_2024-06-23/data/sav/'
sav = readsav(data_directory + sav_file + '.sav', python_dict=True)
#sav = readsav('../data/sav/' + sav_file + '.sav', python_dict=True)

# Plot spectra
if plot_flag:
    fig = plt.figure(figsize=(5, 3.5))
    ax = fig.add_subplot()
    for i_rec in range(sav['data']['n_rec'][0]):
        plt.plot(sav['data']['bands'][0], sav['data']['rrs'][0][i_rec])
    plt.grid(linestyle='--', linewidth=0.5, color='.8')
    plt.title(sav_file)
    plt.xlabel('Wavelengths [nm]')
    plt.ylabel('$R_{rs}$ [sr$^{-1}$]')
    file_name = data_directory + sav_file + 'res[Rrs].png'
    plt.savefig(file_name, dpi=300, bbox_inches='tight')
    print('Out: ' + file_name)
    plt.close()

# MLPs

# Three bands: Table 4
par_file = "./models/blts_rrs490-rrs510-rrs555-mlp-1-of-1-test-1-of-1.par"
mlp_3 = loadmat(par_file, squeeze_me=True, struct_as_record=False)
if printmlp_flag:
    print('\nmlp_3')
bsc.mlp_dict(mlp_3, printmlp_flag=printmlp_flag)

# Four bands (670 incl.): Table 3
par_file = "./models/blts_rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par"
mlp_4 = loadmat(par_file, squeeze_me=True, struct_as_record=False)
if printmlp_flag:
    print('\nmlp_4')
bsc.mlp_dict(mlp_4, printmlp_flag=printmlp_flag)

# Five bands (670 incl.): Table 2
par_file = "./models/blts_rrs443-rrs490-rrs510-rrs555-rrs670-mlp-1-of-1-test-1-of-1.par"
mlp_5 = loadmat(par_file, squeeze_me=True, struct_as_record=False)
if printmlp_flag:
    print('\nmlp_5')
bsc.mlp_dict(mlp_5, printmlp_flag=printmlp_flag)

# ENS CDF

# Compute ens Chl for all data
n_rec = sav['data']['n_rec'][0]
chl_cdf = np.zeros([n_rec])
cdf_ens = np.zeros([n_rec])
for i_rec in range(n_rec):

    # Rrs spectrum
    rrs = sav['data']['rrs'][0][i_rec]

    # Chla and weights

    # MLP 2018 Table4 uses 3 bands (490, 510, 555)
    bsc_3 = bsc.mlp_chl(mlp_3, rrs[[2, 3, 4]])

    # MLP 2018 Table3 uses 4 bands (490, 510, 555 and 670)
    bsc_4 = bsc.mlp_chl(mlp_4, rrs[[2, 3, 4, 5]])

    # MLP 2018 5 bands (443, 490, 510, 555, 670)
    bsc_5 = bsc.mlp_chl(mlp_5, rrs[[1, 2, 3, 4, 5]])

    # Ensemble
    chl = np.array([bsc_3['chl'], bsc_4['chl'], bsc_5['chl']]).transpose()
    cdf = np.array([bsc_3['cdf'], bsc_4['cdf'], bsc_5['cdf']]).transpose()

    # Do the merging with record above the CDF threshold
    idx = np.argwhere(cdf > thresh_cdf).transpose()
    if idx.size:
        chl_cdf[i_rec] = np.inner(
            chl[idx], cdf[idx]).squeeze() / np.sum(cdf[idx])
        cdf_ens[i_rec] = np.mean(cdf[idx])

    # ... otherwise use bsc_3
    else:
        chl_cdf[i_rec] = bsc_3['chl']
        cdf_ens[i_rec] = -999.

    # Log
    if printdata_flag:
        print('i:', 1 + i_rec, 'chl3b:', bsc_3['chl'], 'chl4b:', bsc_4['chl'],
              'chl5b:', bsc_5['chl'], 'ens:', chl_cdf[i_rec])

# Python vs IDL results (ENS3 is in the 7th col of sav data) 
if plot_flag:
    idl_val = sav['data']['chl_cdf'][0][:, 6]
    py_val = chl_cdf
    fig = plt.figure(figsize=(3.5, 3.5))
    ax = fig.add_subplot()
    plt.plot(idl_val, py_val, marker='x')
    plt.title(sav_file)
    plt.xlabel('Chl-a ENS3 IDL')
    plt.ylabel('Chl-a ENS3 Python')
    ax.set_aspect('equal', adjustable='box')
    max_val = 1.1 * np.maximum.reduce([idl_val, idl_val], axis=(1, 0))
    min_val = ax.get_ylim()
    plt.xlim(min_val[0], max_val)
    plt.ylim(min_val[0], max_val)
    yticks = ax.get_yticks()
    plt.xticks(yticks)
    ax.minorticks_on()
    ax.set_box_aspect(1)
    plt.axis('equal')
    plt.grid(linestyle='--', linewidth=0.5, color='.8')
    file_name = data_directory + sav_file + 'res[PYvsIDL].png'
    plt.savefig(file_name, dpi=300, bbox_inches='tight')
    print('Out: ' + file_name)
    plt.close()
