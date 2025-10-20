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
import scipy.io
from scipy import stats

# _______________________________________________________________________________________

# MLP parameters as a simplified disctionary


def mlp_dict(mlp_vals, printmlp_flag=False):

    mlp = {'name': 'mlp'}
    mlp['muIn'] = mlp_vals['model'].muIn
    mlp['stdIn'] = mlp_vals['model'].stdIn
    mlp['muOut'] = mlp_vals['model'].muOut
    mlp['stdOut'] = mlp_vals['model'].stdOut
    mlp['w1'] = mlp_vals['model'].par.w1
    mlp['b1'] = mlp_vals['model'].par.b1
    mlp['w2'] = mlp_vals['model'].par.w2
    mlp['b2'] = mlp_vals['model'].par.b2
    mlp['pcvecs'] = mlp_vals['model'].pcvecs
    mlp['pcvals'] = mlp_vals['model'].pcvals

    # Print values
    if printmlp_flag:

        np.set_printoptions(linewidth=np.inf)
        np.set_printoptions(formatter={'all': lambda x: format(x, '10.6E')})
        print('mu_l = ', np.array_str(mlp['muIn']))
        print('sigma_l = ', np.array_str(mlp['stdIn']))
        print('w1 = ', np.array_str(mlp['w1']))
        print('b1 = ', np.array_str(mlp['b1']))
        print('w2 = ', np.array_str(mlp['w2']))
        print('b2 = {0:.6e}'.format(mlp['b2']))
        print('mu_c = {0:.6e}'.format(mlp['muOut']))
        print('sigma_c = {0:.6e}'.format(mlp['stdOut']))
        print('pcvecs = ', np.array_str(mlp['pcvecs']))
        print('pcvals = ', np.array_str(mlp['pcvals']))
        np.set_printoptions()


    return mlp
# _______________________________________________________________________________________

# Whitening transformation


def zscored_calc(samp, ref_mean, ref_eigenval, ref_eigenvec):

    xi = np.inner((samp - ref_mean), ref_eigenvec.T)

    zscored = xi/np.sqrt(ref_eigenval)

    eta = np.linalg.norm(zscored, axis=len(samp.shape) -
                         1, keepdims=True) / len(ref_mean)

    return zscored, eta
# _____________________________________________________________________________________________________

# Mahalanobis distance


def mdist_calc(samp, mu, cov_inv):

    if len(samp.shape) == 1:
        mdist = np.sqrt(np.dot(np.dot((samp - mu), cov_inv),
                               (samp - mu).T))
    else:
        n_rows, n_cols = samp.shape
        mdist = np.zeros(n_rows)
        for i_row in range(n_rows):
            mdist[i_row] = np.sqrt(np.dot(np.dot((samp[i_row, :] - mu), cov_inv),
                                          (samp[i_row, :] - mu).T))

    return mdist
# _______________________________________________________________________________________

# Compute Chl with the MLP


def mlp_chl(mlp_model, samp, test_flag=False):

    # Data pre-processing
    samp_log = np.log10(samp)
    x = (samp_log - mlp_model['model'].muIn) / mlp_model['model'].stdIn


    # MLP forward
    z = np.tanh(x.dot(mlp_model['model'].par.w1) + mlp_model['model'].par.b1)
    y = z.dot(mlp_model['model'].par.w2) + mlp_model['model'].par.b2

    # Data post-processing
    chl = 10**((y * mlp_model['model'].stdOut) + mlp_model['model'].muOut)

    # Novelty
    zeta, eta = zscored_calc(
        samp_log, mlp_model['model'].muIn, mlp_model['model'].pcvals, mlp_model['model'].pcvecs)

    # Probability distribution based on z-scored data
    df = len(mlp_model['model'].muIn)
    mdist_zeta = mdist_calc(zeta, np.zeros(df), np.identity(df))


    # Survival probability based on the Chi2 distribution
    cdf = np.squeeze(1 - stats.chi2.cdf(mdist_zeta**2, df=df))

    # Weight
    zeta = 1 / eta

    # Return results
    res = dict()
    res["chl"] = chl
    res["zeta"] = zeta
    res["cdf"] = cdf

    return res
# _______________________________________________________________________________________