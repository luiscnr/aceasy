import pandas as pd

from balmlpensemble import BalMLP
import numpy as np
def main():
    print('STARTED')
    balmlp = BalMLP(None)
    # rrs = np.array([[0.001901, 0.002393, 0.003571, 0.003793, 0.003952, 0.000817], \
    #                 [0.002598, 0.002946, 0.003723, 0.003385, 0.002716, 0.000433]])
    #
    # ens = balmlp.compute_chla_ensemble(rrs)
    #
    # print(ens)
    file = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/CHLA_DATA/matchup_bal_cci_chl_surf6_orig__rrsmatch_w12CHL__FM.csv'
    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/BAL_EVOLUTION/EXAMPLES/CHLA/CHLA_DATA/matchup_bal_cci_chl_surf6_orig__rrsmatch_w12CHL__FM_out.csv'
    df = pd.read_csv(file,sep=';')
    var_names = ['rrs_412','rrs_443','rrs_490','rrs_510','rrs_555','rrs_670']
    rrs = df.loc[:,var_names]
    balmlp.compute_chla_ensemble(np.array(rrs))
    rrs = pd.concat([rrs,pd.Series(balmlp.chl_6)],axis=1)
    rrs = pd.concat([rrs, pd.Series(balmlp.chl_4)], axis=1)
    rrs = pd.concat([rrs, pd.Series(balmlp.chl_3)], axis=1)
    rrs = pd.concat([rrs, pd.Series(balmlp.chl_5_670)], axis=1)
    rrs = pd.concat([rrs, pd.Series(balmlp.chl_5_412)], axis=1)
    rrs.to_csv(file_out,sep=';')


    print(rrs)


if __name__ == '__main__':
    main()