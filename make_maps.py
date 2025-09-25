import argparse, configparser, os
import shutil
from datetime import timedelta
import numpy.ma
import pandas as pd
from netCDF4 import Dataset
from matplotlib import pyplot as plt
from matplotlib.colors import LogNorm
import matplotlib.ticker as mticker
import cartopy.crs as ccrs
import cartopy
from datetime import datetime as dt
import numpy as np
import matplotlib as mpl
import matplotlib.colors as colors
from matplotlib.colors import ListedColormap
from config_reader import ConfigReader

parser = argparse.ArgumentParser(description="Make maps launcher")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-i", "--input_path", help="Input path")
parser.add_argument("-c","--config_path",help = "Configuration file with options in section: MAKE_MAPS")
parser.add_argument("-o", "--output_path", help="Output path")
args = parser.parse_args()
# options = configparser.ConfigParser()
#     options.read(fconfig)
#     if options.has_section('GLOBAL'):

def compute_diff():
    from datetime import datetime as dt
    from datetime import timedelta
    dir_base_new = '/store3/OC/CCI_v2017/daily_v202411'
    format_name = 'M$DATE1$.0000.bal.all_products.CCI.$DATE2$0000.v0.$DATE1$0000.data_BAL202411.nc'
    dir_base_old = '/store3/OC/CCI_v2017/daily_v202207'
    dir_base_dif = '/store3/OC/CCI_v2017/daily_diff'
    date_here = dt(2018, 6, 1)
    end_date = dt(2018, 9, 30)
    while date_here <= end_date:
        print('-------------------------------------------------------------------')
        print(date_here)
        date1 = date_here.strftime('%Y%j')
        date2 = date_here.strftime('%d%b%y')
        name = format_name.replace('$DATE1$', date1)
        name = name.replace('$DATE2$', date2)
        file_new = os.path.join(dir_base_new, name)
        dir_old = os.path.join(dir_base_old, date_here.strftime('%Y'), date_here.strftime('%j'))
        file_old = os.path.join(dir_old, f'C{date1}-chl-bal-hr.nc')
        if not os.path.exists(file_old) or not os.path.exists(file_new):
            date_here = date_here + timedelta(hours=24)
            continue
        print(file_new)
        print(file_old)

        dataset_old = Dataset(file_old)
        chl_old = dataset_old.variables['CHL'][:]
        chl_old = chl_old.squeeze()
        chl_old = np.flipud(chl_old)
        dataset_old.close()

        dataset_new = Dataset(file_new)
        chl_new = dataset_new.variables['CHL'][:]
        lat_array = dataset_new.variables['latitude'][:]
        lon_array = dataset_new.variables['longitude'][:]

        ##getting cdf_flag_multiple and weights
        cdf_mlp3b = dataset_new.variables['CDF_MLP3B'][:]
        cdf_mlp4b = dataset_new.variables['CDF_MLP4B'][:]
        cdf_mlp5b = dataset_new.variables['CDF_MLP5B'][:]
        cdf_mask_mlp3b = np.ma.where(cdf_mlp3b >= 0.001, 2, 0)
        cdf_mask_mlp4b = np.ma.where(cdf_mlp4b >= 0.001, 4, 0)
        cdf_mask_mlp5b = np.ma.where(cdf_mlp5b >= 0.001, 8, 0)
        cdf_flag_multiple = np.ma.filled(cdf_mask_mlp3b, 0) + np.ma.filled(cdf_mask_mlp4b, 0) + np.ma.filled(
            cdf_mask_mlp5b, 0)
        cdf_flag_multiple[cdf_flag_multiple == 0] = 1
        cdf_flag_multiple = np.ma.array(cdf_flag_multiple,
                                        mask=(cdf_mlp3b.mask * cdf_mask_mlp4b.mask * cdf_mask_mlp5b.mask))
        cdf_mlp3b = np.ma.masked_less(cdf_mlp3b, 0.001)
        cdf_mlp4b = np.ma.masked_less(cdf_mlp4b, 0.001)
        cdf_mlp5b = np.ma.masked_less(cdf_mlp5b, 0.001)
        cdf_sum = np.ma.filled(cdf_mlp3b, 0) + np.ma.filled(cdf_mlp4b, 0) + np.ma.filled(cdf_mlp5b, 0)
        weight_mlp3b = np.ma.divide(cdf_mlp3b, cdf_sum)
        weight_mlp4b = np.ma.divide(cdf_mlp4b, cdf_sum)
        weight_mlp5b = np.ma.divide(cdf_mlp5b, cdf_sum)

        ##chl dif
        chl_dif = chl_old - chl_new

        ##chl percent difference
        chl_percent_diff = 100 * ((chl_new - chl_old) / chl_old)
        chl_abs_percent_diff = np.ma.abs(chl_percent_diff)

        ##coverage
        coverage = np.zeros(chl_old.shape)
        coverage[~chl_old.mask] = coverage[~chl_old.mask] + 1
        coverage[~chl_new.mask] = coverage[~chl_new.mask] + 2

        file_out = os.path.join(dir_base_dif, f'Diff_Completed_{date1}.nc')
        nc_out = Dataset(file_out, 'w')

        # copy dimensions
        for name, dimension in dataset_new.dimensions.items():
            nc_out.createDimension(
                name, (len(dimension) if not dimension.isunlimited() else None))

        nc_out.createVariable('latitude', 'f4', ('lat',), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['latitude'][:] = lat_array
        nc_out.createVariable('longitude', 'f4', ('lon',), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['longitude'][:] = lon_array
        nc_out.createVariable('chl_old', 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['chl_old'][:] = chl_old
        nc_out.createVariable('chl_new', 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['chl_new'][:] = chl_new
        nc_out.createVariable('chl_dif', 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['chl_dif'][:] = chl_dif
        nc_out.createVariable('coverage', 'i4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['coverage'][:] = coverage
        nc_out.createVariable('cdf_flag_multiple', 'i4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['cdf_flag_multiple'][:] = cdf_flag_multiple
        nc_out.createVariable('weight_mlp_3b', 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['weight_mlp_3b'][:] = weight_mlp3b
        nc_out.createVariable('weight_mlp_4b', 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['weight_mlp_4b'][:] = weight_mlp4b
        nc_out.createVariable('weight_mlp_5b', 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['weight_mlp_5b'][:] = weight_mlp5b
        nc_out.createVariable('chl_rpd', 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['chl_rpd'][:] = chl_percent_diff
        nc_out.createVariable('chl_apd', 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out['chl_apd'][:] = chl_abs_percent_diff

        nc_out.close()
        dataset_new.close()

        date_here = date_here + timedelta(hours=24)
    return True


##using complete diff files, local
def compare_old_new_v2():
    from datetime import datetime as dt
    from datetime import timedelta
    dir_daily_diff = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CODE_DAVIDE_2024/daily_diff'
    file_out = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CODE_DAVIDE_2024/CoverageNew_v2.nc'
    date_here = dt(2018, 6, 1)
    end_date = dt(2018, 9, 30)

    sum_rpd_cdf = np.ma.zeros((1147, 1185))
    sum_apd_cdf = np.ma.zeros((1147, 1185))
    n_cdf_total = np.ma.zeros((1147, 1185))

    sum_rpd_nocdf = np.ma.zeros((1147, 1185))
    sum_apd_nocdf = np.ma.zeros((1147, 1185))
    n_nocdf_total = np.ma.zeros((1147, 1185))

    sum_rpd = np.ma.zeros((1147, 1185))
    sum_apd = np.ma.zeros((1147, 1185))
    n_total = np.ma.zeros((1147, 1185))

    sum_rpd_cdf_complete = np.ma.zeros((1147, 1185))
    sum_apd_cdf_complete = np.ma.zeros((1147, 1185))
    n_cdf_complete_total = np.ma.zeros((1147, 1185))

    sum_weight_mpl3 = np.ma.zeros((1147, 1185))
    sum_weight_mpl4 = np.ma.zeros((1147, 1185))
    sum_weight_mpl5 = np.ma.zeros((1147, 1185))

    nc_out = None
    while date_here <= end_date:

        date = date_here.strftime('%Y%j')
        file_diff = os.path.join(dir_daily_diff, f'Diff_Completed_{date}.nc')
        # print(file_diff)
        if not os.path.exists(file_diff):
            date_here = date_here + timedelta(hours=24)
            continue
        print('-------------------------------------------------------------------')
        print(date_here)
        dataset = Dataset(file_diff)
        if nc_out is None:
            print('Starting nc_out')
            nc_out = Dataset(file_out, 'w')
            # copy dimensions
            for name, dimension in dataset.dimensions.items():
                nc_out.createDimension(name, (len(dimension) if not dimension.isunlimited() else None))
            lat_array = dataset.variables['latitude'][:]
            lon_array = dataset.variables['longitude'][:]
            nc_out.createVariable('latitude', 'f4', ('lat',), fill_value=-999.0, zlib=True, complevel=6)
            nc_out['latitude'][:] = lat_array
            nc_out.createVariable('longitude', 'f4', ('lon',), fill_value=-999.0, zlib=True, complevel=6)
            nc_out['longitude'][:] = lon_array

        flag_multiple = dataset.variables['cdf_flag_multiple'][:]
        chl_rpd = dataset.variables['chl_rpd'][:]
        chl_apd = dataset.variables['chl_apd'][:]

        sum_rpd_cdf[flag_multiple >= 2] = sum_rpd_cdf[flag_multiple >= 2] + chl_rpd[flag_multiple >= 2]
        sum_apd_cdf[flag_multiple >= 2] = sum_apd_cdf[flag_multiple >= 2] + chl_apd[flag_multiple >= 2]
        n_cdf_total[flag_multiple >= 2] = n_cdf_total[flag_multiple >= 2] + 1

        sum_rpd_nocdf[flag_multiple == 1] = sum_rpd_nocdf[flag_multiple == 1] + chl_rpd[flag_multiple == 1]
        sum_apd_nocdf[flag_multiple == 1] = sum_apd_nocdf[flag_multiple == 1] + chl_apd[flag_multiple == 1]
        n_nocdf_total[flag_multiple == 1] = n_nocdf_total[flag_multiple == 1] + 1

        sum_rpd[flag_multiple >= 1] = sum_rpd[flag_multiple >= 1] + chl_rpd[flag_multiple >= 1]
        sum_apd[flag_multiple >= 1] = sum_apd[flag_multiple >= 1] + chl_apd[flag_multiple >= 1]
        n_total[flag_multiple >= 1] = n_total[flag_multiple >= 1] + 1

        sum_rpd_cdf_complete[flag_multiple == 14] = sum_rpd_cdf_complete[flag_multiple == 14] + chl_rpd[
            flag_multiple == 14]
        sum_apd_cdf_complete[flag_multiple == 14] = sum_apd_cdf_complete[flag_multiple == 14] + chl_apd[
            flag_multiple == 14]
        n_cdf_complete_total[flag_multiple == 14] = n_cdf_complete_total[flag_multiple == 14] + 1

        weight_mpl3 = dataset.variables['weight_mlp_3b'][:]
        sum_weight_mpl3 = sum_weight_mpl3 + np.ma.filled(weight_mpl3, 0)

        weight_mpl4 = dataset.variables['weight_mlp_4b'][:]
        sum_weight_mpl4 = sum_weight_mpl4 + np.ma.filled(weight_mpl4, 0)

        weight_mpl5 = dataset.variables['weight_mlp_5b'][:]
        sum_weight_mpl5 = sum_weight_mpl5 + np.ma.filled(weight_mpl5, 0)

        dataset.close()
        date_here = date_here + timedelta(hours=24)

    n_cdf_total = np.ma.masked_equal(n_cdf_total, 0)
    n_nocdf_total = np.ma.masked_equal(n_nocdf_total, 0)
    n_total = np.ma.masked_equal(n_total, 0)
    n_cdf_complete_total = np.ma.masked_equal(n_cdf_complete_total, 0)
    coverage_cdf = (n_cdf_total / n_total) * 100
    coverage_no_cdf = (n_nocdf_total / n_total) * 100

    mrpd_cdf = sum_rpd_cdf / n_cdf_total
    mapd_cdf = sum_apd_cdf / n_cdf_total
    mrpd_nocdf = sum_rpd_nocdf / n_nocdf_total
    mapd_nocdf = sum_apd_nocdf / n_nocdf_total
    mrpd = sum_rpd / n_total
    mapd = sum_apd / n_total
    mrpd_cdf_complete = sum_rpd_cdf_complete / n_cdf_complete_total
    mapd_cdf_complete = sum_apd_cdf_complete / n_cdf_complete_total

    coverage_mlp3 = (sum_weight_mpl3 / n_cdf_total) * 100
    coverage_mlp4 = (sum_weight_mpl4 / n_cdf_total) * 100
    coverage_mlp5 = (sum_weight_mpl5 / n_cdf_total) * 100

    variable_list = {
        'n_total': n_total,
        'n_cdf_total': n_cdf_total,
        'n_nocdf_total': n_nocdf_total,
        'coverage_cdf': coverage_cdf,
        'coverage_nocdf': coverage_no_cdf,
        'mrpd_cdf': mrpd_cdf,
        'mapd_cdf': mapd_cdf,
        'mrpd_nocdf': mrpd_nocdf,
        'mapd_nocdf': mapd_nocdf,
        'mrpd': mrpd,
        'mapd': mapd,
        'mrpd_cdf_complete': mrpd_cdf_complete,
        'mapd_cdf_complete': mapd_cdf_complete,
        'coverage_mlp3': coverage_mlp3,
        'coverage_mlp4': coverage_mlp4,
        'coverage_mlp5': coverage_mlp5,
    }
    for name_var in variable_list.keys():
        array = variable_list[name_var]
        nc_out.createVariable(name_var, 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out[name_var][:] = array
    nc_out.close()

    return True


##using incomplete diff files
def compare_old_new_v1():
    from datetime import datetime as dt
    from datetime import timedelta
    dir_base_new = '/store3/OC/CCI_v2017/daily_v202411'
    format_name = 'M$DATE1$.0000.bal.all_products.CCI.$DATE2$0000.v0.$DATE1$0000.data_BAL202411.nc'
    dir_base_old = '/store3/OC/CCI_v2017/daily_diff'
    file_comparison_old_new = '/store3/OC/CCI_v2017/daily_v202411/Comparison_old_new.csv'
    fw = open(file_comparison_old_new, 'w')
    fw.write('Date;Chl-aOld;Chl-aNew;FlagMultiple')
    date_here = dt(2018, 6, 1)
    end_date = dt(2018, 9, 30)

    weight_mlp3b_total = np.ma.zeros((1147, 1185))
    weight_mlp4b_total = np.ma.zeros((1147, 1185))
    weight_mlp5b_total = np.ma.zeros((1147, 1185))
    n_cdf_total = np.ma.zeros((1147, 1185))
    n_nocdf_total = np.ma.zeros((1147, 1185))
    file_out = '/store3/OC/CCI_v2017/daily_v202411/CoverageNew.nc'
    nc_out = None
    while date_here <= end_date:
        print('-------------------------------------------------------------------')
        print(date_here)
        date1 = date_here.strftime('%Y%j')
        date2 = date_here.strftime('%d%b%y')
        name = format_name.replace('$DATE1$', date1)
        name = name.replace('$DATE2$', date2)
        file_new = os.path.join(dir_base_new, name)
        file_old = os.path.join(dir_base_old, f'Diff_{date1}.nc')
        if os.path.exists(file_new) and os.path.exists(file_old):
            dataset_old = Dataset(file_old)
            chla_new = dataset_old.variables['chl_new'][:]
            chla_old = dataset_old.variables['chl_old'][:]
            coverage = dataset_old.variables['coverage'][:]
            dataset_old.close()
            dataset = Dataset(file_new)
            cdf_mlp3b = dataset.variables['CDF_MLP3B'][:]
            cdf_mlp4b = dataset.variables['CDF_MLP4B'][:]
            cdf_mlp5b = dataset.variables['CDF_MLP5B'][:]

            if nc_out is None:
                nc_out = Dataset(file_out, 'w')
                # copy dimensions
                for name, dimension in dataset.dimensions.items():
                    nc_out.createDimension(name, (len(dimension) if not dimension.isunlimited() else None))
                lat_array = dataset.variables['latitude'][:]
                lon_array = dataset.variables['longitude'][:]
                nc_out.createVariable('latitude', 'f4', ('lat',), fill_value=-999.0, zlib=True, complevel=6)
                nc_out['latitude'][:] = lat_array
                nc_out.createVariable('longitude', 'f4', ('lon',), fill_value=-999.0, zlib=True, complevel=6)
                nc_out['longitude'][:] = lon_array

            dataset.close()
            cdf_mask_mlp3b = np.ma.where(cdf_mlp3b >= 0.001, 2, 0)
            cdf_mask_mlp4b = np.ma.where(cdf_mlp4b >= 0.001, 4, 0)
            cdf_mask_mlp5b = np.ma.where(cdf_mlp5b >= 0.001, 8, 0)
            cdf_flag_multiple = cdf_mask_mlp3b + cdf_mask_mlp4b + cdf_mask_mlp5b
            cdf_flag_multiple[cdf_flag_multiple == 0] = 1

            cdf_mlp3b = np.ma.masked_less(cdf_mlp3b, 0.001)
            cdf_mlp4b = np.ma.masked_less(cdf_mlp4b, 0.001)
            cdf_mlp5b = np.ma.masked_less(cdf_mlp5b, 0.001)
            cdf_mlp3b[np.logical_and(cdf_mlp3b.mask, cdf_flag_multiple >= 2)] = 0
            cdf_mlp4b[np.logical_and(cdf_mlp4b.mask, cdf_flag_multiple >= 2)] = 0
            cdf_mlp5b[np.logical_and(cdf_mlp5b.mask, cdf_flag_multiple >= 2)] = 0
            cdf_sum = cdf_mlp3b + cdf_mlp4b + cdf_mlp5b
            weight_mlp3b = np.ma.divide(cdf_mlp3b, cdf_sum)
            weight_mlp4b = np.ma.divide(cdf_mlp4b, cdf_sum)
            weight_mlp5b = np.ma.divide(cdf_mlp5b, cdf_sum)

            weight_mlp3b_total[weight_mlp3b > 0] = weight_mlp3b_total[weight_mlp3b > 0] + weight_mlp3b[weight_mlp3b > 0]
            weight_mlp4b_total[weight_mlp4b > 0] = weight_mlp4b_total[weight_mlp4b > 0] + weight_mlp4b[weight_mlp4b > 0]
            weight_mlp5b_total[weight_mlp5b > 0] = weight_mlp5b_total[weight_mlp5b > 0] + weight_mlp4b[weight_mlp5b > 0]
            n_cdf_total[cdf_flag_multiple >= 2] = n_cdf_total[cdf_flag_multiple >= 2] + 1
            n_nocdf_total[cdf_flag_multiple == 1] = n_nocdf_total[cdf_flag_multiple == 1] + 1

            values_new = chla_new[coverage == 3].flatten()
            values_old = chla_old[coverage == 3].flatten()
            flag_multiple = cdf_flag_multiple[coverage == 3].flatten()
            if len(values_new) > 0:
                for idx in range(len(values_new)):
                    fw.write('\n')
                    fw.write(
                        f'{date_here.strftime("%Y-%m-%d")};{values_old[idx]};{values_new[idx]};{flag_multiple[idx]}')
        date_here = date_here + timedelta(hours=24)

    fw.close()

    ##coverage file
    weight_mlp3b_total = np.ma.masked_equal(weight_mlp3b_total, 0)
    weight_mlp4b_total = np.ma.masked_equal(weight_mlp4b_total, 0)
    weight_mlp5b_total = np.ma.masked_equal(weight_mlp5b_total, 0)
    n_total = n_cdf_total + n_nocdf_total
    n_cdf_total = np.ma.masked_equal(n_cdf_total, 0)
    n_nocdf_total = np.ma.masked_equal(n_nocdf_total, 0)
    n_total = np.ma.masked_equal(n_total, 0)
    coverage_cdf = (n_cdf_total / n_total) * 100
    coverage_no_cdf = (n_nocdf_total / n_total) * 100
    coverage_cdf_mlp3 = (weight_mlp3b_total / n_cdf_total) * 100
    coverage_cdf_mlp4 = (weight_mlp4b_total / n_cdf_total) * 100
    coverage_cdf_mlp5 = (weight_mlp5b_total / n_cdf_total) * 100
    coverage_total_mlp3 = ((weight_mlp3b_total + n_nocdf_total) / n_total) * 100
    variable_list = {
        'weight_mlp3b_total': weight_mlp3b_total,
        'weight_mlp4b_total': weight_mlp4b_total,
        'weight_mlp5b_total': weight_mlp5b_total,
        'n_total': n_total,
        'n_cdf_total': n_cdf_total,
        'n_nocdf_total': n_nocdf_total,
        'coverage_cdf': coverage_cdf,
        'coverage_nocdf': coverage_no_cdf,
        'coverage_cdf_mlp3': coverage_cdf_mlp3,
        'coverage_cdf_mlp4': coverage_cdf_mlp4,
        'coverage_cdf_mlp5': coverage_cdf_mlp5,
        'coverage_total_mlp3': coverage_total_mlp3,
    }
    for name_var in variable_list.keys():
        array = variable_list[name_var]
        nc_out.createVariable(name_var, 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        nc_out[name_var][:] = array
    nc_out.close()

    return True


def plot_coverage():
    #file_coverage = '/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/CODE_DAVIDE_2024/CoverageNew_v2.nc'
    file_coverage = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CODE_AND_MAP_ANALYSIS_202411/COVERAGE_ENS_YEAR/COVERAGE_ENS_CDF_ALL_YEARS.nc'



    dataset = Dataset(file_coverage)
    lat_array = dataset.variables['lat'][:]
    lon_array = dataset.variables['lon'][:]

    total_variables = ['n_total','n_cdf_total','n_nocdf_total']
    titles = ['Total days','Total days (CDF)','Total days (No CDF)']
    for variable,title in zip(total_variables,titles):
        coverage = np.ma.squeeze(dataset.variables[variable][0,:,:])
        fig, ax = start_full_figure()
        h = ax.pcolormesh(lon_array, lat_array, coverage, vmin=0, vmax=3200, cmap=mpl.colormaps['jet'])
        cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
        cbar.ax.tick_params(labelsize=15)
        cbar.set_label(label=f'# Days', size=15)
        ax.set_title(title)
        file_out = os.path.join(os.path.dirname(file_coverage), f'{variable.upper()}.png')
        fig.savefig(file_out, dpi=300, bbox_inches='tight')
        plt.close(fig)


    # coverage = np.ma.squeeze(dataset.variables['coverage_cdf'][0,:,:])
    # coverage = coverage * 100
    # fig, ax = start_full_figure()
    # h = ax.pcolormesh(lon_array, lat_array, coverage, vmin=0, vmax=100, cmap=mpl.colormaps['jet'])
    # cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # cbar.set_label(label=f'Coverage(%)', size=15)
    # title = f'CDF Coverage'
    # ax.set_title(title)
    # file_out = os.path.join(os.path.dirname(file_coverage), f'CDFCoverage.png')
    # fig.savefig(file_out, dpi=300, bbox_inches='tight')
    # plt.close(fig)
    #
    # coverage = np.ma.squeeze(dataset.variables['coverage_no_cdf'][0,:,:])
    # coverage = coverage * 100
    # fig, ax = start_full_figure()
    # h = ax.pcolormesh(lon_array, lat_array, coverage, vmin=0, vmax=100, cmap=mpl.colormaps['jet'])
    # cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # cbar.set_label(label=f'Coverage(%)', size=15)
    # title = f'NO CDF Coverage'
    # ax.set_title(title)
    # file_out = os.path.join(os.path.dirname(file_coverage), f'NOCDFCoverage.png')
    # fig.savefig(file_out, dpi=300, bbox_inches='tight')
    # plt.close(fig)
    #
    # coverage_variables = ['coverage_total_mlp3','coverage_total_mlp4','coverage_total_mlp5']
    # titles = ['Total Coverage MLP 3 bands','Total Coverage MLP 4 bands','Total Coverage MLP 5 bands']
    # for variable,title in zip(coverage_variables,titles):
    #     coverage = np.ma.squeeze(dataset.variables[variable][0,:,:])
    #     coverage = coverage * 100
    #     fig, ax = start_full_figure()
    #     h = ax.pcolormesh(lon_array, lat_array, coverage, vmin=0, vmax=100, cmap=mpl.colormaps['jet'])
    #     cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    #     cbar.ax.tick_params(labelsize=15)
    #     cbar.set_label(label=f'Coverage(%)', size=15)
    #     ax.set_title(title)
    #     file_out = os.path.join(os.path.dirname(file_coverage), f'{variable.upper()}.png')
    #     fig.savefig(file_out, dpi=300, bbox_inches='tight')
    #     plt.close(fig)
    #
    # coverage_variables = ['coverage_cdf_mlp_3', 'coverage_cdf_mlp_4', 'coverage_cdf_mlp_5']
    # titles = ['CDF Coverage MLP 3 bands', 'CDF Coverage MLP 4 bands', 'CDF Coverage MLP 5 bands']
    # for variable, title in zip(coverage_variables, titles):
    #     coverage = np.ma.squeeze(dataset.variables[variable][0, :, :])
    #     coverage = coverage * 100
    #     fig, ax = start_full_figure()
    #     h = ax.pcolormesh(lon_array, lat_array, coverage, vmin=0, vmax=100, cmap=mpl.colormaps['jet'])
    #     cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    #     cbar.ax.tick_params(labelsize=15)
    #     cbar.set_label(label=f'Coverage(%)', size=15)
    #     ax.set_title(title)
    #     file_out = os.path.join(os.path.dirname(file_coverage), f'{variable.upper()}.png')
    #     fig.savefig(file_out, dpi=300, bbox_inches='tight')
    #     plt.close(fig)

    # variables = ['mrpd_cdf', 'mrpd_nocdf', 'mrpd', 'mrpd_cdf_complete']
    # titles = [f'Mean RPD (%) - Only CDF', f'Mean RPD (%) - Only NO CDF', f'Mean RPD (%) - All data',
    #           f'Mean RPD(%) - Only CDF Complete']
    # for variable, title in zip(variables, titles):
    #     array = dataset.variables[variable][:]
    #
    #     # ranged color maps
    #     bounds = [-1000, -50, -40, -30, -20, -10, -5, 0, 5, 10, 20, 30, 40, 50, 1000]
    #     norm = colors.BoundaryNorm(boundaries=bounds, ncolors=len(bounds) + 1)
    #     cmap_r = mpl.colormaps['jet'].resampled(len(bounds) + 1)
    #     # continous color maps
    #     cmap_c = mpl.colormaps['jet']
    #     fig, ax = start_full_figure()
    #     # continued
    #     # h = ax.pcolormesh(lon_array, lat_array, array, vmin=-50, vmax=50, cmap=cmap_c)
    #     # range
    #     h = ax.pcolormesh(lon_array, lat_array, array, norm=norm, cmap=cmap_r)
    #
    #     cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    #     cbar.ax.tick_params(labelsize=15)
    #     cbar.set_label(label=f'RPD(%)', size=15)
    #     ax.set_title(title)
    #     file_out = os.path.join(os.path.dirname(file_coverage), f'{variable.upper()}.png')
    #     fig.savefig(file_out, dpi=300, bbox_inches='tight')
    #     plt.close(fig)
    #
    # variables = ['mapd_cdf', 'mapd_nocdf', 'mapd', 'mapd_cdf_complete']
    # titles = [f'Mean APD (%) - Only CDF', f'Mean APD (%) - Only NO CDF', f'Mean APD (%) - All data',
    #           f'Mean APD(%) - Only CDF Complete']
    # for variable, title in zip(variables, titles):
    #     array = dataset.variables[variable][:]
    #     # ranged color maps
    #     bounds = [0, 5, 10, 20, 30, 40, 50, 1000]
    #     norm = colors.BoundaryNorm(boundaries=bounds, ncolors=len(bounds) + 1)
    #     cmap_r = mpl.colormaps['jet'].resampled(len(bounds) + 1)
    #     # continous color maps
    #     cmap_c = mpl.colormaps['jet']
    #     fig, ax = start_full_figure()
    #     # continued
    #     # h = ax.pcolormesh(lon_array, lat_array, array, vmin=-50, vmax=50, cmap=cmap_c)
    #     # range
    #     h = ax.pcolormesh(lon_array, lat_array, array, norm=norm, cmap=cmap_r)
    #
    #     cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    #     cbar.ax.tick_params(labelsize=15)
    #     cbar.set_label(label=f'APD(%)', size=15)
    #     ax.set_title(title)
    #     file_out = os.path.join(os.path.dirname(file_coverage), f'{variable.upper()}.png')
    #     fig.savefig(file_out, dpi=300, bbox_inches='tight')
    #     plt.close(fig)

    dataset.close()
    return True


def plot_map_general(file_nc, file_out, variable, title, label, vmin, vmax):
    dataset = Dataset(file_nc)
    lat_array = dataset.variables['lat'][:]
    lon_array = dataset.variables['lon'][:]
    data = dataset.variables[variable][:]
    print(file_out, np.ma.min(data), np.ma.max(data))
    dataset.close()

    fig, ax = start_full_figure()
    if vmin is not None and vmax is not None:
        h = ax.pcolormesh(lon_array, lat_array, data, vmin=vmin, vmax=vmax, cmap=mpl.colormaps['jet'])
    else:
        h = ax.pcolormesh(lon_array, lat_array, data, cmap=mpl.colormaps['jet'])
    cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    cbar.set_label(label=label, size=15)
    ax.set_title(title)
    fig.savefig(file_out, dpi=300, bbox_inches='tight')
    plt.close(fig)

def compute_total_coverage_cci():
    print(f'COMPUTING TOTAL COVERAGE....')
    dir_base = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CODE_AND_MAP_ANALYSIS_202411/COVERAGE_ENS_YEAR'
    sum_variables = ['n_cdf_total','n_nocdf_total','n_total','weight_mlp3b_cdf','weight_mlp3b_total','weight_mlp4b_cdf','weight_mlp5b_cdf']
    coverage_variables = ['coverage_cdf','coverage_cdf_mlp_3','coverage_cdf_mlp_4','coverage_cdf_mlp_5','coverage_no_cdf','coverage_total_mlp3','coverage_total_mlp4','coverage_total_mlp5']
    file_out = os.path.join(dir_base,'COVERAGE_ENS_CDF_ALL_YEARS.nc')
    nlat = 1147
    nlon = 1185
    ntime = 13
    print('[INFO] Creating output file: ')
    nc_out = Dataset(file_out, 'w')
    nc_out.createDimension('lat', nlat)
    nc_out.createDimension('lon', nlon)
    nc_out.createDimension('time', 13)

    nc_out.createVariable('lat', 'f4', ('lat',), zlib=True, complevel=6, fill_value=-999.0)
    nc_out.createVariable('lon', 'f4', ('lon',), zlib=True, complevel=6, fill_value=-999.0)
    var_time = nc_out.createVariable('time', 'i4', ('time',), zlib=True, complevel=6, fill_value=-999.0)
    var_time[:] = np.arange(13).astype(np.int32)
    var_time.units = f'0 is all the year, 1-12 for each month.'

    for name_var in sum_variables:
        nc_out.createVariable(name_var, 'f4', ('time', 'lat', 'lon'), zlib=True, complevel=6, fill_value=-999.0)
    for name_var in coverage_variables:
        nc_out.createVariable(name_var, 'f4', ('time', 'lat', 'lon'), zlib=True, complevel=6, fill_value=-999.0)

    for idx,var in enumerate(sum_variables):

        var = sum_variables[idx]
        print(f'[INFO] Variable {var} started...')
        array_sum = np.zeros((ntime, nlat, nlon))
        for year in range(1997,2024):
            print(f'[INFO] -> {year}')
            file_year = os.path.join(dir_base,f'COVERAGE_ENSCDF_{year}.nc')
            dataset_y = Dataset(file_year)
            if idx == 0 and year==1997:
                nc_out.variables['lat'][:] = dataset_y.variables['lat'][:]
                nc_out.variables['lon'][:] = dataset_y.variables['lon'][:]
            array_y = dataset_y.variables[var][:]
            dataset_y.close()
            array_sum = array_sum + np.ma.filled(array_y,0)
        nc_out.variables[var][:] = array_sum[:]
        print(f'[INFO] Variable {var} completed')

    print(f'[INFO] Setting coverage...')
    coverage_cdf = np.ma.masked_all((ntime, nlat, nlon))
    coverage_no_cdf = np.ma.masked_all((ntime, nlat, nlon))
    n_total = nc_out.variables['n_total'][:]
    n_cdf_total = nc_out.variables['n_cdf_total'][:]
    n_nocdf_total = nc_out.variables['n_nocdf_total'][:]
    coverage_cdf[np.where(n_total > 0)] = n_cdf_total[np.where(n_total > 0)] / n_total[np.where(n_total > 0)]
    coverage_no_cdf[np.where(n_total > 0)] = n_nocdf_total[np.where(n_total > 0)] / n_total[np.where(n_total > 0)]

    coverage_cdf_mlp3 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_cdf_mlp4 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_cdf_mlp5 = np.ma.masked_all((ntime, nlat, nlon))
    weight_mlp3b_cdf = nc_out.variables['weight_mlp3b_cdf'][:]
    weight_mlp4b_cdf = nc_out.variables['weight_mlp4b_cdf'][:]
    weight_mlp5b_cdf = nc_out.variables['weight_mlp5b_cdf'][:]

    coverage_cdf_mlp3[np.where(n_cdf_total > 0)] = weight_mlp3b_cdf[np.where(n_cdf_total > 0)] / n_cdf_total[
        np.where(n_cdf_total > 0)]
    coverage_cdf_mlp4[np.where(n_cdf_total > 0)] = weight_mlp4b_cdf[np.where(n_cdf_total > 0)] / n_cdf_total[
        np.where(n_cdf_total > 0)]
    coverage_cdf_mlp5[np.where(n_cdf_total > 0)] = weight_mlp5b_cdf[np.where(n_cdf_total > 0)] / n_cdf_total[
        np.where(n_cdf_total > 0)]

    coverage_total_mlp3 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_total_mlp4 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_total_mlp5 = np.ma.masked_all((ntime, nlat, nlon))
    weight_mlp3b_total = nc_out.variables['weight_mlp3b_total'][:]
    coverage_total_mlp3[np.where(n_total > 0)] = weight_mlp3b_total[np.where(n_total > 0)] / n_total[
        np.where(n_total > 0)]
    coverage_total_mlp4[np.where(n_total > 0)] = weight_mlp4b_cdf[np.where(n_total > 0)] / n_total[
        np.where(n_total > 0)]
    coverage_total_mlp5[np.where(n_total > 0)] = weight_mlp5b_cdf[np.where(n_total > 0)] / n_total[
        np.where(n_total > 0)]



    nc_out.variables['n_total'][:] = np.ma.masked_equal(n_total, 0)
    nc_out.variables['n_cdf_total'][:] = np.ma.masked_equal(n_cdf_total, 0)
    nc_out.variables['n_nocdf_total'][:] = np.ma.masked_equal(n_nocdf_total, 0)
    nc_out.variables['weight_mlp3b_cdf'][:] = np.ma.masked_equal(weight_mlp3b_cdf, 0)
    nc_out.variables['weight_mlp4b_cdf'][:] = np.ma.masked_equal(weight_mlp4b_cdf, 0)
    nc_out.variables['weight_mlp5b_cdf'][:] = np.ma.masked_equal(weight_mlp5b_cdf, 0)
    nc_out.variables['weight_mlp3b_total'][:] = np.ma.masked_equal(weight_mlp3b_total, 0)

    coverage_variables = ['coverage_cdf', 'coverage_cdf_mlp_3', 'coverage_cdf_mlp_4', 'coverage_cdf_mlp_5',
                          'coverage_no_cdf', 'coverage_total_mlp3', 'coverage_total_mlp4', 'coverage_total_mlp5']
    nc_out.variables['coverage_cdf'][:] = coverage_cdf[:]
    nc_out.variables['coverage_no_cdf'][:] = coverage_no_cdf[:]
    nc_out.variables['coverage_cdf_mlp_3'][:] = coverage_cdf_mlp3
    nc_out.variables['coverage_cdf_mlp_4'][:] = coverage_cdf_mlp4
    nc_out.variables['coverage_cdf_mlp_5'][:] = coverage_cdf_mlp5
    nc_out.variables['coverage_total_mlp3'][:] = coverage_total_mlp3
    nc_out.variables['coverage_total_mlp4'][:] = coverage_total_mlp4
    nc_out.variables['coverage_total_mlp5'][:] = coverage_total_mlp5

    nc_out.close()

    print(f'[INFO] Completed')


def compute_year_coverage_cci(year):
    print(f'COMPUTING COVERAGE FOR YEAR....')
    from datetime import timedelta
    dir_base = '/store3/OC/CCI_v2017/daily_v202411'
    file_out = os.path.join(dir_base, f'COVERAGE_ENSCDF_{year}.nc')

    file_in_format = 'MDATE1.0000.bal.all_products.CCI.DATE20000.v0.DATE10000.data_BAL202411.nc'
    format_date1 = '%Y%j'
    format_date2 = '%d%b%y'
    nlat = 1147
    nlon = 1185
    ntime = 13
    date_here = dt(year, 1, 1)
    date_end = dt(year, 12, 31)

    ##arrays
    n_total = np.zeros((ntime, nlat, nlon))
    n_cdf_total = np.zeros((ntime, nlat, nlon))
    n_nocdf_total = np.zeros((ntime, nlat, nlon))
    weight_mlp3b_cdf = np.zeros((ntime, nlat, nlon))
    weight_mlp4b_cdf = np.zeros((ntime, nlat, nlon))
    weight_mlp5b_cdf = np.zeros((ntime, nlat, nlon))

    n_mlp_3b = np.zeros((ntime, nlat, nlon))
    n_mlp_4b = np.zeros((ntime, nlat, nlon))
    n_mlp_5b = np.zeros((ntime, nlat, nlon))
    n_mlp_3b4b = np.zeros((ntime,nlat,nlon))
    n_mlp_3b5b = np.zeros((ntime, nlat, nlon))
    n_mlp_4b5b = np.zeros((ntime, nlat, nlon))
    n_mlp_3b4b5b = np.zeros((ntime,nlat,nlon))

    coverage_mlp_3b = np.zeros((ntime, nlat, nlon))
    coverage_mlp_4b = np.zeros((ntime, nlat, nlon))
    coverage_mlp_5b = np.zeros((ntime, nlat, nlon))
    coverage_mlp_3b4b = np.zeros((ntime, nlat, nlon))
    coverage_mlp_3b5b = np.zeros((ntime, nlat, nlon))
    coverage_mlp_4b5b = np.zeros((ntime, nlat, nlon))
    coverage_mlp_3b4b5b = np.zeros((ntime, nlat, nlon))


    coverage_cdf = np.ma.masked_all((ntime, nlat, nlon))
    coverage_no_cdf = np.ma.masked_all((ntime, nlat, nlon))
    coverage_cdf_mlp3 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_cdf_mlp4 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_cdf_mlp5 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_total_mlp3 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_total_mlp4 = np.ma.masked_all((ntime, nlat, nlon))
    coverage_total_mlp5 =np.ma.masked_all((ntime, nlat, nlon))

    lat = None
    lon = None

    while date_here <= date_end:
        yyyy = date_here.strftime('%Y')
        jjj = date_here.strftime('%j')
        date1_str = date_here.strftime(format_date1)
        date2_str = date_here.strftime(format_date2)
        name_file = file_in_format.replace('DATE1', date1_str)
        name_file = name_file.replace('DATE2', date2_str)
        file_in = os.path.join(dir_base, yyyy, jjj, name_file)
        index_month = int(date_here.month)
        if not os.path.exists(file_in):
            print(f'[WARNING] Input file {file_in} does not exist. Skipping...')
            date_here = date_here + timedelta(hours=24)
            continue

        print(f'[INFO] Date: {date_here}')
        input_dataset = Dataset(file_in)
        if lat is None and lon is None:
            lat = input_dataset.variables['lat'][:]
            lon = input_dataset.variables['lon'][:]

        weight_mlp3 = input_dataset.variables['WEIGHT_MLP3B'][:]
        weight_mlp4 = input_dataset.variables['WEIGHT_MLP4B'][:]
        weight_mlp5 = input_dataset.variables['WEIGHT_MLP5B'][:]
        cdf_flag = input_dataset.variables['CDF_FLAG_MULTIPLE'][:]


        row,col = np.where(cdf_flag>=1)
        n_total[0, row, col] = n_total[0, row, col] + 1
        n_total[index_month, row, col] = n_total[index_month, row, col] + 1

        row, col = np.where(cdf_flag >= 2)
        n_cdf_total[0, row, col] = n_cdf_total[0, row, col] + 1
        n_cdf_total[index_month, row, col] = n_cdf_total[index_month, row, col] + 1

        row, col = np.where(cdf_flag == 1)
        n_nocdf_total[0, row, col] = n_nocdf_total[0, row, col] + 1
        n_nocdf_total[index_month, row, col] = n_nocdf_total[index_month, row, col] + 1

        row, col = np.where(cdf_flag == 2)
        n_mlp_3b[0,row,col] = n_mlp_3b[0,row,col]+1
        n_mlp_3b[index_month, row, col] = n_mlp_3b[index_month, row, col] + 1
        weight_mlp3b_cdf[0, row,col] = weight_mlp3b_cdf[0,row,col] + weight_mlp3[row,col]
        weight_mlp3b_cdf[index_month, row, col] = weight_mlp3b_cdf[index_month, row, col] + weight_mlp3[row, col]


        row, col = np.where(cdf_flag == 4)
        n_mlp_4b[0, row, col] = n_mlp_4b[0, row, col] + 1
        n_mlp_4b[index_month, row, col] = n_mlp_4b[index_month, row, col] + 1
        weight_mlp4b_cdf[0, row, col] = weight_mlp4b_cdf[0, row, col] + weight_mlp4[row, col]
        weight_mlp4b_cdf[index_month, row, col] = weight_mlp4b_cdf[index_month, row, col] + weight_mlp4[row, col]

        row, col = np.where(cdf_flag == 8)
        n_mlp_5b[0, row, col] = n_mlp_5b[0, row, col] + 1
        n_mlp_5b[index_month, row, col] = n_mlp_5b[index_month, row, col] + 1
        weight_mlp5b_cdf[0, row, col] = weight_mlp5b_cdf[0, row, col] + weight_mlp5[row, col]
        weight_mlp5b_cdf[index_month, row, col] = weight_mlp5b_cdf[index_month, row, col] + weight_mlp5[row, col]

        row, col = np.where(cdf_flag == 6)
        n_mlp_3b4b[0, row, col] = n_mlp_3b4b[0, row, col] + 1
        n_mlp_3b4b[index_month, row, col] = n_mlp_3b4b[index_month, row, col] + 1
        weight_mlp3b_cdf[0, row, col] = weight_mlp3b_cdf[0, row, col] + weight_mlp3[row, col]
        weight_mlp3b_cdf[index_month, row, col] = weight_mlp3b_cdf[index_month, row, col] + weight_mlp3[row, col]
        weight_mlp4b_cdf[0, row, col] = weight_mlp4b_cdf[0, row, col] + weight_mlp4[row, col]
        weight_mlp4b_cdf[index_month, row, col] = weight_mlp4b_cdf[index_month, row, col] + weight_mlp4[row, col]


        row, col = np.where(cdf_flag == 10)
        n_mlp_3b5b[0, row, col] = n_mlp_3b5b[0, row, col] + 1
        n_mlp_3b5b[index_month, row, col] = n_mlp_3b5b[index_month, row, col] + 1
        weight_mlp3b_cdf[0, row, col] = weight_mlp3b_cdf[0, row, col] + weight_mlp3[row, col]
        weight_mlp3b_cdf[index_month, row, col] = weight_mlp3b_cdf[index_month, row, col] + weight_mlp3[row, col]
        weight_mlp5b_cdf[0, row, col] = weight_mlp5b_cdf[0, row, col] + weight_mlp5[row, col]
        weight_mlp5b_cdf[index_month, row, col] = weight_mlp5b_cdf[index_month, row, col] + weight_mlp5[row, col]

        row, col = np.where(cdf_flag == 12)
        n_mlp_4b5b[0, row, col] = n_mlp_4b5b[0, row, col] + 1
        n_mlp_4b5b[index_month, row, col] = n_mlp_4b5b[index_month, row, col] + 1
        weight_mlp4b_cdf[0, row, col] = weight_mlp4b_cdf[0, row, col] + weight_mlp4[row, col]
        weight_mlp4b_cdf[index_month, row, col] = weight_mlp4b_cdf[index_month, row, col] + weight_mlp4[row, col]
        weight_mlp5b_cdf[0, row, col] = weight_mlp5b_cdf[0, row, col] + weight_mlp5[row, col]
        weight_mlp5b_cdf[index_month, row, col] = weight_mlp5b_cdf[index_month, row, col] + weight_mlp5[row, col]

        row, col = np.where(cdf_flag == 14)
        n_mlp_3b4b5b[0, row, col] = n_mlp_3b4b5b[0, row, col] + 1
        n_mlp_3b4b5b[index_month, row, col] = n_mlp_3b4b5b[index_month, row, col] + 1
        weight_mlp3b_cdf[0, row, col] = weight_mlp3b_cdf[0, row, col] + weight_mlp3[row, col]
        weight_mlp3b_cdf[index_month, row, col] = weight_mlp3b_cdf[index_month, row, col] + weight_mlp3[row, col]
        weight_mlp4b_cdf[0, row, col] = weight_mlp4b_cdf[0, row, col] + weight_mlp4[row, col]
        weight_mlp4b_cdf[index_month, row, col] = weight_mlp4b_cdf[index_month, row, col] + weight_mlp4[row, col]
        weight_mlp5b_cdf[0, row, col] = weight_mlp5b_cdf[0, row, col] + weight_mlp5[row, col]
        weight_mlp5b_cdf[index_month, row, col] = weight_mlp5b_cdf[index_month, row, col] + weight_mlp5[row, col]


        input_dataset.close()

        date_here = date_here + timedelta(hours=24)


    print('[INFO] Output shapes: ')
    print(f'--> n_total: {n_total.shape}')
    print(f'--> n_cdf_total: {n_cdf_total.shape}')
    print(f'--> n_nocdf_total: {n_nocdf_total.shape}')
    print(f'--> weight_mlp_3b: {weight_mlp3b_cdf.shape}')
    print(f'--> weight_mlp_4b: {weight_mlp4b_cdf.shape}')
    print(f'--> weight_mlp_5b: {weight_mlp5b_cdf.shape}')

    coverage_cdf[np.where(n_total>0)] = n_cdf_total[np.where(n_total>0)]/n_total[np.where(n_total>0)]
    coverage_no_cdf[np.where(n_total>0)] = n_nocdf_total[np.where(n_total>0)]/n_total[np.where(n_total>0)]
    print(f'--> coverage_cdf: {coverage_cdf.shape}')
    print(f'--> coverage_no_cdf: {coverage_no_cdf.shape}')

    ##coverage of each cdf flag with respect to the total cdf
    coverage_mlp_3b[np.where(n_cdf_total > 0)] = n_mlp_3b[np.where(n_cdf_total > 0)] / n_cdf_total[np.where(n_cdf_total > 0)]
    coverage_mlp_4b[np.where(n_cdf_total > 0)] = n_mlp_4b[np.where(n_cdf_total > 0)] / n_cdf_total[np.where(n_cdf_total > 0)]
    coverage_mlp_5b[np.where(n_cdf_total > 0)] = n_mlp_5b[np.where(n_cdf_total > 0)] / n_cdf_total[np.where(n_cdf_total > 0)]
    coverage_mlp_3b4b[np.where(n_cdf_total > 0)] = n_mlp_3b4b[np.where(n_cdf_total > 0)] / n_cdf_total[np.where(n_cdf_total > 0)]
    coverage_mlp_3b5b[np.where(n_cdf_total > 0)] = n_mlp_3b5b[np.where(n_cdf_total > 0)] / n_cdf_total[np.where(n_cdf_total > 0)]
    coverage_mlp_4b5b[np.where(n_cdf_total > 0)] = n_mlp_4b5b[np.where(n_cdf_total > 0)] / n_cdf_total[np.where(n_cdf_total > 0)]
    coverage_mlp_3b4b5b[np.where(n_cdf_total > 0)] = n_mlp_3b4b5b[np.where(n_cdf_total > 0)] / n_cdf_total[np.where(n_cdf_total > 0)]


    coverage_cdf_mlp3[np.where(n_cdf_total > 0)] = weight_mlp3b_cdf[np.where(n_cdf_total > 0)]/ n_cdf_total[np.where(n_cdf_total > 0)]
    coverage_cdf_mlp4[np.where(n_cdf_total > 0)] = weight_mlp4b_cdf[np.where(n_cdf_total > 0)]/ n_cdf_total[np.where(n_cdf_total > 0)]
    coverage_cdf_mlp5[np.where(n_cdf_total > 0)] = weight_mlp5b_cdf[np.where(n_cdf_total > 0)]/ n_cdf_total[np.where(n_cdf_total > 0)]
    print(f'--> coverage_cdf_mlp3: {coverage_cdf_mlp3.shape}')
    print(f'--> coverage_cdf_mlp4: {coverage_cdf_mlp4.shape}')
    print(f'--> coverage_cdf_mlp5: {coverage_cdf_mlp5.shape}')

    weight_mlp3b_total = n_nocdf_total.copy()
    print(f'--> weight_mlp3b_total (start): {weight_mlp3b_total.shape}')
    weight_mlp3b_total[np.where(n_nocdf_total == 0)] = weight_mlp3b_cdf[np.where(n_nocdf_total==0)]
    weight_mlp3b_total[np.where(n_nocdf_total > 0)] = weight_mlp3b_cdf[np.where(n_nocdf_total > 0)]+ weight_mlp3b_total[np.where(n_nocdf_total > 0)]
    print(f'--> weight_mlp3b_total (end): {weight_mlp3b_total.shape}')
    coverage_total_mlp3[np.where(n_total > 0)] = weight_mlp3b_total[np.where(n_total > 0)]/ n_total[np.where(n_total > 0)]
    coverage_total_mlp4[np.where(n_total > 0)] = weight_mlp4b_cdf[np.where(n_total > 0)]/ n_total[np.where(n_total > 0)]
    coverage_total_mlp5[np.where(n_total > 0)] = weight_mlp5b_cdf[np.where(n_total > 0)]/ n_total[np.where(n_total > 0)]
    print(f'--> coverage_total_mlp3: {coverage_total_mlp3.shape}')
    print(f'--> coverage_total_mlp4: {coverage_total_mlp4.shape}')
    print(f'--> coverage_total_mlp5: {coverage_total_mlp5.shape}')


    n_total = np.ma.masked_equal(n_total,0)
    n_cdf_total = np.ma.masked_equal(n_cdf_total,0)
    n_nocdf_total = np.ma.masked_equal(n_nocdf_total,0)
    weight_mlp3b_cdf = np.ma.masked_equal(weight_mlp3b_cdf,0)
    weight_mlp4b_cdf = np.ma.masked_equal(weight_mlp4b_cdf,0)
    weight_mlp5b_cdf = np.ma.masked_equal(weight_mlp5b_cdf,0)
    weight_mlp3b_total = np.ma.masked_equal(weight_mlp3b_total, 0)

    print('[INFO] Creating output file: ')
    nc_out = Dataset(file_out,'w')
    nc_out.createDimension('lat',nlat)
    nc_out.createDimension('lon',nlon)
    nc_out.createDimension('time',13)

    var_lat = nc_out.createVariable('lat','f4',('lat',),zlib=True,complevel=6,fill_value=-999.0)
    var_lat[:] = lat[:]
    var_lon = nc_out.createVariable('lon', 'f4', ('lon',), zlib=True, complevel=6, fill_value=-999.0)
    var_lon[:] = lon[:]
    var_time = nc_out.createVariable('time', 'i4', ('time',), zlib=True, complevel=6, fill_value=-999.0)
    var_time[:] = np.arange(13).astype(np.int32)
    var_time.units = f'0 is all the year, 1-12 for each month. Year: {year}'

    list_variables = {
        'n_total': n_total,
        'n_cdf_total': n_cdf_total,
        'n_nocdf_total': n_nocdf_total,
        'weight_mlp3b_cdf' : weight_mlp3b_cdf,
        'weight_mlp4b_cdf' : weight_mlp4b_cdf,
        'weight_mlp5b_cdf' : weight_mlp5b_cdf,
        'weight_mlp3b_total' : weight_mlp3b_total,
        'coverage_cdf': coverage_cdf,
        'coverage_no_cdf': coverage_no_cdf,
        'coverage_cdf_mlp3': coverage_cdf_mlp3,
        'coverage_cdf_mlp4' : coverage_cdf_mlp4,
        'coverage_cdf_mlp5' :coverage_cdf_mlp5,
        'coverage_total_mlp3' :coverage_total_mlp3,
        'coverage_total_mlp4' :coverage_total_mlp4,
        'coverage_total_mlp5' :coverage_total_mlp5,
        'n_mlp_3b': n_mlp_3b,
        'n_mlp_4b': n_mlp_4b,
        'n_mlp_5b': n_mlp_5b,
        'n_mlp_3b4b': n_mlp_3b4b,
        'n_mlp_3b5b': n_mlp_3b5b,
        'n_mlp_4b5b': n_mlp_4b5b,
        'n_mlp_3b4b5b': n_mlp_3b4b5b,
        'coverage_mlp_3b': coverage_mlp_3b,
        'coverage_mlp_4b': coverage_mlp_4b,
        'coverage_mlp_5b': coverage_mlp_5b,
        'coverage_mlp_3b4b': coverage_mlp_3b4b,
        'coverage_mlp_3b5b': coverage_mlp_3b5b,
        'coverage_mlp_4b5b': coverage_mlp_4b5b,
        'coverage_mlp_3b4b5b': coverage_mlp_3b4b5b
    }
    for name_var in list_variables:
        array = list_variables[name_var]
        var = nc_out.createVariable(name_var,'f4',('time','lat','lon'),zlib=True,complevel=6,fill_value=-999.0)
        var[:] = array[:]

    nc_out.close()

    return True

##period: 'all' (all year) or 'cyano' (days 161 to 270)
def compute_year_cyano(dir_base,year,period,file_mask):

    print(f'YEAR: {year}')

    mask = None
    if os.path.exists(file_mask):
        dmask = Dataset(file_mask)
        mask = dmask.variables['Land_Mask'][:]
        dmask.close()

    file_out = os.path.join(dir_base, f'CYANOBLOOM_COVERAGE_{year}.nc')
    file_nc = os.path.join(dir_base, f'CYANOBLOOM_{year}.nc')
    dataset = Dataset(file_nc)
    timec = dataset.variables['time'][:]
    cbloom = dataset.variables['CYANOBLOOM'][:]
    dataset.close()

    ndays = cbloom.shape[0]
    if mask is not None:
        print(f'Applying mask. Starting from: {np.ma.count(cbloom)}')
        for iday in range(ndays):
            cbloom[iday,mask==1] = np.ma.masked
        print(f'to --> {np.ma.count(cbloom)}')


    coverage_valid = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    coverage_sub = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    coverage_surface = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    coverage_both = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    coverage_any = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    p_coverage_sub = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    p_coverage_surface = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    p_coverage_both = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    p_coverage_any = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    first_day = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    last_day = np.zeros((cbloom.shape[1], cbloom.shape[2]))



    if ndays>=365:
        jdays = np.arange(ndays)+1
    else:
        jdays = np.array([int(dt.fromtimestamp(t).strftime('%j')) for t in timec])


    for iday in range(ndays):
        jday = jdays[iday]
        if (jday<161 or jday>270) and period=='cyano':
            continue
        cbloom_jday = np.ma.squeeze(cbloom[iday, :, :])
        cbloom_jday = np.ma.filled(cbloom_jday, -999)
        coverage_valid[cbloom_jday >= 0] = coverage_valid[cbloom_jday >= 0] + 1
        coverage_sub[cbloom_jday == 1] = coverage_sub[cbloom_jday == 1] + 1
        coverage_surface[cbloom_jday == 2] = coverage_surface[cbloom_jday == 2] + 1
        coverage_both[cbloom_jday == 3] = coverage_both[cbloom_jday == 3] + 1
        coverage_any[cbloom_jday >= 1] = coverage_any[cbloom_jday >= 1] + 1
        first_day[np.logical_and(cbloom_jday >= 1, first_day == 0)] = jday
        last_day[cbloom_jday >= 1] = jday


    coverage_valid[coverage_valid==0] = -999
    coverage_sub[coverage_valid==-999]=-999
    coverage_surface[coverage_valid == -999] = -999
    coverage_both[coverage_valid == -999] = -999
    coverage_any[coverage_valid == -999] = -999
    first_day[first_day<=0] = -999
    last_day[last_day<=0] = -999



    p_coverage_sub[coverage_valid != -999] = (coverage_sub[coverage_valid != -999] / coverage_valid[
        coverage_valid != -999]) * 100
    p_coverage_surface[coverage_valid != -999] = (coverage_surface[coverage_valid != -999] / coverage_valid[
        coverage_valid != -999]) * 100
    p_coverage_both[coverage_valid != -999] = (coverage_both[coverage_valid != -999] / coverage_valid[
        coverage_valid != -999]) * 100
    p_coverage_any[coverage_valid != -999] = (coverage_any[coverage_valid != -999] / coverage_valid[
        coverage_valid != -999]) * 100
    p_coverage_sub[coverage_valid == -999] = -999
    p_coverage_surface[coverage_valid == -999] = -999
    p_coverage_both[coverage_valid == -999] = -999
    p_coverage_any[coverage_valid == -999] = -999


    new_variables = {
        'COVERAGE_VALID': coverage_valid,
        'COVERAGE_SUB': coverage_sub,
        'COVERAGE_SURFACE': coverage_surface,
        'COVERAGE_BOTH': coverage_both,
        'COVERAGE_ANY': coverage_any,
        'P_COVERAGE_SUB': p_coverage_sub,
        'P_COVERAGE_SURFACE': p_coverage_surface,
        'P_COVERAGE_BOTH': p_coverage_both,
        'P_COVERAGE_ANY': p_coverage_any,
        'FIRST_DAY': first_day,
        'LAST_DAY': last_day
    }


    shutil.copy(file_nc, file_out)
    ncout = Dataset(file_out, 'a')
    for var_name in new_variables:
        var = ncout.createVariable(var_name, 'f4', ('lat', 'lon'), fill_value=-999.0, zlib=True, complevel=6)
        data = new_variables[var_name]
        var[:] = data
    ncout.close()

    # dtal = Dataset(file_out)
    # fday = dtal.variables['FIRST_DAY'][:]
    # print(np.ma.min(fday))
    # dtal.close()




def get_lines_cyano_csv_from_year_files(dir_base,year):

    print(f'[INFO] Working for year: {year}')
    file_out = os.path.join(dir_base, f'CYANOBLOOM_COVERAGE_{year}.nc')
    line_start = 'Year;CoverageValid;CoverageSub;CoverageSurface;CoverageBoth;CoverageAny;P_CoverageSub;P_Coverage_Surface;P_Coverage_Both;P_Coverage_Any;FirstDay;LastDay'
    line = f'{year}'

    ncout = Dataset(file_out)
    n_valid = ncout.variables['COVERAGE_VALID'][:]
    n_valid_all = np.ma.sum(n_valid)
    for var_name in ncout.variables:
        if var_name == 'lat' or var_name == 'lon' or var_name == 'time' or var_name == 'CYANOBLOOM':
            continue
        if var_name.startswith('COVERAGE'):
            data = ncout.variables[var_name][:]
            line = f'{line};{np.ma.sum(data)}'
        elif var_name.startswith('P_COVERAGE'):
            var_name_c = var_name[2:]
            data = ncout.variables[var_name_c][:]
            value = (np.ma.sum(data)/n_valid_all)*100
            line = f'{line};{value}'
        elif var_name=='FIRST_DAY':
            data = ncout.variables[var_name][:]
            line = f'{line};{np.min(data)}'
        elif var_name=='LAST_DAY':
            data = ncout.variables[var_name][:]
            line = f'{line};{np.max(data)}'
    ncout.close()

    return line_start, line

def finisce():
    return True

def get_options_from_config_file(config_file):
    reader = ConfigReader(config_file,None)

    if not reader.check_section('MAKE_MAPS'):
        print(f'[ERROR] Section MAKE_MAPS in required in the configuration file {os.path.basename(config_file)}')
        return
    compulsory_options = {
        'input_path':{
            'type':'directory_in'
        },
        'variable':{
            'type': 'str'
        },
        'name_file_format':{
            'type': 'str'
        },
        'start_date':{
            'type': 'date'
        },
        'end_date':{
            'type': 'date'
        }
    }

    other_options = {
        'input_path_organization':{
            'type':'str',
            'default': '%Y/%j'
        },
        'date_name_file_format':{
            'type':'str',
            'default': '%Y%j'
        },
        'frequency':{
            'type':'str',
            'default':'d',
            'potential_values': ['d','m']
        }
    }

    opt_dict = reader.retrieve_options('MAKE_MAPS',compulsory_options,other_options)
    return opt_dict

def run_maps(options):
    input_path = options['input_path']
    work_date = options['start_date']
    end_date = options['end_date']
    fr = options['frequency']
    if fr=='m':
        work_date = work_date.replace(day=15)
        end_date = end_date.replace(day=15)

    while work_date<=end_date:

        if fr=='d':
            work_date = work_date + timedelta(hours=24)
        elif fr=='m':
            next_y = work_date.year if work_date.month<12 else (work_date.year+1)
            next_m = (work_date.month+1) if work_date.month<12 else 1
            work_date = dt(next_y,next_m,15)


def test_masks():
    file_mask = '/home/gosuser/Processing/gos-oc-processingchains_v202411/s3olcibalProcessing/BAL_Land_Mask_fr.nc'
    dir_base = '/store3/OC/OLCI_BAL/dailyolci_202411'
    date_ref = dt(1981,1,1)
    # file_mask = '/mnt/c/DATA/BAL_Land_Mask_fr.nc'
    #file_data = '/mnt/c/DATA/O2020102-chl-bal-fr.nc'
    # #file_data = '/mnt/c/DATA/O2020102-chl-bal-fr_nomask.nc'


    dmask = Dataset(file_mask)
    mask_array = dmask.variables['Land_Mask'][:]
    dmask.close()

    work_date = dt(2016,4,26)
    end_date = dt(2025,5,22)
    year_ref = 0
    fw = None
    while work_date<=end_date:
        yyyy = work_date.strftime('%Y')
        jjj = work_date.strftime('%j')
        #file_data = os.path.join(dir_base,yyyy,jjj,f'O{yyyy}{jjj}-chl-bal-fr.nc')
        file_data = os.path.join(dir_base, yyyy, jjj, f'O{yyyy}{jjj}-pft-bal-fr.nc')
        if os.path.exists(file_data):
            if work_date.year!=year_ref:
                if fw is not None:
                    fw.close()
                year_ref = work_date.year
                file_out = os.path.join(dir_base,f'Check_Mask_{year_ref}.csv')
                fw = open(file_out,'w')
                fw.write('Date;NameFile;NInvalid;ValidTime')

            dataset = Dataset(file_data)
            #data = np.ma.squeeze(dataset.variables['CHL'][:])
            data = np.ma.squeeze(dataset.variables['GREEN'][:])
            date_here = date_ref + timedelta(seconds=int(dataset.variables['time'][0]))
            valid_time = 1 if work_date == date_here else 0
            dataset.close()
            data_inmask = data[mask_array==1]
            ninvalid = np.ma.count(data_inmask)
            fw.write('\n')
            fw.write(f'{work_date.strftime("%Y-%m-%d")};O{yyyy}{jjj}-chl-bal-fr.nc;{ninvalid};{valid_time}')

        work_date = work_date + timedelta(hours=24)

    if fw is not None:
        fw.close()


    return True

def plot_ebro(date_here,type_file):
    dir_base = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/NOW/Ebro/SOURCES'
    if type_file=='multi':
        name_file = '$DATE$_cmems_obs-oc_med_bgc-plankton_myint_l3-multi-1km_P1D.nc'
    if type_file=='olci':
        name_file = '$DATE$_cmems_obs-oc_med_bgc-plankton_myint_l3-olci-300m_P1D.nc'
    if type_file=='hr':
        name_file = '$DATE$_P1D_CMEMS_HROC_L3-geophy_MED_31T_100m-v01.nc'


    file_here = os.path.join(dir_base,date_here.strftime('%Y/%j'),name_file.replace('$DATE$',date_here.strftime('%Y%m%d')))
    if not os.path.exists(file_here):
        print(f'{file_here} was not found')
        return True

    file_out = os.path.join(os.path.dirname(dir_base),'IMAGES',f'Chla_{type_file}_{date_here.strftime("%Y%m%d")}.png')
    fig, ax = plt.subplots(subplot_kw=dict(projection=ccrs.PlateCarree()))
    fig.set_figwidth(15)
    fig.set_figheight(15)



    # coastlines
    ax.add_feature(cartopy.feature.LAND, zorder=0, edgecolor='black', linewidth=0.5)
    # ax.coastlines(resolution='10m')

    # grid lines
    gl = ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False, linewidth=0.5, linestyle='dotted')
    gl.xlocator = mticker.FixedLocator([0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2])
    gl.ylocator = mticker.FixedLocator([40, 40.2, 40,4, 40.6, 40.8, 41])
    gl.right_labels = False
    gl.left_labels = True
    gl.bottom_labels = True
    gl.top_labels = False
    gl.xlabel_style = {'size': 15}
    gl.ylabel_style = {'size': 15}

    ##data
    limits = [40, 41, 0, 1.2]
    dataset = Dataset(file_here)
    array_lat = dataset.variables['lat'][:]
    array_lon = dataset.variables['lon'][:]
    r1, r2, c1, c2  = get_geo_limits(array_lat,array_lon,limits)

    data = np.ma.squeeze(dataset.variables['CHL'][:])
    dataset.close()
    if type_file=='hr':
        lat_here = array_lat[r2:r1]
        lon_here = array_lon[c1:c2]
        date_here = data_here = data[r2:r1,c1:c2]
    else:
        lat_here = array_lat[r1:r2]
        lon_here = array_lon[c1:c2]
        data_here = data[r1:r2,c1:c2]
    print('lat',lat_here.shape)
    print('lon',lon_here.shape)
    print('data',data_here.shape)

    h = ax.pcolormesh(lon_here, lat_here, data_here, norm=LogNorm(vmin=0.01, vmax=10))
    cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    units = r'mg m$^-$$^3$'
    cbar.set_label(label=f'CHL ({units})', size=15)
    title = f'Chlorophyll a concentration ({units})'
    plt.title(title)
    plt.tight_layout()
    fig.savefig(file_out, dpi=300, bbox_inches='tight')
    plt.close(fig)

    return True

def get_geo_limits(array_lat,array_lon,geo_limits):
    r1 = np.argmin(np.abs(array_lat-geo_limits[0]))
    r2 = np.argmin(np.abs(array_lat - geo_limits[1]))
    c1 = np.argmin(np.abs(array_lon - geo_limits[2]))
    c2 = np.argmin(np.abs(array_lon - geo_limits[3]))

    return r1, r2, c1, c2

def main():
    if args.config_path:
        if not os.path.isfile(args.config_path):
            print(f'[ERROR] Config file {args.config_path} does not exist or is not a valid file')
            return
        options = get_options_from_config_file(args.config_path)
        run_maps(options)
        return
    #if test_masks():
        #return

    # input_date = dt.strptime(args.input_path,'%Y-%m-%d')
    # type = args.output_path
    # if plot_ebro(input_date,type):
    #     return
    # if plot_coverage():
    #     return
    # if compare_old_new_v2():
    #     return
    # if compute_diff():
    #     return

    ##CREATE FILES CYANOBLOOM_COVERAGE
    # dir_base = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CYANOBLOOM_EVOLUTION'
    # file_mask = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/MASKS/BAL_Land_Mask_hr.nc'
    # for year in range(1997,2025):
    #     compute_year_cyano(dir_base,year,'cyano',file_mask)



    ##CREATE CSV FILE FOR CYANO YEAR FILES
    # dir_base = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CYANOBLOOM_EVOLUTION'
    # fcsv_out = os.path.join(dir_base, 'CyanoEvolution_CyanoPeriod.csv')
    # fw = open(fcsv_out, 'w')
    # started = False
    # for year in range(1997,2025):
    #     line_start,line = get_lines_cyano_csv_from_year_files(dir_base,year)
    #     if not started:
    #         fw.write(line_start)
    #         fw.write('\n')
    #         fw.write(line)
    #         started = True
    #     else:
    #         fw.write('\n')
    #         fw.write(line)
    # fw.close()

    ##CREATE MAPS CYANOBLOOM_COVERAGE
    # dir_base = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CYANOBLOOM_EVOLUTION'
    # variable = 'COVERAGE_VALID'
    # label = '#Days'
    # vmin = 0
    # vmax = 366
    # for year in range(2024, 2025):
    #     file_nc = os.path.join(dir_base, f'CYANOBLOOM_COVERAGE_{year}.nc')
    #     title = f'{variable}-{year}'
    #     file_out = os.path.join(os.path.dirname(file_nc), f'{variable}_{year}.tif')
    #     plot_map_general(file_nc,file_out,variable,title,label,vmin,vmax)

    ##CDF ENSEMBLE COVERAGE FOR EACH YEAR (WITH TOTAL AND MONTHLY RESULTS) BASED ON CCI DAILY DATA1 - RUN ON HPC-SERVERS
    # for year in range(1997,2025):
    #   compute_year_coverage_cci(year)

    ##TOTAL CDF ENSEMBLE COVERAGE BASED ON YEAR COVERAGE FILES. IT INCLUCES TOTAL AND MONTHLY RESULTS - LOCAL RUN
    compute_total_coverage_cci()


    # file_check = '/mnt/c/Users/LuisGonzalez/OneDrive - NOLOGIN OCEANIC WEATHER SYSTEMS S.L.U/CNR/OCTAC_WORK/BAL_EVOLUTION_202411/CYANOBLOOM_EVOLUTION/CYANOBLOOM_COVERAGE_2023.nc'
    # dataset = Dataset(file_check)
    # cbloom = dataset.variables['CYANOBLOOM'][:]
    # coverage_valid = np.zeros((cbloom.shape[1], cbloom.shape[2]))
    # ndays = cbloom.shape[0]
    # for iday in range(ndays):
    #     jday = iday + 1
    # # for jday in range(161,271,1):
    # #     iday = jday - 1
    #     cbloom_jday = np.ma.squeeze(cbloom[iday, :, :])
    #     cbloom_jday = np.ma.filled(cbloom_jday, -999)
    #     coverage_valid[cbloom_jday >= 0] = coverage_valid[cbloom_jday >= 0] + 1
    #
    # coverage_valid_prev = dataset.variables['COVERAGE_VALID'][:]
    # coverage_valid = np.ma.masked_equal(coverage_valid,0)
    # print(coverage_valid.shape,coverage_valid_prev.shape)
    # print(np.ma.count(coverage_valid),np.ma.count(coverage_valid_prev))
    #
    # dataset.close()


    if finisce():
        return

    input_path = args.input_path

    if os.path.isdir(input_path):
        print(f'[INFO] Input path directory: {input_path}')
        launch_multiple_maps(input_path)
    else:
        try:
            print(f'[INFO] Input path: {input_path}')
            dataset = Dataset(input_path)
        except:
            print(f'[ERROR] Input path is not valid')
            return
        if args.output_path:
            output_path = args.output_path
        else:
            output_path = input_path.replace('.nc', '.png')
        dateherestr, type = get_date_here_from_file_name(input_path)
        if type == 'diff':  ##Daily diff
            name = os.path.basename(input_path)
            datestr = name[5:12]
            dateherestr = dt.strptime(datestr, '%Y%j').strftime('%Y%m%d')
            launch_single_map_daily_diff(dataset, output_path, dateherestr)
        elif type == 'cci':
            launch_single_map_cci(dataset, output_path, dateherestr)
        elif type == 'olci':
            launch_single_map_olci(dataset, output_path, dateherestr)


def launch_multiple_maps(input_dir):
    for name in os.listdir(input_dir):
        if not name.endswith('.nc') or name.startswith('Coverage'):
            continue
        input_path = os.path.join(input_dir, name)
        date_here_str, type = get_date_here_from_file_name(input_path)

        if type == 'cci':
            try:
                print(f'[INFO] Input path: {input_path}')
                dataset = Dataset(input_path)
            except:
                continue
            output_path = input_path.replace('.nc', '.png')
            launch_single_map_cci(dataset, output_path, date_here_str)
        elif type == 'olci':
            try:
                print(f'[INFO] Input path: {input_path}')
                dataset = Dataset(input_path)
            except:
                continue
            output_path = input_path.replace('.nc', '.png')
            launch_single_map_olci(dataset, output_path, date_here_str)
        elif type == 'diff':
            if not name.endswith('.nc'):
                continue
            input_path = os.path.join(input_dir, name)
            try:
                print(f'[INFO] Input path: {input_path}')
                dataset = Dataset(input_path)
            except:
                continue
            datestr = name[5:12]
            dateherestr = dt.strptime(datestr, '%Y%j').strftime('%Y%m%d')
            output_path = input_path.replace('.nc', '.png')
            # print(output_path)
            launch_single_map_daily_diff(dataset, output_path, dateherestr)


def launch_single_map_daily_diff(dataset, output_path, dateherestr):
    print(f'[INFO] Output path: {output_path}')

    lat_array = dataset.variables['latitude'][:]
    lon_array = dataset.variables['longitude'][:]

    chl_old = dataset.variables['chl_old'][:]
    fig, ax = start_full_figure()
    h = ax.pcolormesh(lon_array, lat_array, chl_old, norm=LogNorm(vmin=0.1, vmax=100))
    cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    units = r'mg m$^-$$^3$'
    cbar.set_label(label=f'CHL ({units})', size=15)
    title = f'Chlorophyll a concentration - OLD ({units})'
    if dateherestr is not None:
        title = f'{title} - {dateherestr}'
    ax.set_title(title, fontsize=20)
    file_chla_old = os.path.join(os.path.dirname(output_path), f'Img_Chla_OLD_{dateherestr}.png')
    fig.savefig(file_chla_old, dpi=300, bbox_inches='tight')
    plt.close(fig)

    chl_new = dataset.variables['chl_new'][:]
    fig, ax = start_full_figure()
    h = ax.pcolormesh(lon_array, lat_array, chl_new, norm=LogNorm(vmin=0.1, vmax=100))
    cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    units = r'mg m$^-$$^3$'
    cbar.set_label(label=f'CHL ({units})', size=15)
    title = f'Chlorophyll a concentration - NEW ({units})'
    if dateherestr is not None:
        title = f'{title} - {dateherestr}'
    ax.set_title(title, fontsize=20)
    file_chla_new = os.path.join(os.path.dirname(output_path), f'Img_Chla_NEW_{dateherestr}.png')
    fig.savefig(file_chla_new, dpi=300, bbox_inches='tight')
    plt.close(fig)

    chl_dif = dataset.variables['chl_dif'][:]
    fig, ax = start_full_figure()
    h = ax.pcolormesh(lon_array, lat_array, chl_dif, cmap=mpl.colormaps['jet'])
    cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    units = r'mg m$^-$$^3$'
    cbar.set_label(label=f'CHL-Diff', size=15)
    title = f'Chl-a OLD - Chl-a NEW ({units})'
    if dateherestr is not None:
        title = f'{title} - {dateherestr}'
    ax.set_title(title, fontsize=20)
    file_diff = os.path.join(os.path.dirname(output_path), f'Img_Chla_DIFF_{dateherestr}.png')
    fig.savefig(file_diff, dpi=300, bbox_inches='tight')
    plt.close(fig)

    # coverage
    coverage = dataset.variables['coverage'][:]
    coverage = numpy.ma.masked_equal(coverage, 0)
    fig, ax = start_full_figure()
    bounds = [0, 1, 2, 3, 4]
    norm = colors.BoundaryNorm(boundaries=bounds, ncolors=5)

    newcolors = [
        [0, 0, 0, 1],
        [1, 0, 0, 1],
        [0, 0, 1, 1],
        [0, 1, 0, 1]
    ]
    newcmp = ListedColormap(newcolors)
    h = ax.pcolormesh(lon_array, lat_array, coverage, norm=norm, cmap=newcmp)
    cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    cbar.set_label(label=f'Chl-a Coverage', size=15)
    title = f'Coverage[OLD:red;NEW:blue;BOTH:green]'
    if dateherestr is not None:
        title = f'{title} - {dateherestr}'
    ax.set_title(title, fontsize=20)
    file_coverage = os.path.join(os.path.dirname(output_path), f'Img_Coverage_{dateherestr}.png')
    fig.savefig(file_coverage, dpi=300, bbox_inches='tight')
    plt.close(fig)

    fig, ax = plt.subplots(2, 2, figsize=(15, 9), frameon=True, gridspec_kw={'wspace': 0, 'hspace': 0})
    from matplotlib import image as img
    image = img.imread(file_chla_old)
    ax[0, 0].imshow(image)
    ax[0, 0].set_xticks([])
    ax[0, 0].set_yticks([])
    image = img.imread(file_chla_new)
    ax[0, 1].imshow(image)
    ax[0, 1].set_xticks([])
    ax[0, 1].set_yticks([])
    image = img.imread(file_diff)
    ax[1, 0].imshow(image)
    ax[1, 0].set_xticks([])
    ax[1, 0].set_yticks([])
    image = img.imread(file_coverage)
    ax[1, 1].imshow(image)
    ax[1, 1].set_xticks([])
    ax[1, 1].set_yticks([])
    fig.tight_layout()
    file_out = os.path.join(os.path.dirname(output_path), f'Img_All_{dateherestr}.png')
    plt.savefig(file_out, dpi=300, bbox_inches='tight', facecolor='white')

    dataset.close()
    plt.close(fig)


def launch_single_map_olci(dataset, output_path, dateherestr):
    print(f'[INFO] Output path: {output_path}')

    # lat_array = dataset.variables['latitude'][:]
    # lon_array = dataset.variables['longitude'][:]
    # data = dataset.variables['CHL'][:]
    #
    # data_stats = np.ma.compressed(data)
    #
    # ##chl-a- fix-range
    # fig, ax = start_full_figure()
    # h = ax.pcolormesh(lon_array, lat_array, data, norm=LogNorm(vmin=0.1, vmax=100))
    # cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # units = r'mg m$^-$$^3$'
    # cbar.set_label(label=f'CHL ({units})', size=15)
    # title = f'Chlorophyll a concentration ({units})'
    # if dateherestr is not None:
    #     title = f'{title} - {dateherestr}'
    # ax.set_title(title, fontsize=20)
    # fig.savefig(output_path, dpi=300, bbox_inches='tight')
    # plt.close(fig)
    #
    # ##chl-a
    # fig,ax = start_full_figure()
    # min_chla = np.percentile(data_stats, 1)
    # max_chla = np.percentile(data_stats, 99)
    # h = ax.pcolormesh(lon_array,lat_array,data,norm=LogNorm(vmin=min_chla, vmax=max_chla))
    # cbar = fig.colorbar(h,cax = None, ax = ax, use_gridspec = True, fraction=0.03,format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # units = r'mg m$^-$$^3$'
    # cbar.set_label(label=f'CHL ({units})', size=15)
    # title = f'Chlorophyll a concentration ({units})'
    # if dateherestr is not None:
    #     title = f'{title} - {dateherestr}'
    # ax.set_title(title, fontsize=20)
    # output_path_here = os.path.join(os.path.dirname(output_path), f'Img_Chla_{dateherestr}.png')
    # fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    # plt.close(fig)

    # ##cdf flag
    # cdf_mlp3b = dataset.variables['CDF_MLP3B'][:]
    # cdf_mlp4b = dataset.variables['CDF_MLP4B'][:]
    # cdf_mlp5b = dataset.variables['CDF_MLP5B'][:]
    # cdf_mask_mlp3b = np.ma.where(cdf_mlp3b >= 0.001,2,0)
    # cdf_mask_mlp4b = np.ma.where(cdf_mlp4b >= 0.001,4,0)
    # cdf_mask_mlp5b = np.ma.where(cdf_mlp5b >= 0.001,8,0)
    # cdf_flag_multiple = np.ma.filled(cdf_mask_mlp3b,0)+np.ma.filled(cdf_mask_mlp4b,0)+np.ma.filled(cdf_mask_mlp5b,0)
    # cdf_flag_multiple[cdf_flag_multiple == 0] = 1
    # cdf_flag_multiple = np.ma.array(cdf_flag_multiple,mask=(cdf_mlp3b.mask*cdf_mask_mlp4b.mask*cdf_mask_mlp5b.mask))

    # cdf_mlp3b = np.ma.masked_less(cdf_mlp3b, 0.001)
    # cdf_mlp4b = np.ma.masked_less(cdf_mlp4b, 0.001)
    # cdf_mlp5b = np.ma.masked_less(cdf_mlp5b, 0.001)
    # cdf_sum = np.ma.filled(cdf_mlp3b,0) + np.ma.filled(cdf_mlp4b,0) + np.ma.filled(cdf_mlp5b,0)
    # weight_mlp3b = np.ma.divide(cdf_mlp3b, cdf_sum)
    # weight_mlp4b = np.ma.divide(cdf_mlp4b, cdf_sum)
    # weight_mlp5b = np.ma.divide(cdf_mlp5b, cdf_sum)
    #
    #
    #
    # weight_arrays = [weight_mlp3b, weight_mlp4b, weight_mlp5b]
    # titles = ['Weight CDF MLP3B','Weight CDF MLP4B','Weight CDF MLP5B']
    # ##weight arrays
    # for idx in range(len(weight_arrays)):
    #     fig, ax = start_full_figure()
    #     array = weight_arrays[idx]
    #     h = ax.pcolormesh(lon_array, lat_array,array,cmap = mpl.colormaps['jet'],vmin=0,vmax=1)
    #     cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    #     cbar.ax.tick_params(labelsize=15)
    #     cbar.set_label(label=f'Weight', size=15)
    #     title = titles[idx]
    #     if dateherestr is not None:
    #         title = f'{title} - {dateherestr}'
    #     ax.set_title(title, fontsize=20)
    #     name = title.replace(' ','_')
    #     output_path_here = os.path.join(os.path.dirname(output_path), f'Img_{name}_.png')
    #     fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    #     plt.close(fig)

    # #flag multiple
    # fig, ax = start_full_figure()
    # bounds = [1, 2, 4, 6, 8, 10, 12, 14, 15]
    # norm = colors.BoundaryNorm(boundaries=bounds, ncolors=9)
    # h = ax.pcolormesh(lon_array, lat_array, cdf_flag_multiple,norm = norm, cmap = mpl.colormaps['Set1'])
    # cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03,format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # cbar.set_label(label=f'CDF Flag Multiple', size=15)
    # title = f'CDF Flag Multiple'
    # if dateherestr is not None:
    #     title = f'{title} - {dateherestr}'
    # ax.set_title(title, fontsize=20)
    # output_path_here = os.path.join(os.path.dirname(output_path),f'Img_FlagMultiple_{dateherestr}.png')
    # fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    # plt.close(fig)

    # #flag_cyano
    # rrs555 = dataset.variables['RRS560'][:]
    # rrs670 = dataset.variables['RRS665'][:]
    # rrs555 = rrs555 * np.pi
    # rrs670 = rrs670 * np.pi
    # mask_cyano = np.zeros(rrs670.shape)
    # mask_cyano[rrs555 > 4.25e-3] = mask_cyano[rrs555 > 4.25e-3] + 1
    # mask_cyano[rrs670 > 1.22e-3] = mask_cyano[rrs670 > 1.22e-3] + 2
    # mask_cyano = np.ma.array(mask_cyano,mask = cdf_flag_multiple.mask)
    # fig, ax = start_full_figure()
    # bounds = [0, 1, 2, 3, 4]
    # norm = colors.BoundaryNorm(boundaries=bounds, ncolors=5)
    # newcolors = ['blue','red','green','purple']
    # newcmp = ListedColormap(newcolors)
    # h = ax.pcolormesh(lon_array, lat_array, mask_cyano, norm=norm, cmap=newcmp)
    # cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # cbar.set_label(label=f'Flag Cyano', size=15)
    # title = f'Cyanobacterial flag'
    # if dateherestr is not None:
    #     title = f'{title} - {dateherestr}'
    # ax.set_title(title, fontsize=20)
    # output_path_here = os.path.join(os.path.dirname(output_path), f'Img_FlagCyano_{dateherestr}.png')
    # fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    # plt.close(fig)
    #
    # dataset.close()

    ##multiple plot
    file_out = os.path.join(os.path.dirname(output_path), f'Img_DayAll_{dateherestr}.png')
    if os.path.exists(file_out):
        return
    fig, ax = plt.subplots(2, 3, figsize=(15, 6), frameon=True, gridspec_kw={'wspace': 0, 'hspace': 0})
    from matplotlib import image as img
    files_img = [
        [f'Img_Chla_{dateherestr}.png', f'Img_FlagMultiple_{dateherestr}.png', f'Img_FlagCyano_{dateherestr}.png'],
        [f'Img_Weight_CDF_MLP3B_-_{dateherestr}_.png', f'Img_Weight_CDF_MLP4B_-_{dateherestr}_.png',
         f'Img_Weight_CDF_MLP5B_-_{dateherestr}_.png']]
    for irow in range(2):
        for icol in range(3):
            file_img = os.path.join(os.path.dirname(output_path), files_img[irow][icol])
            image = img.imread(file_img)
            ax[irow, icol].imshow(image)
            ax[irow, icol].set_xticks([])
            ax[irow, icol].set_yticks([])
    fig.tight_layout()
    plt.savefig(file_out, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close(fig)

    print(f'[INFO] Completed')


def launch_single_map_cci(dataset, output_path, dateherestr):
    print(f'[INFO] Output path: {output_path}')

    # lat_array = dataset.variables['latitude'][:]
    # lon_array = dataset.variables['longitude'][:]
    # data = dataset.variables['CHL'][:]
    #
    # data_stats = np.ma.compressed(data)

    ##chl-a- fix-range
    # fig, ax = start_full_figure()
    # h = ax.pcolormesh(lon_array, lat_array, data, norm=LogNorm(vmin=0.1, vmax=100))
    # cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # units = r'mg m$^-$$^3$'
    # cbar.set_label(label=f'CHL ({units})', size=15)
    # title = f'Chlorophyll a concentration ({units})'
    # if dateherestr is not None:
    #     title = f'{title} - {dateherestr}'
    # ax.set_title(title, fontsize=20)
    # fig.savefig(output_path, dpi=300, bbox_inches='tight')
    # plt.close(fig)

    ##chl-a
    # fig,ax = start_full_figure()
    # min_chla = np.percentile(data_stats, 1)
    # max_chla = np.percentile(data_stats, 99)
    # h = ax.pcolormesh(lon_array,lat_array,data,norm=LogNorm(vmin=min_chla, vmax=max_chla))
    # cbar = fig.colorbar(h,cax = None, ax = ax, use_gridspec = True, fraction=0.03,format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # units = r'mg m$^-$$^3$'
    # cbar.set_label(label=f'CHL ({units})', size=15)
    # title = f'Chlorophyll a concentration ({units})'
    # if dateherestr is not None:
    #     title = f'{title} - {dateherestr}'
    # ax.set_title(title, fontsize=20)
    # output_path_here = os.path.join(os.path.dirname(output_path), f'Img_Chla_{dateherestr}.png')
    # fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    # plt.close(fig)

    # #cdf flag
    # cdf_mlp3b = dataset.variables['CDF_MLP3B'][:]
    # cdf_mlp4b = dataset.variables['CDF_MLP4B'][:]
    # cdf_mlp5b = dataset.variables['CDF_MLP5B'][:]
    # cdf_mask_mlp3b = np.ma.where(cdf_mlp3b >= 0.001,2,0)
    # cdf_mask_mlp4b = np.ma.where(cdf_mlp4b >= 0.001,4,0)
    # cdf_mask_mlp5b = np.ma.where(cdf_mlp5b >= 0.001,8,0)
    # cdf_flag_multiple = np.ma.filled(cdf_mask_mlp3b,0)+np.ma.filled(cdf_mask_mlp4b,0)+np.ma.filled(cdf_mask_mlp5b,0)
    # cdf_flag_multiple[cdf_flag_multiple == 0] = 1
    # cdf_flag_multiple = np.ma.array(cdf_flag_multiple,mask=(cdf_mlp3b.mask*cdf_mask_mlp4b.mask*cdf_mask_mlp5b.mask))
    #
    # cdf_mlp3b = np.ma.masked_less(cdf_mlp3b, 0.001)
    # cdf_mlp4b = np.ma.masked_less(cdf_mlp4b, 0.001)
    # cdf_mlp5b = np.ma.masked_less(cdf_mlp5b, 0.001)
    # cdf_sum = np.ma.filled(cdf_mlp3b,0) + np.ma.filled(cdf_mlp4b,0) + np.ma.filled(cdf_mlp5b,0)
    # weight_mlp3b = np.ma.divide(cdf_mlp3b, cdf_sum)
    # weight_mlp4b = np.ma.divide(cdf_mlp4b, cdf_sum)
    # weight_mlp5b = np.ma.divide(cdf_mlp5b, cdf_sum)

    # weight_arrays = [weight_mlp3b, weight_mlp4b, weight_mlp5b]
    # titles = ['Weight CDF MLP3B','Weight CDF MLP4B','Weight CDF MLP5B']
    # ##weight arrays
    # for idx in range(len(weight_arrays)):
    #     fig, ax = start_full_figure()
    #     array = weight_arrays[idx]
    #     h = ax.pcolormesh(lon_array, lat_array,array,cmap = mpl.colormaps['jet'],vmin=0,vmax=1)
    #     cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    #     cbar.ax.tick_params(labelsize=15)
    #     cbar.set_label(label=f'Weight', size=15)
    #     title = titles[idx]
    #     if dateherestr is not None:
    #         title = f'{title} - {dateherestr}'
    #     ax.set_title(title, fontsize=20)
    #     name = title.replace(' ','_')
    #     output_path_here = os.path.join(os.path.dirname(output_path), f'Img_{name}_.png')
    #     fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    #     plt.close(fig)

    # #flag multiple
    # fig, ax = start_full_figure()
    # bounds = [1, 2, 4, 6, 8, 10, 12, 14, 15]
    # norm = colors.BoundaryNorm(boundaries=bounds, ncolors=9)
    # h = ax.pcolormesh(lon_array, lat_array, cdf_flag_multiple,norm = norm, cmap = mpl.colormaps['Set1'])
    # cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03,format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # cbar.set_label(label=f'CDF Flag Multiple', size=15)
    # title = f'CDF Flag Multiple'
    # if dateherestr is not None:
    #     title = f'{title} - {dateherestr}'
    # ax.set_title(title, fontsize=20)
    # output_path_here = os.path.join(os.path.dirname(output_path),f'Img_FlagMultiple_{dateherestr}.png')
    # fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    # plt.close(fig)

    # #flag_cyano
    # rrs555 = dataset.variables['RRS560'][:]
    # rrs670 = dataset.variables['RRS665'][:]
    # rrs555 = rrs555 * np.pi
    # rrs670 = rrs670 * np.pi
    # mask_cyano = np.zeros(rrs670.shape)
    # mask_cyano[rrs555 > 4.25e-3] = mask_cyano[rrs555 > 4.25e-3] + 1
    # mask_cyano[rrs670 > 1.22e-3] = mask_cyano[rrs670 > 1.22e-3] + 2
    # mask_cyano = np.ma.array(mask_cyano,mask = cdf_flag_multiple.mask)
    # fig, ax = start_full_figure()
    # bounds = [0, 1, 2, 3, 4]
    # norm = colors.BoundaryNorm(boundaries=bounds, ncolors=5)
    # newcolors = ['blue','red','green','purple']
    # newcmp = ListedColormap(newcolors)
    # h = ax.pcolormesh(lon_array, lat_array, mask_cyano, norm=norm, cmap=newcmp)
    # cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    # cbar.ax.tick_params(labelsize=15)
    # cbar.set_label(label=f'Flag Cyano', size=15)
    # title = f'Cyanobacterial flag'
    # if dateherestr is not None:
    #     title = f'{title} - {dateherestr}'
    # ax.set_title(title, fontsize=20)
    # output_path_here = os.path.join(os.path.dirname(output_path), f'Img_FlagCyano_{dateherestr}.png')
    # fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    # plt.close(fig)
    #
    # dataset.close()

    # multiple plot
    file_out = os.path.join(os.path.dirname(output_path), f'Img_DayAll_{dateherestr}.png')
    if os.path.exists(file_out):
        return
    fig, ax = plt.subplots(2, 3, figsize=(15, 6), frameon=True, gridspec_kw={'wspace': 0, 'hspace': 0})
    from matplotlib import image as img
    files_img = [
        [f'Img_Chla_{dateherestr}.png', f'Img_FlagMultiple_{dateherestr}.png', f'Img_FlagCyano_{dateherestr}.png'],
        [f'Img_Weight_CDF_MLP3B_-_{dateherestr}_.png', f'Img_Weight_CDF_MLP4B_-_{dateherestr}_.png',
         f'Img_Weight_CDF_MLP5B_-_{dateherestr}_.png']]
    for irow in range(2):
        for icol in range(3):
            file_img = os.path.join(os.path.dirname(output_path), files_img[irow][icol])
            image = img.imread(file_img)
            ax[irow, icol].imshow(image)
            ax[irow, icol].set_xticks([])
            ax[irow, icol].set_yticks([])
    fig.tight_layout()
    plt.savefig(file_out, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close(fig)

    print(f'[INFO] Completed')


def get_date_here_from_file_name(input_path):
    type = 'diff'
    name = os.path.basename(input_path)
    date_here_str = None
    if name.startswith('M') and name.endswith('BAL202411.nc'):
        try:
            date_here = dt.strptime(name[1:8], '%Y%j')
            date_here_str = date_here.strftime('%Y-%m-%d')
            type = 'cci'
        except:
            pass
    if name.startswith('O') and name.endswith('BAL202411.nc'):
        try:
            date_here = dt.strptime(name[1:8], '%Y%j')
            date_here_str = date_here.strftime('%Y-%m-%d')
            type = 'olci'
        except:
            pass

    return date_here_str, type


def start_full_figure():
    # start figure and axes

    fig, ax = plt.subplots(subplot_kw=dict(projection=ccrs.PlateCarree()))
    fig.set_figwidth(15)
    fig.set_figheight(15)

    # coastlines
    ax.add_feature(cartopy.feature.LAND, zorder=0, edgecolor='black', linewidth=0.5)
    # ax.coastlines(resolution='10m')

    # Prep circular boundary
    # r_extent = self.area_def.area_extent[1]
    # r_extent *= 1.005
    # circle_path = mpath.Path.unit_circle()
    # circle_path = mpath.Path(circle_path.vertices.copy() * r_extent, circle_path.codes.copy())
    # ax.set_boundary(circle_path)
    # ax.set_frame_on(False)

    # grid lines
    gl = ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False, linewidth=0.5, linestyle='dotted')
    gl.xlocator = mticker.FixedLocator([5, 10, 15, 20, 25, 30])
    gl.ylocator = mticker.FixedLocator([55, 57.5, 60, 62.5, 65])
    gl.right_labels = False
    gl.left_labels = True
    gl.bottom_labels = True
    gl.top_labels = False
    gl.xlabel_style = {'size': 15}
    gl.ylabel_style = {'size': 15}
    # plt.draw()
    # for ea in gl.label_artists:
    #     txt = ea.get_text()
    #     pos = ea.get_position()
    #     if txt == '135°W' and pos[0] < (-2000000):
    #         ea.set_visible(True)
    #     if txt == '45°E' and pos[0] > 2000000:
    #         ea.set_visible(True)
    #     if pos[0] == 90:
    #         ea.set_visible(True)
    #         ea.set_position([135, pos[1]])
    #     if pos[0] == -90:
    #         ea.set_visible(True)
    #         ea.set_position([-45, pos[1]])
    #     if pos[1] == 65:
    #         ea.set_visible(False)

    return fig, ax


if __name__ == '__main__':
    main()
