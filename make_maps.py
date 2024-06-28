import argparse, os

import numpy.ma
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

parser = argparse.ArgumentParser(description="Make maps launcher")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-i", "--input_path", help="Input path", required=True)
parser.add_argument("-o", "--output_path", help="Output path")
args = parser.parse_args()


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

        chl_dif = chl_old - chl_new
        coverage = np.zeros(chl_old.shape)
        coverage[~chl_old.mask] = coverage[~chl_old.mask] + 1
        coverage[~chl_new.mask] = coverage[~chl_new.mask] + 2

        file_out = os.path.join(dir_base_dif, f'Diff_{date1}.nc')
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

        nc_out.close()
        dataset_new.close()

        date_here = date_here + timedelta(hours=24)
    return True


def compare_old_new():
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

    weight_mlp3b_total = np.ma.zeros((1147,1185))
    weight_mlp4b_total = np.ma.zeros((1147, 1185))
    weight_mlp5b_total = np.ma.zeros((1147, 1185))
    n_cdf_total = np.ma.zeros((1147,1185))
    n_nocdf_total = np.ma.zeros((1147,1185))
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
            n_cdf_total[flag_multiple>=2] = n_cdf_total[flag_multiple>=2]+1
            n_nocdf_total[flag_multiple == 1] = n_nocdf_total[flag_multiple ==1] + 1

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
    weight_mlp3b_total = np.ma.masked_equal(weight_mlp3b_total,0)
    weight_mlp4b_total = np.ma.masked_equal(weight_mlp4b_total,0)
    weight_mlp5b_total = np.ma.masked_equal(weight_mlp5b_total,0)
    n_total = n_cdf_total + n_nocdf_total
    n_cdf_total = np.ma.masked_equal(n_cdf_total,0)
    n_nocdf_total = np.ma.masked_equal(n_nocdf_total,0)
    n_total = np.ma.masked_equal(n_total, 0)
    coverage_cdf = (n_cdf_total/n_total)*100
    coverage_no_cdf = (n_nocdf_total / n_total) * 100
    coverage_cdf_mlp3 = (weight_mlp3b_total / n_cdf_total) * 100
    coverage_cdf_mlp4 = (weight_mlp4b_total / n_cdf_total) * 100
    coverage_cdf_mlp5 = (weight_mlp5b_total / n_cdf_total) * 100
    coverage_total_mlp3 = ((weight_mlp3b_total+n_nocdf_total) / n_total) * 100
    variable_list = {
        'weight_mlp3b_total': weight_mlp3b_total,
        'weight_mlp4b_total': weight_mlp4b_total,
        'weight_mlp5b_total': weight_mlp5b_total,
        'n_total': n_total,
        'n_cdf_total':n_cdf_total,
        'n_nocdf_total': n_nocdf_total,
        'coverage_cdf':coverage_cdf,
        'coverage_nocdf':coverage_no_cdf,
        'coverage_cdf_mlp3':coverage_cdf_mlp3,
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


def main():
    if compare_old_new():
        return
    # if compute_diff():
    #     return

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
        dateherestr = get_date_here_from_file_name(input_path)
        if dateherestr is None:
            name = os.path.basename(input_path)
            datestr = name[5:12]
            dateherestr = dt.strptime(datestr, '%Y%j').strftime('%Y%m%d')
            launch_single_map_temp(dataset, output_path, dateherestr)
        else:
            launch_single_map(dataset, output_path, dateherestr)


def launch_multiple_maps(input_dir):
    for name in os.listdir(input_dir):
        input_path = os.path.join(input_dir, name)
        date_here_str = get_date_here_from_file_name(input_path)

        if date_here_str is not None:
            try:
                print(f'[INFO] Input path: {input_path}')
                dataset = Dataset(input_path)
            except:
                continue
            output_path = input_path.replace('.nc', '.png')
            launch_single_map(dataset, output_path, date_here_str)
        else:
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
            launch_single_map_temp(dataset, output_path, dateherestr)


def launch_single_map_temp(dataset, output_path, dateherestr):
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
    from matplotlib.colors import ListedColormap
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


def launch_single_map(dataset, output_path, dateherestr):
    print(f'[INFO] Output path: {output_path}')

    lat_array = dataset.variables['latitude'][:]
    lon_array = dataset.variables['longitude'][:]
    data = dataset.variables['CHL'][:]

    data_stats = np.ma.compressed(data)

    ##chl-a- fix-range
    fig, ax = start_full_figure()
    h = ax.pcolormesh(lon_array, lat_array, data, norm=LogNorm(vmin=0.1, vmax=100))
    cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    units = r'mg m$^-$$^3$'
    cbar.set_label(label=f'CHL ({units})', size=15)
    title = f'Chlorophyll a concentration ({units})'
    if dateherestr is not None:
        title = f'{title} - {dateherestr}'
    ax.set_title(title, fontsize=20)
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

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
    #
    # #cdf flag
    # cdf_mlp3b = dataset.variables['CDF_MLP3B'][:]
    # cdf_mlp4b = dataset.variables['CDF_MLP4B'][:]
    # cdf_mlp5b = dataset.variables['CDF_MLP5B'][:]
    # cdf_mask_mlp3b = np.ma.where(cdf_mlp3b >= 0.001,2,0)
    # cdf_mask_mlp4b = np.ma.where(cdf_mlp4b >= 0.001,4,0)
    # cdf_mask_mlp5b = np.ma.where(cdf_mlp5b >= 0.001,8,0)
    # cdf_flag_multiple = cdf_mask_mlp3b+cdf_mask_mlp4b+cdf_mask_mlp5b
    # cdf_flag_multiple[cdf_flag_multiple == 0] = 1
    #
    # cdf_mlp3b = np.ma.masked_less(cdf_mlp3b, 0.001)
    # cdf_mlp4b = np.ma.masked_less(cdf_mlp4b, 0.001)
    # cdf_mlp5b = np.ma.masked_less(cdf_mlp5b, 0.001)
    # cdf_sum = cdf_mlp3b + cdf_mlp4b + cdf_mlp5b
    # weight_mlp3b = np.ma.divide(cdf_mlp3b, cdf_sum)
    # weight_mlp4b = np.ma.divide(cdf_mlp4b, cdf_sum)
    # weight_mlp5b = np.ma.divide(cdf_mlp5b, cdf_sum)
    # #weight_sum = weight_mlp3b+weight_mlp4b+weight_mlp5b
    #
    # weight_arrays = [ weight_mlp3b, weight_mlp4b, weight_mlp5b]
    # titles = ['Weight CDF MLP3B','Weight CDF MLP4B','Weight CDF MLP5B']
    # ##weight arrays
    # for idx in range(len(weight_arrays)):
    #     fig, ax = start_full_figure()
    #     array = weight_arrays[idx]
    #     print(array.shape,type(array),np.ma.count_masked(array))
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
    #
    #
    # ##flag multiple
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

    dataset.close()

    print(f'[INFO] Completed')


def get_date_here_from_file_name(input_path):
    name = os.path.basename(input_path)
    date_here_str = None
    if name.startswith('M') and name.endswith('BAL202411.nc'):
        try:
            date_here = dt.strptime(name[1:8], '%Y%j')
            date_here_str = date_here.strftime('%Y-%m-%d')
        except:
            pass
    return date_here_str


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
