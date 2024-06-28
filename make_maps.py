import argparse,os
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

def main():
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
        launch_single_map(dataset, output_path, dateherestr)

def launch_multiple_maps(input_dir):
    for name in os.listdir(input_dir):
        input_path = os.path.join(input_dir,name)
        date_here_str = get_date_here_from_file_name(input_path)
        if date_here_str is not None:
            try:
                print(f'[INFO] Input path: {input_path}')
                dataset = Dataset(input_path)
            except:
                continue
            output_path = input_path.replace('.nc', '.png')
            launch_single_map(dataset, output_path, date_here_str)

def launch_single_map(dataset,output_path,dateherestr):

    print(f'[INFO] Output path: {output_path}')

    lat_array = dataset.variables['latitude'][:]
    lon_array = dataset.variables['longitude'][:]
    data = dataset.variables['CHL'][:]

    data_stats = np.ma.compressed(data)

    min_chla = np.percentile(data_stats,1)
    max_chla = np.percentile(data_stats,99)


    ##chl-a
    fig,ax = start_full_figure()
    h = ax.pcolormesh(lon_array,lat_array,data,norm=LogNorm(vmin=min_chla, vmax=max_chla))
    cbar = fig.colorbar(h,cax = None, ax = ax, use_gridspec = True, fraction=0.03,format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    units = r'mg m$^-$$^3$'
    cbar.set_label(label=f'CHL ({units})', size=15)
    title = f'Chlorophyll a concentration ({units})'
    if dateherestr is not None:
        title = f'{title} - {dateherestr}'
    ax.set_title(title, fontsize=20)
    output_path_here = os.path.join(os.path.dirname(output_path), f'Img_Chla_{dateherestr}.png')
    fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    plt.close(fig)

    #cdf flag
    cdf_mlp3b = dataset.variables['CDF_MLP3B'][:]
    cdf_mlp4b = dataset.variables['CDF_MLP4B'][:]
    cdf_mlp5b = dataset.variables['CDF_MLP5B'][:]
    cdf_mask_mlp3b = np.ma.where(cdf_mlp3b >= 0.001,2,0)
    cdf_mask_mlp4b = np.ma.where(cdf_mlp4b >= 0.001,4,0)
    cdf_mask_mlp5b = np.ma.where(cdf_mlp5b >= 0.001,8,0)
    cdf_flag_multiple = cdf_mask_mlp3b+cdf_mask_mlp4b+cdf_mask_mlp5b
    cdf_flag_multiple[cdf_flag_multiple == 0] = 1

    cdf_mlp3b = np.ma.masked_less(cdf_mlp3b, 0.001)
    cdf_mlp4b = np.ma.masked_less(cdf_mlp4b, 0.001)
    cdf_mlp5b = np.ma.masked_less(cdf_mlp5b, 0.001)
    cdf_sum = cdf_mlp3b + cdf_mlp4b + cdf_mlp5b
    weight_mlp3b = np.ma.divide(cdf_mlp3b, cdf_sum)
    weight_mlp4b = np.ma.divide(cdf_mlp4b, cdf_sum)
    weight_mlp5b = np.ma.divide(cdf_mlp5b, cdf_sum)
    #weight_sum = weight_mlp3b+weight_mlp4b+weight_mlp5b

    weight_arrays = [ weight_mlp3b, weight_mlp4b, weight_mlp5b]
    titles = ['Weight CDF MLP3B','Weight CDF MLP4B','Weight CDF MLP5B']
    ##weight arrays
    for idx in range(len(weight_arrays)):
        fig, ax = start_full_figure()
        array = weight_arrays[idx]
        print(array.shape,type(array),np.ma.count_masked(array))
        h = ax.pcolormesh(lon_array, lat_array,array,cmap = mpl.colormaps['jet'],vmin=0,vmax=1)
        cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03, format="$%.2f$")
        cbar.ax.tick_params(labelsize=15)
        cbar.set_label(label=f'Weight', size=15)
        title = titles[idx]
        if dateherestr is not None:
            title = f'{title} - {dateherestr}'
        ax.set_title(title, fontsize=20)
        name = title.replace(' ','_')
        output_path_here = os.path.join(os.path.dirname(output_path), f'Img_{name}_.png')
        fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
        plt.close(fig)


    ##flag multiple
    fig, ax = start_full_figure()
    bounds = [1, 2, 4, 6, 8, 10, 12, 14, 15]
    norm = colors.BoundaryNorm(boundaries=bounds, ncolors=9)
    h = ax.pcolormesh(lon_array, lat_array, cdf_flag_multiple,norm = norm, cmap = mpl.colormaps['Set1'])
    cbar = fig.colorbar(h, cax=None, ax=ax, use_gridspec=True, fraction=0.03,format="$%.2f$")
    cbar.ax.tick_params(labelsize=15)
    cbar.set_label(label=f'CDF Flag Multiple', size=15)
    title = f'CDF Flag Multiple'
    if dateherestr is not None:
        title = f'{title} - {dateherestr}'
    ax.set_title(title, fontsize=20)
    output_path_here = os.path.join(os.path.dirname(output_path),f'Img_FlagMultiple_{dateherestr}.png')
    fig.savefig(output_path_here, dpi=300, bbox_inches='tight')
    plt.close(fig)



    dataset.close()


    print(f'[INFO] Completed')

def get_date_here_from_file_name(input_path):
    name = os.path.basename(input_path)
    date_here_str =None
    if name.startswith('M') and name.endswith('BAL202411.nc'):
        try:
            date_here = dt.strptime(name[1:8],'%Y%j')
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
    #ax.coastlines(resolution='10m')

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
    #plt.draw()
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