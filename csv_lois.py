import pandas as pd
import numpy as np

class CSV_FILE:

    def __init__(self,path_csv):
        self.df = pd.read_csv(path_csv,sep=';')
        col_names = self.df.columns.tolist()
        self.wl_list = []
        self.wl_col_names = []
        for c in col_names:
            if c.startswith('RRS'):
                try:
                    rrs_val = float(c[3:].replace('_','.'))
                    self.wl_list.append(rrs_val)
                    self.wl_col_names.append(c)
                except:
                    pass
        if len(self.wl_list)==0:
            print(f'[WARNING] No valid RRS columns identified in file {path_csv}')
        self.max_diff_wl = 5

        self.df_out = None

    def start_copy_output(self):
        self.df_out = self.df.copy()

    def add_column_to_output(self,col_name,data):
        self.df_out[col_name] = data

    def save_output(self,file_out):
        self.df_out.to_csv(file_out,sep=';')

    def get_rrs_spectra(self,wl_list_here,apply_band_shift):
        if len(self.wl_list)==0:
            return None
        col_names_spectra = []
        wl_list_array = np.array(self.wl_list)
        for wl in wl_list_here:
            wlf = np.float32(wl)
            index_wl = np.argmin(np.abs(wlf-wl_list_array))
            diff = abs(wlf - wl_list_array[index_wl])
            if diff<=self.max_diff_wl:
                col_names_spectra.append(self.wl_col_names[index_wl])
            else:
                print(f'Wavelength {wl} is not avaiable in the CSV wavelength list: {self.wl_list}')
                return None

        spectra = np.array(self.df.loc[:,col_names_spectra])

        return spectra




