import argparse
import os

# code_home = os.path.abspath('')
# print(code_home)
from c2rcc import C2RCC
from polymer_lois import POLYMER
from fub_csiro_lois import FUB_CSIRO

parser = argparse.ArgumentParser(description="Atmospheric correction launcher")

parser.add_argument("-v", "--verbose", help="Verbose mode.", action="store_true")
parser.add_argument("-p", "--product", help="Input product (testing)")
parser.add_argument('-i', "--inputpath", help="Input directory")
parser.add_argument('-o', "--outputpath", help="Output directory", required=True)
parser.add_argument('-c', "--config_file", help="Configuration file (Default: aceasy_config.ini)")
parser.add_argument('-ac', "--atm_correction", help="Atmospheric correction", choices=["C2RCC","POLYMER","FUB_CSIRO"], required=True)
args = parser.parse_args()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('[INFO] Started')
    fconfig = None
    if args.config_file:
        fconfig = args.config_file
    else:
        if os.path.exists('aceasy_config.ini'):
            fconfig = 'aceasy_config.ini'
        elif os.path.exists(os.path.join('aceasy', 'aceasy_config.ini')):
            fconfig = os.path.join('aceasy', 'aceasy_config.ini')
    if fconfig is None:
        print(f'[ERROR] Config file is required')
        exit(1)
    if not os.path.exists(fconfig):
        print(f'[ERROR] Config file: {fconfig} does not exist')
        exit(1)

    if not args.product and not args.inputpath:
        print(f'[ERROR] Product name or input folder are required')
        exit(1)

    input_path = None
    if args.inputpath:
        input_path = args.inputpath
    if args.product:
        prod_path = args.product

    if not args.outputpath:
        print(f'[ERROR] Output folder option is required')
        exit(1)
    output_path = args.outputpath

    if args.atm_correction == 'C2RCC':
        corrector = C2RCC(fconfig, args.verbose)
    elif args.atm_correction == 'POLYMER':
        corrector = POLYMER(fconfig, args.verbose)
    elif args.atm_correction == 'FUB_CSIRO':
        corrector = FUB_CSIRO(fconfig,args.verbose)

    if not corrector.check_runac():
        exit(1)
    if args.verbose:
        print(f'[INFO] Started {args.atm_correction} processor')

    if input_path is None:  # single product, for testing
        p = corrector.run_process(prod_path,output_path)
        if args.verbose:
            print('--------------------------------------------------')
    else:
        for f in os.listdir(input_path):
            prod_path = os.path.join(input_path,f)
            if os.path.isdir(prod_path) and f.endswith('.SEN3'):
                if args.verbose:
                    print('--------------------------------------------------')
                p = corrector.run_process(prod_path,output_path)


    # if args.atm_correction == 'C2RCC':
    #     corrector = C2RCC(fconfig, args.verbose)
    #     if not corrector.check_runac():
    #         exit(1)
    #     if args.verbose:
    #         print('[INFO] Started C2RCC processor')
    #
    #     if input_path is None: #single product, for testing
    #         p = corrector.run_process(prod_path,output_path)
    #         if args.verbose:
    #             print('--------------------------------------------------')
    #     else:
    #         for f in os.listdir(input_path):
    #             prod_path = os.path.join(input_path,f)
    #             if os.path.isdir(prod_path) and f.endswith('.SEN3'):
    #                 if args.verbose:
    #                     print('--------------------------------------------------')
    #                 p = corrector.run_process(prod_path,output_path)

