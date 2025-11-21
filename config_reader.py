import os,configparser

class ConfigReader(object):

    def __init__(self, config_file,options):
        self.options = None
        if options is not None:
            self.options = options
        else:
            self.options = configparser.ConfigParser()
            self.options.read(config_file)

        self.type_list = ['str','file','directory','directory_in','date','int','float','boolean','rrslist','strlist','floatlist']

    def check_options(self):
        if self.options is None:
            print(f'[ERROR] Options could not be read from the configuration file')
            return False
        return True

    def get_sections(self):
        return self.options.sections()

    def check_section(self,section):
        if not self.options.has_section(section):
            print(f'[ERROR] Section {section} is not available the configuration file')
            return False
        return True

    def check_option(self,section,option):
        if not self.options.has_option(section,option):
            print(f'[ERROR] Option {option} in section {section} in not available the configuration file')
            return False
        return True

    def retrieve_options(self,section,compulsory_options,other_options):
        if not self.check_section(section):
            return None
        opt_dict = {}
        compulsory_are_present = True
        for op in compulsory_options:
            if not self.check_option(section,op):
                compulsory_are_present = False
            else:
                type,default,potential_values = self.check_option_obj(compulsory_options[op])
                if type is None:
                    compulsory_are_present = False
                    continue
                opt_dict[op] = self.get_value_param(section,op,default,type,potential_values)

        if not compulsory_are_present:
            return None

        if other_options is None:
            return opt_dict

        for op in other_options:
            if self.check_option(section,op):
                type, default, potential_values = self.check_option_obj(compulsory_options[op])
                if type is not None:
                    opt_dict[op] = self.get_value_param(section, op, default, type, potential_values)

        return opt_dict

    def check_option_obj(self,param_obj):
        type,default,potential_values  =[None]*3
        if 'type' in param_obj.keys():
            type = param_obj['type']
        if type not in self.type_list:
            type = None
        if 'default' in param_obj.keys():
            default = param_obj['default']
        if 'potential_values' in param_obj.keys():
            potential_values = param_obj['potential_values']

        return type,default,potential_values


    def get_value(self, section, key):
        value = None
        if self.options.has_option(section, key):
            try:
                value = self.options[section][key]
            except:
                print(f'[ERROR] Parsing error in section {section} - {key}')
        return value

    def get_value_param(self, section, key, default, type, potential_values):
        value = self.get_value(section, key)

        if value is None:
            return default
        if type == 'str':
            if potential_values is None:
                return value.strip()
            else:
                if value.strip().lower() in potential_values:
                    return value
                else:
                    print(
                        f'[ERROR] [{section}] {value} is not a valid  value for {key}. Valid values: {potential_values} ')
                    return default
        if type == 'file':
            if not os.path.exists(value.strip()):
                return default
            else:
                return value.strip()
        if type == 'directory' or type=='directory_in':
            directory = value.strip()

            if not os.path.isdir(directory):
                if type=='directory_in':
                    return default
                else:
                    try:
                        os.mkdir(directory)
                        return directory
                    except:
                        return default
            else:
                return directory
        if type == 'int':
            return int(value)
        if type == 'float':
            return float(value)
        if type == 'boolean':
            if value == '1' or value.upper() == 'TRUE':
                return True
            elif value == '0' or value.upper() == 'FALSE':
                return False
            else:
                return True
        if type == 'rrslist':
            list_str = value.split(',')
            list = []
            for vals in list_str:
                vals = vals.strip().replace('.', '_')
                list.append(f'RRS{vals}')
            return list
        if type == 'strlist':
            list_str = value.split(',')
            list = []
            for vals in list_str:
                list.append(vals.strip())
            return list
        if type == 'floatlist':
            list_str = value.split(',')
            list = []
            for vals in list_str:
                vals = vals.strip()
                list.append(float(vals))
            return list

        if type == 'date':
            from datetime import datetime as dt
            try:
                val = dt.strptime(value.strip(),'%Y-%m-%d')
            except:
                return default
            return val



class FileSelector(object):
    def __init__(self,info_dict):
        self.input_path = None
        self.input_path_organization = None
        self.input_name_file_format = None
        self.input_name_file_date_format = None

        self.output_path = None
        self.ouput_path_organization = None
        self.input_name_file_format = None
        self.input_name_file_date_format = None

        if info_dict is not None:
            if 'input_path' in info_dict.keys():
                self.input_path = info_dict['input_path']
            if 'input_path_organization' in info_dict.keys():
                self.input_path_organization = info_dict['input_path_organization']
            if 'input_name_file_format' in info_dict.keys():
                self.input_name_file_format = info_dict['input_name_file_format']
            if 'input_name_file_date_format' in info_dict.keys():
                self.input_name_file_date_format = info_dict['input_name_file_date_format']

            if 'output_path' in info_dict.keys():
                self.output_path = info_dict['output_path']
            if 'output_path_organization' in info_dict.keys():
                self.output_path_organization = info_dict['output_path_organization']
            if 'output_name_file_format' in info_dict.keys():
                self.output_name_file_format = info_dict['output_name_file_format']
            if 'output_name_file_date_format' in info_dict.keys():
                self.output_name_file_date_format = info_dict['output_name_file_date_format']

        self.input_path_organization = self.check_path_organization(self.input_path_organization)
        self.output_path_organization = self.check_path_organization(self.output_path_organization)

    def check_path_organization(self,org):
        if org is not None:
            if org.lower()=='none':
                org = None
            else:
                if org == 'YYYYmmdd':
                    org = '%Y/%m/%d'
                elif org == 'YYYYmm':
                    org = '%Y/%m'
                elif org == 'YYYYjjj':
                    org = ('%Y/%j')
                else:
                    org = org.replace('YYYY', '%Y')
                    org = org.replace('mm', '%m')
                    org = org.replace('dd', '%d')
                    org = org.replace('jjj', '%j')
        return org

    def get_input_file(self,work_date):
        input_folder = self.get_folder(work_date,self.input_path,self.input_path_organization,False)

    def get_folder(self,work_date,path,org,tryCreate):
        if org is None:
            final_path = path
        else:
            org_l = org.strip().split('/')
            try:
                if len(org_l)==1:
                    final_path = os.path.join(path,work_date.strftime(org_l[0]))
                elif len(org_l)==2:
                    final_path = os.path.join(path,work_date.strftime(org_l[0]),work_date.strftime(org_l[1]))
                elif len(org_l)==3:
                    final_path = os.path.join(path,work_date.strftime(org_l[0]),work_date.strftime(org_l[1]),work_date.strftime(org_l[2]))
                else:
                    final_path = path
                    for o in org_l:
                        final_path = os.path.join(final_path,o)
            except:
                print(f'[ERROR] Path date for date {work_date.strftime("%Y-%m-%d")} and path {path} could not be obtained using  {org}')
                final_path = None

        if final_path is not None:
            if not os.path.isdir(final_path) and tryCreate:
                try:
                    os.mkdir(final_path)
                except:
                    print(f'[ERROR] Path {final_path} is not available and could not be created. Review permissions')
                    return None

        return final_path
