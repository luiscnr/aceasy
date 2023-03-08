from libsvm.svmutil import *
import csv
import math
import GeoInfo


class SVMLois:
    # Create a empty SVM
    def __init__(self):
        self.svm_model = []
        self.input_scale = []
        self.input_min = []
        self.input_max = []
        self.optimal_threshold = 0.5
        self.max_prob = 1

    # Init SVM model for prediction of Pseudo-nitzschia spp. blooms based on Polymer 4.8 data
    def init_pseudonitzschia_model_version48(self):
        self.svm_model = svm_load_model('PN_SVM_BNB_Prob_48.model')  # Loading model in file 'SVM_BNB_Prob_48.model'
        self.input_scale = [10000] * 13
        self.input_scale[10:] = [1, 1, 1]
        self.input_min = [42, 44, 47, 59, 63, 46, 7, -14, -1, -3, 24, 0, 18]
        self.input_max = [364, 300, 287, 329, 341, 308, 84, 56, 39, 23, 73, 52, 238]
        self.optimal_threshold = 0.41

    # Init SVM model for prediction of Pseudo-nitzschia spp. blooms based on Polymer 4.12 data
    def init_pseudonitzschia_model_version412(self):
        self.svm_model = svm_load_model('PN_SVM_BNB_Prob_412.model')  # Loading model in file 'SVM_BNB_Prob_4_12.model'
        self.input_scale = [10000] * 15
        self.input_scale[12:] = [1, 1, 1]
        self.input_min = [-25, 0, 15, 15, 15, 30, -5, -25, -25, -60, 0, 0, 24, 0, 15]
        self.input_max = [215, 215, 215, 225, 225, 225, 70, 40, 55, 35, 45, 30, 73, 53, 240]
        self.optimal_threshold = 0.59

    # Init SVM model for prediction Alexandrium minutum presence based on Polymer 4.8 data
    def init_alexandrium_model_version48(self):
        self.svm_model = svm_load_model(
            'Alex_SVM_PA_Prob_48.model')  # Loading model in file 'Alex_SVM_PA_Prob_48.model'
        self.input_scale = [10000] * 10
        self.input_min = [59, 56, 65, 82, 88, 70, 13, 0, 3, 0]
        self.input_max = [233, 214, 195, 191, 178, 175, 53, 32, 38, 21]
        self.optimal_threshold = 0.2

    # Init SVM model for prediction Alexandrium minutum presence based on Polymer 4.12 data
    def init_alexandrium_model_version412(self):
        self.svm_model = svm_load_model(
            'Alex_SVM_PA_Prob_412.model')  # Loading model in file 'Alex_SVM_PA_Prob_412.model'
        self.input_scale = [10000] * 12
        self.input_min = [15, 25, 40, 40, 40, 30, -10, -20, -20, -20, 0, 0]
        self.input_max = [110, 110, 120, 130, 130, 170, 60, 40, 50, 20, 40, 25]
        self.optimal_threshold = 0.6

    # Init SVM model for prediction of ASP closures for a given ria
    def init_pseudonitzschia_prediction_model(self, day):
        if day == 1:
            self.svm_model = svm_load_model('PN_Prediction_Day1.model')
            self.optimal_threshold = 0.06
            self.max_prob = 0.11
        elif day == 2:
            self.svm_model = svm_load_model('PN_Prediction_Day2.model')
            self.optimal_threshold = 0.07
            self.max_prob = 0.17
        elif day == 3:
            self.svm_model = svm_load_model('PN_Prediction_Day3.model')
            self.optimal_threshold = 0.09
            self.max_prob = 0.16
        elif day == 4:
            self.svm_model = svm_load_model('PN_Prediction_Day4.model')
            self.optimal_threshold = 0.05
            self.max_prob = 0.10
        elif day == 5:
            self.svm_model = svm_load_model('PN_Prediction_Day5.model')
            self.optimal_threshold = 0.06
            self.max_prob = 0.11

        self.input_scale = [1] * 3
        self.input_min = [0, 11, -3]
        self.input_max = [17.5, 20.8, 3]

    # Run SVM Pseudo-nitzschia model. Model MUST be initialized before.
    def run_pseudonitzschia_model(self, input_data):
        p_label, p_val, n_invalid = self.run_model_probability(input_data)

        bloom_probability = p_val[0][0]
        p_label = int(p_label[0])

        if p_label == -1:
            p_label = 0

        p_label_opt = 0
        if bloom_probability >= self.optimal_threshold:
            p_label_opt = 1

        return bloom_probability, p_label, p_label_opt, n_invalid

    # Run SVM Pseudo-nitzschia predictioin model (ASP closure prediction model). Model MUST be initialized before.
    def run_pseudonitzschia_prediction_model(self, input_data):
        p_label, p_val, n_invalid = self.run_model_probability(input_data)
        svm_probability = p_val[0][0]
        asp_prob = 50 + (50 * ((svm_probability - self.optimal_threshold) / (self.max_prob - self.optimal_threshold)))
        asp_prob_simple = math.floor(asp_prob / 5) * 5
        return asp_prob, asp_prob_simple, n_invalid

    # Run SVM and Regression Alexandrium model, using Polymer 4.8. SVM Model MUST be initialized before.
    def run_alexandrium_model_48(self, input_data):

        # Define parameters for regression model. input_reg: Rw400, Rw412, Rw443, Rw490, Rw620, Rw665, Rw754
        input_reg = input_data[0:4] + input_data[6:9]
        input_reg_scale = self.input_scale[0:4] + self.input_scale[6:9]
        reg_coefs = [-0.046018953, 0.0448387529174285, -0.113172264857912, 0.152987206896707, -0.348795742576785,
                     0.303322567844891, 0.212070987608326]
        reg_intercept = 1.4325406856899

        # Run presence/absence model
        p_label, p_val, n_invalid = self.run_model_probability(input_data)

        presence_probability = p_val[0][0]
        p_label = int(p_label[0])

        if p_label == -1:
            p_label = 0

        p_label_opt = 0
        if presence_probability >= self.optimal_threshold:
            p_label_opt = 1

        # Regression model, only if p_label_opt=1
        abundance = 0
        if p_label_opt == 1:
            abundance = reg_intercept
            for i in range(len(input_reg)):
                input_reg[i] = input_reg[i] * input_reg_scale[i]
                abundance = abundance + (reg_coefs[i] * input_reg[i])

        return abundance, presence_probability, p_label, p_label_opt, n_invalid

    # Run SVM and Regression Alexandrium model, using Poymer 4.12. SVM Model MUST be initialized before.
    def run_alexandrium_model_412(self, input_data):
        # Define parameters for regression model. input_reg: Rw400,Rw412,Rw443,Rw490,Rw560,Rw620,Rw665,Rw681,Rw754
        input_reg = input_data[0:4] + input_data[5:9] + input_data[10:11]
        input_reg_scale = self.input_scale[0:4] + self.input_scale[5:9] + self.input_scale[10:11]
        reg_coefs = [-0.048935327, 0.12604099320132, -0.302947533300721, 0.236959191042387, 0.0506942592620245,
                     -0.49225716860401, 0.225291791325802, 0.105735728730446, 0.286734485662525]
        reg_intercept = 1.28978440193377

        # Run presence/absence model input_data: Rw400,Rw412,Rw443,Rw490,Rw510,Rw560,Rw620,Rw665,Rw681,Rw709,Rw754,Rw779x
        p_label, p_val, n_invalid = self.run_model_probability(input_data)

        presence_probability = p_val[0][0]
        p_label = int(p_label[0])

        if p_label == -1:
            p_label = 0

        p_label_opt = 0
        if presence_probability >= self.optimal_threshold:
            p_label_opt = 1

        # Regression model, only if p_label_opt=1
        abundance = 0
        if p_label_opt == 1:
            abundance = reg_intercept
            for i in range(len(input_reg)):
                input_reg[i] = input_reg[i] * input_reg_scale[i]
                abundance = abundance + (reg_coefs[i] * input_reg[i])

        return abundance, presence_probability, p_label, p_label_opt, n_invalid

    # Run SVM Probabilty model (General method)
    def run_model_probability(self, input_data):
        input_data_scaled = input_data
        n_invalid = 0
        for i in range(len(input_data)):
            input_data_scaled[i] = input_data[i] * self.input_scale[i]
            input_data_scaled[i] = (input_data_scaled[i] - self.input_min[i]) / (self.input_max[i] - self.input_min[i])
            if input_data_scaled[i] < 0:
                input_data_scaled[i] = 0
                n_invalid += 1
            elif input_data_scaled[i] > 1:
                input_data_scaled[i] = 1
                n_invalid += 1
        # print(input_data_scaled)
        x = [input_data_scaled]
        p_label, p_acc, p_val = svm_predict([], x, self.svm_model, '-b 1')
        return p_label, p_val, n_invalid


def checkPNModel_48():
    print("Example from file")
    csv_name = "D:\COAST_OBS\PSEUDO_NITZSCHIA_SENTINEL_4_8Improved\Modelos_DATOS_FIN\DATOS_FIN.csv"
    bp = list()
    bflag = list()
    svm = SVMLois()
    svm.init_pseudonitzschia_model_version48()
    with open(csv_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            elif line_count > 0:
                rsubset = row[7:20]
                inputdata = [float(v) for v in rsubset]
                print(inputdata)
                bloom_probability, p_label, p_label_opt, ninvalid = svm.run_pseudonitzschia_model(inputdata)
                bp.append(bloom_probability)
                bflag.append(ninvalid)
                line_count += 1
    for p in bp:
        print(p)


def checkPNModel_412():
    print("Example from file 4.12")
    csv_name = "D:\COAST_OBS\PSEUDO_NITZSCHIA_SENTINEL_4_12\Modelos_DATOS_FIN\DATOS_FIN.csv"
    bp = list()
    bflag = list()
    svm = SVMLois()
    svm.init_pseudonitzschia_model_version412()
    with open(csv_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            elif line_count > 0:
                rsubset = row[7:22]
                inputdata = [float(v) for v in rsubset]
                print(inputdata)
                bloom_probability, p_label, p_label_opt, ninvalid = svm.run_pseudonitzschia_model(inputdata)
                bp.append(bloom_probability)
                bflag.append(ninvalid)
                line_count += 1
    for p in bp:
        print(p)


def checkAlexModel_48():
    print("Example from file")
    csv_name = "D:\COAST_OBS\ALEXANDRIUM_MINUTUM\Modelos_PA_SVMJava\ALEXANDRIUM_PAQ9.csv"
    pp = list()
    pflag = list()
    svm = SVMLois()
    svm.init_alexandrium_model_version48()
    with open(csv_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            elif line_count > 0:
                rsubset = row[4:14]
                inputdata = [float(v) for v in rsubset]
                print(inputdata)
                presence_probability, p_label, p_label_opt, ninvalid = svm.run_alexandrium_model_48(inputdata)
                pp.append(presence_probability)
                pflag.append(ninvalid)
                line_count += 1
    for p in pp:
        print(p)


def checkAlexModel_412():
    print("Example from file")
    csv_name = "D:\COAST_OBS\ALEXANDRIUM_MINUTUM_412\Modelos_PA_SVMJava\ALEXANDRIUM_PA.csv"
    pp = list()
    pflag = list()
    svm = SVMLois()
    svm.init_alexandrium_model_version412()
    with open(csv_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            elif line_count > 0:
                rsubset = row[8:20]
                inputdata = [float(v) for v in rsubset]
                print(inputdata)
                abundance, presence_probability, p_label, p_label_opt, ninvalid = svm.run_alexandrium_model_412(
                    inputdata)
                pp.append(abundance)
                pflag.append(ninvalid)
                line_count += 1
    for p in pp:
        print(p)
