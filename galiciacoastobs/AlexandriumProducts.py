import SVMGeneral


# Alexandrium minutum species indicator product (abundance output) (Polymer 4.8)
def run_species_indicator_polymer48(input_data, svm=None):
    if svm is None:
        svm = SVMGeneral.SVMLois()
        svm.init_alexandrium_model_version48()
    abundance, presence_probability, p_label, p_label_opt, quality_flag = svm.run_alexandrium_model_48(input_data)
    if quality_flag > 2:
        abundance = -1
        p_label_opt = -1
    return abundance, p_label_opt, quality_flag


# Alexandrium minutum species indicator product (abundance output) (Polymer 4.12)
def run_species_indicator_polymer412(input_data, svm=None):
    if svm is None:
        svm = SVMGeneral.SVMLois()
        svm.init_alexandrium_model_version412()
    abundance, presence_probability, p_label, p_label_opt, quality_flag = svm.run_alexandrium_model_412(input_data)
    if quality_flag > 2:
        abundance = -1
        p_label_opt = -1
    return abundance, p_label_opt, quality_flag


# Alexandrium minutum higher-level species indicator (bloom probability output)
def run_higher_level_species_indicator(p_label_opt, temperature):
    prob_output = -1
    if p_label_opt == 1 and temperature >= 19:
        prob_output = 1
    else:
        prob_output = 0
    return prob_output
