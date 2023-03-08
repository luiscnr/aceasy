import SVMGeneral


# Pseudo-nitzschia species indicator product (Bloom probability output) (Polymer 4.8)
def run_species_indicator_polymer48(input_data, svm=None):
    if svm is None:
        svm = SVMGeneral.SVMLois()
        svm.init_pseudonitzschia_model_version48()
    bloom_probability, p_label, p_label_opt, quality_flag = svm.run_pseudonitzschia_model(input_data)
    if quality_flag > 2:
        bloom_probability = -1
    return bloom_probability, quality_flag


# Pseudo-nitzschia species indicator product (Bloom probability output) (Polymer 4.12)
def run_species_indicator_polymer412(input_data, svm=None):
    if svm is None:
        svm = SVMGeneral.SVMLois()
        svm.init_pseudonitzschia_model_version412()
    bloom_probability, p_label, p_label_opt, quality_flag = svm.run_pseudonitzschia_model(input_data)
    if quality_flag > 2:
        bloom_probability = -1
    return bloom_probability, quality_flag


# Pseudo-nitschia higher-level species indicator (Simplified bloom probability for each production area) (Polymer 4.8)
def run_higher_level_species_indicator_version48(svm_prob, temperature, chla, ui10):
    pn_abundance = 2.073015084 + (2.259130469 * svm_prob) + (0.084364287 * temperature) + (0.077586382 * chla) + (
            0.132213825 * ui10)
    pn_prob = get_simplified_probability(pn_abundance)
    return pn_prob


# Pseudo-nitschia higher-level species indicator (Simplified bloom probability for each production area) (Polymer 4.12)
def run_higher_level_species_indicator_version412(svm_prob, temperature, chla, ui10):
    pn_abundance = 2.097780131 + (2.503000933 * svm_prob) + (0.081964475 * temperature) + (0.074818865 * chla) + (
            0.12669096 * ui10)
    pn_prob = get_simplified_probability(pn_abundance)
    return pn_prob


# Pseudo-niztschia higher-level prediction product for day +1 (simplified ASP closure probability for echa ria)
def run_prediction_product_day1(chla, temperature, ui10):
    return run_prediction_product_day(chla, temperature, ui10, 1)


# Pseudo-niztschia higher-level prediction product for day +2 (simplified ASP closure probability for echa ria)
def run_prediction_product_day2(chla, temperature, ui10):
    return run_prediction_product_day(chla, temperature, ui10, 2)


# Pseudo-niztschia higher-level prediction product for day +3 (simplified ASP closure probability for echa ria)
def run_prediction_product_day3(chla, temperature, ui10):
    return run_prediction_product_day(chla, temperature, ui10, 3)


# Pseudo-niztschia higher-level prediction product for day +4 (simplified ASP closure probability for echa ria)
def run_prediction_product_day4(chla, temperature, ui10):
    return run_prediction_product_day(chla, temperature, ui10, 4)


# Pseudo-niztschia higher-level prediction product for day +5 (simplified ASP closure probability for echa ria)
def run_prediction_product_day5(chla, temperature, ui10):
    return run_prediction_product_day(chla, temperature, ui10, 5)


# Pseudo-niztschia higher-level prediction product (simplified ASP closure probability for echa ria)
def run_prediction_product_day(chla, temperature, ui10, iday):
    svm = SVMGeneral.SVMLois()
    svm.init_pseudonitzschia_prediction_model(iday)
    input_data = [chla, temperature, ui10]
    asp_prob, asp_prob_simple, quality_flag = svm.run_pseudonitzschia_prediction_model(input_data)
    if quality_flag > 0:
        asp_prob_simple = -1

    return asp_prob_simple, quality_flag


def get_simplified_probability(pn_abundance):
    pn_prob = 0
    if pn_abundance < 3:
        pn_prob = 0
    elif 3 <= pn_abundance < 4:
        pn_prob = 25
    elif 4 <= pn_abundance < 5:
        pn_prob = 50
    elif 5 <= pn_abundance < 5.5:
        pn_prob = 75
    elif pn_abundance >= 5.5:
        pn_prob = 100
    return pn_prob
