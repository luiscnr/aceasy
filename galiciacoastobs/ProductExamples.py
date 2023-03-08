import NNRun
import PseudoNitzschiaProducts
import AlexandriumProducts



def example_chla_polymer48():
    # Example chla product using Polymer 4.8
    # inputData = ['Rw400','Rw412','Rw443','Rw490','Rw510','Rw560','Rw620','Rw665','Rw754','Rw779','sun_zenith','view_zenith','difAZ']
    # ouptutData: chla concentration. Example: 2.22
    input_data = [0.015238104, 0.016272273, 0.015649043, 0.015554788, 0.014581564, 0.009821069, 0.0020103562,
                  0.0010373004, 0.0007033834, 0.00024644934, 38.52334, 47.997093, 28.987137]
    res = NNRun.runall(input_data)
    print("NN Chla (Polymer 4.8): ", res.nnfin)


def example_chla_polymer412():
    # Example chla product using Polymer 4.12
    # inputData = ['Rw400','Rw412','Rw443','Rw490','Rw510','Rw560','Rw620','Rw665','Rw681','Rw709','Rw754','Rw779','sun_zenith','view_zenith','difAZ']
    # ouptutData: chla concentration. Example: 3.78
    input_data = [0.00591314, 0.00748616, 0.00891449, 0.00922937, 0.00950514, 0.01023979, 0.00203152, -0.00020799,
                  0.00026918, -0.00114869, 0.00261384, 0.00166401, 29.49225616, 3.61376834, 30.56717682]
    res = NNRun.runall_polymer412(input_data)
    print("NN Chla (Polymer 4.12): ", res.nnfin)


def example_pseudonitzschia_species_indicator_polymer48():
    # Example pseudonitzschia species indicator using Polymer 4.8 :
    # inputdata = [Rw400,Rw412,Rw443,Rw490,Rw510,Rw560,Rw620,Rw665,Rw754,Rw779,sun_zenith,view_zenith,difAZ]
    # output = bloom_probability. Example: 0.663026
    input_data = [0.00859741, 0.00862197, 0.00922034, 0.01118645, 0.01165646, 0.01202171, 0.00321148, 0.00177493,
                  0.00101687, 0.00063727, 28.26464653, 16.69105148, 21.87540436]
    bloom_probability, quality_flag = PseudoNitzschiaProducts.run_species_indicator_polymer48(input_data)
    print("Pseudo-nitzschia spp. species indicator (Polymer 4.8) Bloom probability: ", bloom_probability)


def example_pseudonitzschia_species_indicator_polymer412():
    # Example pseudonitzschia species indicator using Polymer 4.12 :
    # inputdata = [Rw400,Rw412,Rw443,Rw490,Rw510,Rw560,Rw620,Rw665,Rw681,Rw709,Rw754,Rw779,sun_zenith,view_zenith,difAZ]
    # output = bloom_probability. Example: 0.761233
    input_data = [0.00263883, 0.00337704, 0.00499732, 0.00594555, 0.00544532, 0.00574698, 0.00156302, 0.00058857,
                  0.00093849, -0.00003892, 0.00065834, 0.00047325, 28.04865074, 16.25000381, 21.19128418]
    bloom_probability, quality_flag = PseudoNitzschiaProducts.run_species_indicator_polymer412(input_data)
    print("Pseudo-nitzschia spp. species indicator (Polymer 4.12) Bloom probability: ", bloom_probability)


def example_alexandrium_species_indicator_polymer48():
    # Example Alexandrium minutum species indicator using Polymer 4.8:
    # inputdata = [Rw400,Rw412,Rw443,Rw490,Rw510,Rw560,Rw620,Rw665,Rw754,Rw779]
    # outputdata = abundance. Example: 6.147029385555081
    input_data = [0.01157851, 0.01055063, 0.01051679, 0.01126224, 0.01196015, 0.01173345, 0.0019352, 0.00039415,
                  0.00261171, 0.00164409]
    abundance, p_label_opt, quality_flag = AlexandriumProducts.run_species_indicator_polymer48(input_data)
    print("Alexandrium minutum species indicator (Polymer 4.8) Abundance: ", abundance)


def example_alexandrium_species_indicator_polymer412():
    # Example Alexandrium minutum species indicator using Polymer 4.12:
    # inputdata = [Rw400,Rw412,Rw443,Rw490,Rw510,Rw560,Rw620,Rw665,Rw681,Rw709,Rw754,Rw779]
    # outputdata = abudance. Example: 3.9449655549816143
    input_data = [0.00244506, 0.00375629, 0.00676777, 0.0057539, 0.00413341, 0.00321948, -0.00005107, -0.00054676,
                  0.00018837, -0.00115067, 0.00179077, 0.00096569]
    abundance, p_label_opt, quality_flag = AlexandriumProducts.run_species_indicator_polymer412(input_data)
    print("Alexandrium minutum species indicator (Polymer 4.12) Abundance: ", abundance)


def example_pseudonitzschia_higher_level_species_indicator_polymer48():
    # Example pseudonitzschia higher-level species indicator using Polymer 4.12 :
    # Input (for each production area): svm_prob (production area), temperature (ria), chla (ria), ui10
    # Output: bloom probability (simplified value) for each production area. 75% in this example

    svm_prob = 0.451589349
    temperature = 18.09654301
    chla = 3.6965
    ui10 = 2.020930335
    pnprob = PseudoNitzschiaProducts.run_higher_level_species_indicator_version48(svm_prob, temperature, chla, ui10)
    print("Pseudo-nitzschia higher level spp. species indicator (Polymer 4.8) Bloom probability for a production area:",
          pnprob, "%")


def example_pseudonitzschia_higher_level_species_indicator_polymer412():
    # Example pseudonitzschia higher-level species indicator using Polymer 4.12 :
    # Input (for each production area): svm_prob (production area), temperature (ria), chla (ria), ui10
    # Output: bloom probability (simplified value) for each production area. 100% in this example
    svm_prob = 0.561346533
    temperature = 18.09654301
    chla = 3.6965
    ui10 = 2.020930335
    pnprob = PseudoNitzschiaProducts.run_higher_level_species_indicator_version412(svm_prob, temperature, chla, ui10)
    print(
        "Pseudo-nitzschia higher level spp. species indicator (Polymer 4.12) Bloom probability for a production area:",
        pnprob, "%")


def example_pseudonitzschia_higher_level_prediction_product():
    # Example pseudonizschia_higher higher level prediction product
    # Input: chla, temperature, ui10 for a ria
    # Output: asp_prob_simple: probability of ASP closure for a given ria

    chla = 2.972
    temperature = 12.4098
    ui10 = 0.00754043
    asp_prob_simple1, quality_flag1 = PseudoNitzschiaProducts.run_prediction_product_day1(chla, temperature, ui10)
    asp_prob_simple2, quality_flag2 = PseudoNitzschiaProducts.run_prediction_product_day2(chla, temperature, ui10)
    asp_prob_simple3, quality_flag3 = PseudoNitzschiaProducts.run_prediction_product_day3(chla, temperature, ui10)
    asp_prob_simple4, quality_flag4 = PseudoNitzschiaProducts.run_prediction_product_day4(chla, temperature, ui10)
    asp_prob_simple5, quality_flag5 = PseudoNitzschiaProducts.run_prediction_product_day5(chla, temperature, ui10)
    print('Prediction day 1: ', asp_prob_simple1)
    print('Prediction day 2: ', asp_prob_simple2)
    print('Prediction day 3: ', asp_prob_simple3)
    print('Prediction day 4: ', asp_prob_simple4)
    print('Prediction day 5: ', asp_prob_simple5)


def example_alexandrium_higher_level_species_indicator():
    # Example Alexandrium higher-level species indicator.
    # Input: p_label_opt (1 ó 0), temperature
    # p_label_opt deriva de AlexandriumProducts.run_species_indicator_polymer4x
    # Output: Simplified bloom probability (1 ó 0)
    p_label_opt = 1
    temperature = 18.54
    prob_output = AlexandriumProducts.run_higher_level_species_indicator(p_label_opt, temperature)
    print("Pseudo-nitzschia higher level spp. species indicator (Polymer 4.12) Bloom probability: ", prob_output)


def main():
    print('Running examples: ')

    print('Innovative products: ')
    example_chla_polymer48()
    example_chla_polymer412()
    example_pseudonitzschia_species_indicator_polymer48()
    example_pseudonitzschia_species_indicator_polymer412()
    example_alexandrium_species_indicator_polymer48()
    example_alexandrium_species_indicator_polymer412()

    print('')

    print('Higher-level products: ')
    example_pseudonitzschia_higher_level_species_indicator_polymer48()
    example_pseudonitzschia_higher_level_species_indicator_polymer412()
    example_alexandrium_higher_level_species_indicator()
    example_pseudonitzschia_higher_level_prediction_product()


if __name__ == "__main__":
    main()
