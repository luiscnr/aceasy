import math


def fcm(reflectances):
    # input: Reflectances values: 'Rw400','Rw412','Rw443','Rw490','Rw510','Rw560','Rw620','Rw665','Rw754','Rw779'
    # output: probabC1: membership degree for Cluster#1
    # 	 probabC2: membership degree for Cluster#2
    #	 ID: cluster with higher membership degreee (1 o 2)

    # centers of each cluster
    cluster1 = [0.0206719482308240, 0.0191768727517486, 0.0182695737517308, 0.0194040984265007, 0.0184823767275930,
                0.0133198559067629, 0.00281468735119556, 0.00137617751896151, 0.00103763508657573, 0.000618012539186834]
    cluster2 = [0.0116943178729165, 0.0112395705912906, 0.0113174551111310, 0.0123047534301389, 0.0123072242275690,
                0.0104473780453346, 0.00241182559235096, 0.00115049791603060, 0.00103156642850797, 0.000593277910230501]

    # exponent value
    exponent = 1.1
    elev = 1 / (exponent - 1)

    # compute membership degree for each cluster
    dsquared1 = dist(reflectances, cluster1) ** 2
    dsquared2 = dist(reflectances, cluster2) ** 2

    a1 = (1 / dsquared1) ** elev
    a2 = (1 / dsquared2) ** elev
    b = a1 + a2

    if dsquared1 == 0:
        probabC1 = 1
    else:
        probabC1 = a1 / b

    if dsquared2 == 0:
        probabC2 = 1
    else:
        probabC2 = a2 / b

    # compute ID
    if probabC1 >= probabC2:
        ID = 1
    else:
        ID = 2

    return probabC1, probabC2, ID


def fcm_polymer412(reflectances):
    # input: Reflectances values: 'Rw400','Rw412','Rw443','Rw490','Rw510','Rw560','Rw620','Rw665','Rw681','Rw709','Rw754','Rw779'
    # output: probabC1: membership degree for Cluster#1
    # 	 probabC2: membership degree for Cluster#2
    #	 ID: cluster with higher membership degreee (1 o 2)

    # centers of each cluster
    cluster1 = [0.00920542077796782, 0.00994096385130082, 0.0104953818973012, 0.0115673537071915, 0.0112375094121945,
                0.0105093949894695, 0.00259737685943150, 0.000942776280977560, 0.00162612554888361,
                -8.56444774596477e-05, 0.00122008668753178, 0.000766597782040885]
    cluster2 = [0.00371188123733899, 0.00493811238721051, 0.00616265237085688, 0.00687899634875919, 0.00711829578520000,
                0.00801503822295316, 0.00225326027415004, 0.000989627184435632, 0.00194717489207988,
                6.63444340050192e-05, 0.00129536411161785, 0.000751263312451555]

    # exponent value
    exponent = 1.1
    elev = 1 / (exponent - 1)

    # compute membership degree for each cluster
    dsquared1 = dist(reflectances, cluster1) ** 2
    dsquared2 = dist(reflectances, cluster2) ** 2

    a1 = (1 / dsquared1) ** elev
    a2 = (1 / dsquared2) ** elev
    b = a1 + a2

    if dsquared1 == 0:
        probabC1 = 1
    else:
        probabC1 = a1 / b

    if dsquared2 == 0:
        probabC2 = 1
    else:
        probabC2 = a2 / b

    # compute ID
    if probabC1 >= probabC2:
        ID = 1
    else:
        ID = 2

    return probabC1, probabC2, ID


def dist(x, y):
    # dist: euclidean dist between two points x and y
    distance = math.sqrt(sum([(a - b) ** 2 for a, b in zip(x, y)]))

    return distance


def main():
    # Example:
    # input = [0.01481405,0.01452708,0.01565176,0.01595119,0.01610003,0.01338701,0.003254490,0.00027118,0.00192815,0.00075385]
    # probabC1 = 0.358970726878029
    # probabC2 = 0.641029273121971
    # ID = 2
    probabC1, probabC2, ID = fcm(
        [0.01481405, 0.01452708, 0.01565176, 0.01595119, 0.01610003, 0.01338701, 0.003254490, 0.00027118, 0.00192815,
         0.00075385])
    print(probabC1, "\n")
    print(probabC2, "\n")
    print(ID)


if __name__ == "__main__":
    main()
