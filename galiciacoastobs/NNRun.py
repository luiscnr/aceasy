import NNDefinition
import FCM


def nnruncluster1(inputdata):
    ##Getting network...
    nn = NNDefinition.getNetCluster1()
    ##Run network
    outputvalid, output = nnrun(nn, inputdata)
    return outputvalid, output


def nnruncluster2(inputdata):
    ##Getting network
    nn = NNDefinition.getNetCluster2()
    ##Comput network
    outputvalid, output = nnrun(nn, inputdata)
    return outputvalid, output


def nnruncluster1Polymer412(inputdata):
    ##Getting network...
    nn = NNDefinition.getNetCluster1_Polymer412()
    ##Run network
    outputvalid, output = nnrun(nn, inputdata)
    return outputvalid, output


def nnruncluster2Polymer412(inputdata):
    ##Getting network
    nn = NNDefinition.getNetCluster2_Polymer412()
    ##Comput network
    outputvalid, output = nnrun(nn, inputdata)
    return outputvalid, output


def nnrun(nn, inputdata):
    nmax = len(inputdata)
    ##Preparing input data...
    inputdatascale, inputvalid, nvalid = nn.prepareInput(inputdata)



    ##Computing network
    nodeslayer1 = nn.computeNodesLayer1(inputdatascale)
    nodeslayer2 = nn.computeNodesLayer2(nodeslayer1)
    nodeoutput = nn.computeNodeOutput(nodeslayer2)

    ##Scaling output...
    output = nn.scaleOutputNode(nodeoutput)
    if output < nn.minOutput:
        output = nn.minOutput
    if output > nn.maxOutput:
        output = nn.maxOutput

    ##Check validity
    validrangein = 0
    if nvalid < nmax:
        validrangein = 1
    validrangeout = 0
    if output < nn.minOutput or output > nn.maxOutput:
        validrangeout = 2
    outputvalid = validrangein + validrangeout
    if outputvalid > 0:
        output = -1

    return outputvalid, output


class results:
    def __init__(self):
        ## Init results ##
        self.fcmcluster1 = -999
        self.fcmcluster2 = -999
        self.fcmid = -999
        self.nncluster1 = -999
        self.nncluster1valid = -999
        self.nncluster2 = -999
        self.nncluster2valid = -999
        self.nnfin = -999


def runall(inputdata):
    ##init results object
    res = results()

    ##clustering
    reflectances = inputdata[0:9]
    res.fcmcluster1, res.fcmcluster2, res.fcmid = FCM.fcm(reflectances)

    ##networks
    valid1, net1 = nnruncluster1(inputdata)
    valid2, net2 = nnruncluster2(inputdata)

    if res.fcmid == 1:  # Cluster 1 is predominant
        res.nncluster1 = net1
        res.nncluster1valid = valid1
        res.nncluster2 = -1
        res.nncluster2valid = valid2
        if valid2 == 0:
            res.nncluster2valid = 4

    if res.fcmid == 2:  # Cluster 2 is predominant
        res.nncluster2 = net2
        res.nncluster2valid = valid2
        res.nncluster1 = -1
        res.nncluster1valid = valid1
        if valid1 == 0:
            res.nncluster1valid = 4

    # nnfin, combines nn cluster 1 and nn cluster 2
    if valid1 == 0 and valid2 > 0:  # only cluster 1 is valid
        res.nnfin = res.nncluster1
    elif valid2 == 0 and valid1 > 0:  # only cluster 2 is valid
        res.nnfin = res.nncluster2
    elif valid1 == 0 and valid2 == 0:  # both clusters are valid
        res.nnfin = (net1 * res.fcmcluster1) + (net2 * res.fcmcluster2)
    else:
        res.nnfin = -1

    return res


def runall_polymer412(inputdata):
    ##init results object
    res = results()

    ##clustering
    reflectances = inputdata[0:11]
    res.fcmcluster1, res.fcmcluster2, res.fcmid = FCM.fcm_polymer412(reflectances)

    ##networks
    valid1, net1 = nnruncluster1Polymer412(inputdata)
    valid2, net2 = nnruncluster2Polymer412(inputdata)


    if res.fcmid == 1:  # Cluster 1 is predominant
        res.nncluster1 = net1
        res.nncluster1valid = valid1
        res.nncluster2 = -1
        res.nncluster2valid = valid2
        if valid2 == 0:
            res.nncluster2valid = 4

    if res.fcmid == 2:  # Cluster 2 is predominant
        res.nncluster2 = net2
        res.nncluster2valid = valid2
        res.nncluster1 = -1
        res.nncluster1valid = valid1
        if valid1 == 0:
            res.nncluster1valid = 4

    # nnfin, combines nn cluster 1 and nn cluster 2
    if valid1 == 0 and valid2 > 0:  # only cluster 1 is valid
        res.nnfin = res.nncluster1
    elif valid2 == 0 and valid1 > 0:  # only cluster 2 is valid
        res.nnfin = res.nncluster2
    elif valid1 == 0 and valid2 == 0:  # both clusters are valid
        res.nnfin = (net1 * res.fcmcluster1) + (net2 * res.fcmcluster2)
    else:
        res.nnfin = -1

    return res


def example_runall_polymer_48():
    # Example using polymer 4.8
    # inputData = ['Rw400','Rw412','Rw443','Rw490','Rw510','Rw560','Rw620','Rw665','Rw754','Rw779','sun_zenith','view_zenith','difAZ']
    inputdata = [0.015238104, 0.016272273, 0.015649043, 0.015554788, 0.014581564, 0.009821069, 0.0020103562,
                 0.0010373004, 0.0007033834, 0.00024644934, 38.52334, 47.997093, 28.987137]
    res = runall(inputdata)
    print("Example using Polymer 4.8")
    print("FCM Cluster 1: ", res.fcmcluster1)
    print("FCM Cluster 2: ", res.fcmcluster2)
    print("FCMID: ", res.fcmid)
    print("NN Cluster 1 Valid: ", res.nncluster1valid)
    print("NN Cluster 1: ", res.nncluster1)
    print("NN Cluster 2 Valid: ", res.nncluster2valid)
    print("NN Cluster 2: ", res.nncluster2)
    print("NN Fin: ", res.nnfin)
    print("")


def example_runall_polymer_412():
    # Example using polymer 4.12
    # inputData = ['Rw400','Rw412','Rw443','Rw490','Rw510','Rw560','Rw620','Rw665','Rw681','Rw709','Rw754','Rw779','sun_zenith','view_zenith','difAZ']
    inputdata = [0.00591314, 0.00748616, 0.00891449, 0.00922937, 0.00950514, 0.01023979, 0.00203152, -0.00020799,
                 0.00026918, -0.00114869, 0.00261384, 0.00166401, 29.49225616, 3.61376834, 30.56717682]
    res = runall_polymer412(inputdata)
    print("Example using Polymer 4.12")
    print("FCM Cluster 1: ", res.fcmcluster1)
    print("FCM Cluster 2: ", res.fcmcluster2)
    print("FCMID: ", res.fcmid)
    print("NN Cluster 1 Valid: ", res.nncluster1valid)
    print("NN Cluster 1: ", res.nncluster1)
    print("NN Cluster 2 Valid: ", res.nncluster2valid)
    print("NN Cluster 2: ", res.nncluster2)
    print("NN Fin: ", res.nnfin)
    print("")
