import math
class NNLois:
    def __init__(self):
        ## Create a empty NN ##
        self.wlayer1 = []
        self.biaslayer1 = []
        self.functionlayer1 = 'tansig'
        self.wlayer2 = []
        self.biaslayer2 = []
        self.functionlayer2 = 'tansig'
        self.woutput = []
        self.biasoutput = 0
        self.functionoutput = 'identity'
        self.minInput = []
        self.maxInput = []
        self.minDifAZ2 = -1
        self.maxDifAZ2 = -1
        self.minOutput = 0
        self.maxOutput = 10

    def prepareInput(self,input_data):
        # Scale input data
        input_data_scaled = []
        input_valid = [0] * len(input_data)
        ir = len(input_data)-4
        ifin = len(input_data)-1



        for i in range(len(input_data)):
            input_data_scaled.append((input_data[i] - self.minInput[i]) / (self.maxInput[i] - self.minInput[i]))
            if i <= ir and -0.3 <= input_data_scaled[i] <= 1.3:
                input_valid[i] = 1
            elif i > ir and 0 <= input_data_scaled[i] <= 1:
                input_valid[i] = 1

        # For difAZ, there are two possible input ranges. If difAZ is out of the first range(scaled between 0 and 1)
        # check if it's withing the second range (scaled between 1 and 2)
        if input_valid[ifin] == 0:
            input_data_scaled[ifin] = ((input_data[ifin] - self.minDifAZ2) / (self.maxDifAZ2 - self.minDifAZ2)) + 1
        if 1 <= input_data_scaled[ifin] <= 2:
            input_valid[ifin] = 1

        n_valid = sum(input_valid)

        return input_data_scaled, input_valid, n_valid



    def computeNodesLayer1(self,inputDataScale):
        nodeslayer1 = []
        for i in range(len(self.wlayer1)):
            nodeslayer1.append(0)
            for j in range(len(inputDataScale)):
                nodeslayer1[i] = nodeslayer1[i] + (self.wlayer1[i][j] * inputDataScale[j])

            nodeslayer1[i] = nodeslayer1[i] + self.biaslayer1[i]
            nodeslayer1[i] = self.aplicarFunction(nodeslayer1[i],self.functionlayer1)
        return nodeslayer1

    def computeNodesLayer2(self,nodeslayer1):
        nodeslayer2 = []
        for i in range(len(self.wlayer2)):
            nodeslayer2.append(0)
            for j in range(len(nodeslayer1)):
                nodeslayer2[i] = nodeslayer2[i] + (self.wlayer2[i][j] * nodeslayer1[j])
            nodeslayer2[i] = nodeslayer2[i] + self.biaslayer2[i]
            nodeslayer2[i] = self.aplicarFunction(nodeslayer2[i], self.functionlayer2)
        return nodeslayer2

    def computeNodeOutput(self,nodeslayer2):
        nodeoutput = 0
        for j in range(len(nodeslayer2)):
            nodeoutput = nodeoutput + (self.woutput[j]*nodeslayer2[j])
        nodeoutput = nodeoutput + self.biasoutput
        nodeoutput = self.aplicarFunction(nodeoutput,self.functionoutput)
        return nodeoutput

    def scaleOutputNode(self,nodeoutput):
        outputscaled = (nodeoutput*(self.maxOutput-self.minOutput))+self.minOutput
        return outputscaled

    def aplicarFunction(self,inputvalue,function):
        if function=='tansig':
            outputvalue = math.tanh(inputvalue)
        elif function=='purelin':
            outputvalue = inputvalue
        else:
            outputvalue = inputvalue
        return outputvalue