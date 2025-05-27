import numpy as  np

class Flags_General(object):

    def __init__(self,flagMeanings,flagValues,dtype):
        self.flagValues = flagValues
        self.flagMeanings = flagMeanings

        self.dtype = dtype if dtype is not None else 'int32'

    def Code(self, flagList):
        myCode = np.array(0).astype(self.dtype)
        for flag in flagList:
            myCode |= self.flagValues[self.flagMeanings.index(flag)]
        return myCode

    def Mask(self, flagValues, flagList):
        myCode = self.Code(flagList)
        flagValues = np.array(flagValues).astype(self.dtype)
        #flags = np.uint64(flags)
        # print(myCode,flags)
        #print(myCode)
        return np.bitwise_and(flagValues, myCode)

    def Decode(self, val):
        count = 0
        res = []
        mask = np.zeros(len(self.flagValues))
        for value in self.flagValues:
            if value & val:
                res.append(self.flagMeanings[count])
                mask[count] = 1
            count += 1
        return res, mask
