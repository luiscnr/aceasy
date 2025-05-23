import numpy as np


class Class_Flags_Polymer(object):

    def __init__(self, flag_band):

        desc = flag_band.description.split(',')
        self.mask_values = []
        self.mask_names = []

        for d in desc:
            dval = d.split(':')
            self.mask_values.append(int(dval[1].strip()))
            self.mask_names.append(dval[0].strip())

    def MaskGeneral(self, flags):
        flags = np.int64(flags)
        code = np.empty(flags.shape, dtype=np.int64)
        code[:] = 1023
        # print flags
        # print myCode
        return np.bitwise_and(flags, code)

    def MaskGeneralV5(self,flags):
        flags = np.int64(flags)
        res = np.ones(flags.shape)
        res[flags==0]=0
        res[flags==1024]=0
        return res

    def Code(self, maskList):
        myCode = np.int64(0)
        for flag in maskList:
            myCode |= self.maskValues[self.maskNames.index(flag)]
        return myCode

    def Mask(self, flags, maskList):
        myCode = self.Code(maskList)
        flags = np.int64(flags)
        # print flags
        # print myCode
        return np.bitwise_and(flags, myCode)

    def Decode(self, val):
        count = 0
        res = []
        mask = np.zeros(len(self.maskValues))
        for value in self.maskValues:
            if value & val:
                res.append(self.maskNames[count])
                mask[count] = 1
            count += 1
        return (res, mask)
