import numpy as np
import numpy.ma as ma
import sys, os
import nearest


def read_aw_coeff(outband):
    file_in = open('/store/woc/insitu/optics/script/aw_pope.txt', 'r')
    lines = file_in.readlines()
    wl = []
    aw = []
    for l in lines:
        wl.append(float(l.split()[0]))
        aw.append(float(l.split()[1]))
    return ma.array(np.interp(outband, wl, aw))


def read_bbw_coeff(outband):
    file_in = open('/store/woc/insitu/optics/script/bw_buiteveld.txt', 'r')
    lines = file_in.readlines()
    wl = []
    b = []
    for l in lines:
        wl.append(float(l.split()[0]))
        b.append(float(l.split()[1]))
    b = ma.array(b)
    wl = ma.array(wl)
    b = (b + b * 0.3) * 0.5
    return ma.array(np.interp(outband, wl, b))


def read_bricaud_aph_coeff(outband):
    file_in = open('/store/woc/insitu/optics/script/aph_bricaud_coeff.txt', 'r')
    lines = file_in.readlines()
    wl = []
    a1 = []
    a2 = []

    for l in lines:
        wl.append(float(l.split()[0]))
        a1.append(float(l.split()[1]))
        a2.append(float(l.split()[2]))
    a1 = ma.array(a1)
    a2 = ma.array(a2)
    wl = ma.array(wl)
    aph1 = np.interp(outband, wl, a1)
    aph2 = np.interp(outband, wl, a2)
    aph = ma.array([aph1, aph2])
    return aph


def qaa(rrs_in, band_in, g0, g1):
    rrs_in = ma.array(rrs_in)
    if len(rrs_in.shape) == 1:
        rrs_prov = ma.zeros((rrs_in.shape[0], 1, 1))
        rrs_prov[:, 0, 0] = rrs_in
        rrs_in = rrs_prov
    band_in = ma.array(band_in)
    # check_nan=ma.getmask(rrs_in)
    # rrs_in[ma.where(check_nan==True)]=-1.
    b555 = nearest.nearest(band_in, 555.)[0]  # the closest band to 555 nm
    if ma.abs(band_in[b555] - 555.) > 8.:  # the 555nm band must be close to b555
        sys.exit('no green band')
    Rrs670_max = 20. * rrs_in[b555] ** 1.5
    Rrs670_min = 0.9 * rrs_in[b555] ** 1.7  # two thresholds(TH) for Rrs670

    b670 = nearest.nearest(band_in, 670.)[0]  # the closest band to 555 nm
    if ma.abs(band_in[b670] - 670.) > 10.:  # the 670nm band must be close to b670
        sys.exit('no red band')
    b490 = nearest.nearest(band_in, 490.)[0]  # the closest band to 490 nm
    ind_670 = ma.where((rrs_in[b670] <= 0.) | (rrs_in[b670] > Rrs670_max) | (
                rrs_in[b670] < Rrs670_min))  # check where Rrs 670 is outside the TH
    rrs_in[b670][ind_670] = 1.27 * rrs_in[b555][ind_670] ** 1.47 + 0.00018 * (
                rrs_in[b490][ind_670] / rrs_in[b555][ind_670]) ** (-3.19)  # correct the outside TH values
    """if (rrs_in[b670]<= 0.) or (rrs_in[b670]>Rrs670_max) or (rrs_in[b670]<Rrs670_min):
        b490=nearest.nearest(band_in,490.)
        rrs_in[b670]=1.27*rrs_in[b555]**1.47+0.00018*(rrs_in[b490]/rrs_in[b555])**(-3.19)"""

    rrs0m = rrs_in / (0.52 + 1.7 * rrs_in)  # rrs below above water surface
    u = (-g0 + ma.sqrt(g0 ** 2 + 4 * g1 * rrs0m)) / (2 * g1)  # calculate the u function
    b443 = nearest.nearest(band_in, 443.)[0]
    b490 = nearest.nearest(band_in, 490.)[0]

    x = ma.log10((rrs0m[b443] + rrs0m[b490]) / (rrs0m[b555] + 5 * (rrs0m[b670] / rrs0m[b490]) * rrs0m[b670]))
    aw = read_aw_coeff(band_in)  # pure water abs
    h0 = -1.1459
    h1 = -1.3658
    h2 = -0.4693
    a0 = aw[b555] + 10 ** (h0 + h1 * x + h2 * x ** 2)
    bbw = read_bbw_coeff(band_in)
    bbp0 = ((u[b555] * a0) / (1 - u[b555])) - bbw[b555]  # calculate bbp0 and a0, for now l0 is 555nm
    lam0 = ma.zeros(bbp0.shape)
    lam0[:, :] = band_in[b555]

    # ind_670=None
    ind_670 = ma.where(rrs_in[b670] >= 0.0015)  # where rrs_in>0.0015 (TH2)
    lam0[ind_670] = band_in[b670]  # outside TH2 l0 is 670nm
    a0[ind_670] = aw[b670] + 0.39 * (rrs0m[b670][ind_670] / (rrs0m[b443][ind_670] + rrs0m[b490][ind_670])) ** 1.14
    bbp0[ind_670] = ((u[b670][ind_670] * a0[ind_670]) / (1 - u[b670][ind_670])) - bbw[
        b670]  # calculate new bbp0 and a0, where l0=670nm
    n = 2.0 * (1 - 1.2 * ma.exp(-0.9 * (rrs0m[b443] / rrs0m[b555])))  # bbp spectral slope
    bbp = ma.zeros(rrs0m.shape)
    # bbp0_e=ma.zeros(rrs0m.shape)
    mask_iop = ma.getmask(rrs_in)
    bbp0_e = ma.array([list(bbp0)] * rrs0m.shape[0])
    bbp0_e = ma.array(bbp0_e, mask=mask_iop)
    n_e = ma.array([list(n)] * rrs0m.shape[0])
    n_e = ma.array(n_e, mask=mask_iop)
    lam_e = ma.array([list(lam0)] * rrs0m.shape[0])  # we reshape some arrays for the next calculations
    lam_e = ma.array(lam_e, mask=mask_iop)
    # bbp_sec=ma.transpose(ma.transpose(lam_e)/band_in)**n_e
    bbp = ma.zeros(rrs_in.shape)
    for i in range(bbp0.shape[0]):
        for j in range(bbp0.shape[1]):
            bbp[:, i, j] = bbp0[i, j] * (lam0[i, j] / band_in) ** n[i, j]
    # bbp=ma.transpose(bbp0_e)*ma.transpose(bbp_sec) #bbp at each band
    # bbp=ma.transpose(bbp)

    bbp443 = bbp[b443]
    bbp470 = bbp0 * (lam0 / 470.) ** n
    a = ma.zeros(bbp.shape)
    bbw_e = ma.transpose(
        [[list(bbw)] * rrs0m.shape[1]] * rrs0m.shape[2])  # we reshape some arrays for the next calculations
    bbw_e = ma.array(bbw_e, mask=mask_iop)
    a = (1 - u) * (bbw_e + bbp) / u  # total abs
    zit = 0.74 + (0.2 / (0.8 + rrs0m[b443] / rrs0m[b555]))
    s = 0.015 + (0.002 / (0.6 + rrs0m[b443] / rrs0m[b555]))  # adg spectral slope
    z = ma.exp(s * (442.5 - 415.5))
    b412 = nearest.nearest(band_in, 412.)[0]
    adg443 = ((a[b412] - zit * a[b443]) - (aw[b412] - zit * aw[b443])) / (z - zit)
    aph443 = a[b443] - adg443 - aw[b443]  # calculate adg and aph at 443 nm
    x1 = aph443 / a[b443]
    # if x1 outside some values we recalculate aph and adg
    x1ind = ma.where((x1 < 0.15) | (x1 > 0.6))
    x2 = ma.zeros(x1.shape)
    x2[x1ind] = -0.8 + 1.4 * (a[b443][x1ind] - aw[b443] / a[b412][x1ind] - aw[b412])
    x2[ma.where(x2 < 0.15)] = 0.15
    x2[ma.where(x2 > 0.6)] = 0.6
    x2 = ma.array(x2, mask=ma.getmask(bbp0))
    aph443[x1ind] = a[b443][x1ind] * x2[x1ind]
    adg443[x1ind] = a[b443][x1ind] - aph443[x1ind] - aw[b443]
    # we want to calculate a555; if 555nm is not in band_in we recalculate adg,aph and aw at 555nm to obtain a555
    """if band_in[b555]!=555.:
        adg555=adg443*ma.exp(-s*(551.-443.))	
        aphcoef555=read_bricaud_aph_coeff(ma.array([551.]))
        aphcoef443=read_bricaud_aph_coeff(ma.array([443.]))
        aph555=aphcoef555[0]*(aph443/aphcoef443[0])**((1-aphcoef555[1])/(1-aphcoef443[1]))
        aw555=read_aw_coeff(ma.array(551.))
        a555=adg555+aph555+aw555
    else:
        a555=a[b555]"""
    a555 = a[b555]
    bbp555 = bbp0 * (lam0 / band_in[b555]) ** n
    np.set_printoptions(threshold=sys.maxsize)
    out = {'bbp443': bbp443, 'adg443': adg443, 'aph443': aph443, 'lambda0': lam0, 'eta': n, 's': s, 'a555': a555,
           'bbp0': bbp0, 'a0': a0}
    return out


def bsc_qaa(rrs_in, band_in, band_out, g0=0.089, g1=0.1245):
    if len(rrs_in.shape) == 1:
        shaper = 1
        rrs_prov = ma.zeros((rrs_in.shape[0], 1, 1))
        rrs_prov[:, 0, 0] = rrs_in
        rrs_in = rrs_prov
    if len(rrs_in.shape) == 2:
        shaper = 2
        rrs_prov = ma.zeros((rrs_in.shape[0], rrs_in.shape[2], 1))
        rrs_prov[:, :, 0] = rrs_in
        rrs_in = rrs_prov

    res = qaa(rrs_in, band_in, g0, g1)  # qaa in backward mode to evaluate the parameters
    b_tmp = list(band_in) + list(band_out)
    band_all = sorted(list(set(b_tmp)))  # list with input and output bands
    band_all = ma.array(band_all)
    """bbp=ma.zeros((band_all.shape[0],rrs_in.shape[1],rrs_in.shape[2]))
    adg=ma.zeros((band_all.shape[0],rrs_in.shape[1],rrs_in.shape[2]))
    aph=ma.zeros((band_all.shape[0],rrs_in.shape[1],rrs_in.shape[2]))
    rrs0m=ma.zeros((band_all.shape[0],rrs_in.shape[1],rrs_in.shape[2]))
    rrs_out=ma.zeros((band_out.shape[0],rrs_in.shape[1],rrs_in.shape[2]))"""
    rrs_out = ma.zeros((band_out.shape[0], rrs_in.shape[1], rrs_in.shape[2]))
    rrs0m = ma.zeros((band_all.shape[0], rrs_in.shape[1], rrs_in.shape[2]))  # we initialize some arrays
    n = res['eta']
    bbpr = res['bbp0']
    lamr = res['lambda0']
    adgr = res['adg443']
    aphr = res['aph443']
    s = res['s']  # we define some QAA-derived variabes
    bbp0_e = ma.array([list(bbpr)] * rrs0m.shape[0])
    n_e = ma.array([list(n)] * rrs0m.shape[0])
    lam_e = ma.array([list(lamr)] * rrs0m.shape[0])
    adg_e = ma.array([list(adgr)] * rrs0m.shape[0])
    s_e = ma.array([list(s)] * rrs0m.shape[0])
    aph_e = ma.array([list(aphr)] * rrs0m.shape[0])  # we reshape some arrays for the next calculations
    aphcoef = read_bricaud_aph_coeff(band_all)  # specific phyto abs
    bbp = ma.zeros(rrs0m.shape)
    adg = ma.zeros(rrs0m.shape)
    aph = ma.zeros(rrs0m.shape)
    b443 = nearest.nearest(band_all, 443.)[0]
    for i in range(bbpr.shape[0]):
        for j in range(bbpr.shape[1]):
            bbp[:, i, j] = bbpr[i, j] * (lamr[i, j] / band_all) ** n[i, j]
            adg[:, i, j] = adgr[i, j] * ma.exp(-s[i, j] * (band_all - 443.))
            aph[:, i, j] = aphcoef[0, :] * (aphr[i, j] / aphcoef[0, b443]) ** (
                        (1 - aphcoef[1, :]) / (1 - aphcoef[1, b443]))
    # prod=aphcoef[0,:]*((aph_e/aphcoef[0,b443]))

    # prod_e=ma.array([list(prod)]*rrs0m.shape[0])
    bbw = read_bbw_coeff(band_all)  # pure water  backscattering
    bbw_e = ma.transpose([[list(bbw)] * bbp.shape[1]] * bbp.shape[2])  # we reshape the array for the next calculations
    bb = bbp + bbw_e  # total backscattering
    aw = read_aw_coeff(band_all)  # pure water abs
    aw_e = ma.transpose([[list(aw)] * bbp.shape[1]] * bbp.shape[2])  # we reshape the array for the next calculations
    a = adg + aph + aw_e  # total abs
    b555 = nearest.nearest(band_all, 555.)
    rrs0m = g0 * (bb / (bb + a)) + g1 * (bb / (bb + a)) ** 2.
    rrsf = 0.52 * rrs0m / (1. - 1.7 * rrs0m)  # qaa in forward mode
    rrs_tmp1 = np.zeros(len(band_out))
    rrs_tmp2 = np.zeros(len(band_out))
    for (i, band) in zip(range(len(band_out)), band_out):
        b_all = nearest.nearest(band_all, band)[0]
        b_in = nearest.nearest(band_in, band)
        # rrs if just one nearest band
        if len(b_in) == 1:
            ballin = nearest.nearest(band_all, band_in[b_in[0]])
            rrs_out[i] = rrsf[b_all] * (rrs_in[b_in[0]] / rrsf[ballin])
        # rrs if two nearest bands
        else:
            ballin = nearest.nearest(band_all, band_in[b_in[0]])
            rrs_tmp1[i] = rrsf[b_all] * (rrs_in[b_in[0]] / rrsf[ballin])
            ballin = nearest.nearest(band_all, band_in[b_in[1]])
            rrs_tmp2[i] = rrsf[b_all] * (rrs_in[b_in[1]] / rrsf[ballin])
            rrs_out[i] = (rrs_tmp1 + rrs_tmp2) / 2
    for (i, band) in zip(range(len(band_out)), band_out):
        # if deltal between band_out and nearest band_in >15 nm we recalculate rrs
        if ma.min(ma.abs(band - band_in)) > 15.:
            b = ma.where(band_all == band)
            if b[0][0] != 0 and b[0][0] != len(band_all) - 1:
                cac = ma.where(band_in - band_all[b] > 0.)
                bin_pre = cac[0][0] - 1
                bin_2 = []

                bin_2 = ma.where(band_all == band_in[bin_pre])
                rrs_pre = rrsf[b] * (rrs_in[bin_pre] / rrsf[bin_2])
                bin_post = cac[0][0]
                # bin_3=[]
                # for b_in in band_in[bin_post]:
                bin_3 = ma.where(band_all == band_in[bin_post])
                rrs_post = rrsf[b] * (rrs_in[bin_post] / rrsf[bin_3])
                rrs_out[i] = ((band_all[bin_3] - band_all[b]) * rrs_pre + (
                            band_all[b] - band_all[bin_2]) * rrs_post) / (band_all[bin_3] - band_all[bin_2])
    return rrs_out


def bsc_qaa_iop(rrs_in, band_in, band_out, g0=0.089, g1=0.1245):
    if len(rrs_in.shape) == 1:
        rrs_prov = ma.zeros((rrs_in.shape[0], 1, 1))
        rrs_prov[:, 0, 0] = rrs_in
        rrs_in = rrs_prov
    res = qaa(rrs_in, band_in, g0, g1)  # qaa in backward mode to evaluate the parameters
    b_tmp = list(band_in) + list(band_out)
    band_all = sorted(list(set(b_tmp)))  # list with input and output bands
    band_all = ma.array(band_all)
    """bbp=ma.zeros((band_all.shape[0],rrs_in.shape[1],rrs_in.shape[2]))
    adg=ma.zeros((band_all.shape[0],rrs_in.shape[1],rrs_in.shape[2]))
    aph=ma.zeros((band_all.shape[0],rrs_in.shape[1],rrs_in.shape[2]))
    rrs0m=ma.zeros((band_all.shape[0],rrs_in.shape[1],rrs_in.shape[2]))
    rrs_out=ma.zeros((band_out.shape[0],rrs_in.shape[1],rrs_in.shape[2]))"""
    rrs_out = ma.zeros((band_out.shape[0], rrs_in.shape[1], rrs_in.shape[2]))
    rrs0m = ma.zeros((band_all.shape[0], rrs_in.shape[1], rrs_in.shape[2]))  # we initialize some arrays
    n = res['eta']
    bbpr = res['bbp0']
    lamr = res['lambda0']
    adgr = res['adg443']
    aphr = res['aph443']
    s = res['s']  # we define some QAA-derived variabes
    bbp0_e = ma.array([list(bbpr)] * rrs0m.shape[0])
    n_e = ma.array([list(n)] * rrs0m.shape[0])
    lam_e = ma.array([list(lamr)] * rrs0m.shape[0])
    adg_e = ma.array([list(adgr)] * rrs0m.shape[0])
    s_e = ma.array([list(s)] * rrs0m.shape[0])
    aph_e = ma.array([list(aphr)] * rrs0m.shape[0])  # we reshape some arrays for the next calculations
    aphcoef = read_bricaud_aph_coeff(band_all)  # specific phyto abs
    bbp = ma.zeros(rrs0m.shape)
    adg = ma.zeros(rrs0m.shape)
    aph = ma.zeros(rrs0m.shape)
    b443 = nearest.nearest(band_all, 443.)[0]
    for i in range(bbpr.shape[0]):
        for j in range(bbpr.shape[1]):
            bbp[:, i, j] = bbpr[i, j] * (lamr[i, j] / band_all) ** n[i, j]
            adg[:, i, j] = adgr[i, j] * ma.exp(-s[i, j] * (band_all - 443.))
            aph[:, i, j] = aphcoef[0, :] * (aphr[i, j] / aphcoef[0, b443]) ** (
                        (1 - aphcoef[1, :]) / (1 - aphcoef[1, b443]))
    # prod=aphcoef[0,:]*((aph_e/aphcoef[0,b443]))

    # prod_e=ma.array([list(prod)]*rrs0m.shape[0])
    bbw = read_bbw_coeff(band_all)  # pure water  backscattering
    bbw_e = ma.transpose([[list(bbw)] * bbp.shape[1]] * bbp.shape[2])  # we reshape the array for the next calculations
    bb = bbp + bbw_e  # total backscattering
    aw = read_aw_coeff(band_all)  # pure water abs
    aw_e = ma.transpose([[list(aw)] * bbp.shape[1]] * bbp.shape[2])  # we reshape the array for the next calculations
    a = adg + aph + aw_e  # total abs
    b555 = nearest.nearest(band_all, 555.)
    rrs0m = g0 * (bb / (bb + a)) + g1 * (bb / (bb + a)) ** 2.
    rrsf = 0.52 * rrs0m / (1. - 1.7 * rrs0m)  # qaa in forward mode
    rrs_tmp1 = np.zeros(len(band_out))
    rrs_tmp2 = np.zeros(len(band_out))
    for (i, band) in zip(range(len(band_out)), band_out):
        b_all = nearest.nearest(band_all, band)[0]
        b_in = nearest.nearest(band_in, band)
        # rrs if just one nearest band
        if len(b_in) == 1:
            ballin = nearest.nearest(band_all, band_in[b_in[0]])
            rrs_out[i] = rrsf[b_all] * (rrs_in[b_in[0]] / rrsf[ballin])
        # rrs if two nearest bands
        else:
            ballin = nearest.nearest(band_all, band_in[b_in[0]])
            rrs_tmp1[i] = rrsf[b_all] * (rrs_in[b_in[0]] / rrsf[ballin])
            ballin = nearest.nearest(band_all, band_in[b_in[1]])
            rrs_tmp2[i] = rrsf[b_all] * (rrs_in[b_in[1]] / rrsf[ballin])
            rrs_out[i] = (rrs_tmp1 + rrs_tmp2) / 2
    for (i, band) in zip(range(len(band_out)), band_out):
        # if deltal between band_out and nearest band_in >15 nm we recalculate rrs
        if ma.min(ma.abs(band - band_in)) > 15.:
            b = ma.where(band_all == band)
            if b[0][0] != 0 and b[0][0] != len(band_all) - 1:
                cac = ma.where(band_in - band_all[b] > 0.)
                bin_pre = cac[0][0] - 1
                bin_2 = []

                bin_2 = ma.where(band_all == band_in[bin_pre])
                rrs_pre = rrsf[b] * (rrs_in[bin_pre] / rrsf[bin_2])
                bin_post = cac[0][0]
                # bin_3=[]
                # for b_in in band_in[bin_post]:
                bin_3 = ma.where(band_all == band_in[bin_post])
                rrs_post = rrsf[b] * (rrs_in[bin_post] / rrsf[bin_3])
                rrs_out[i] = ((band_all[bin_3] - band_all[b]) * rrs_pre + (
                            band_all[b] - band_all[bin_2]) * rrs_post) / (band_all[bin_3] - band_all[bin_2])
    return rrs_out, res