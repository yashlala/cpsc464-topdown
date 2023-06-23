#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 25 11:18:59 2020

@author: user007 
"""


import numpy as np
from scipy.optimize import OptimizeResult
import cvxpy as cp
import scipy.sparse as ss


def cdf(x, mu, epsilon):
    t = np.exp(-np.abs(x-mu) * epsilon)/2
    if x < mu:
        return(t)
    return(1-t)

def makeW(n1d, d):
    nCells = n1d ** d
    W = np.vstack(( np.ones(( 1, nCells )), np.zeros((n1d ** (d-1) * d, nCells))))
    inds = np.array(list(range( nCells)), np.int32)
    inds = inds.reshape(tuple([n1d] * d))

    curInd = 1
    for k in range(d):
        inds = np.rollaxis(inds,d-1)
        for index in np.ndindex(*tuple([n1d]*(d-1))):
            W[curInd, inds[index]] = 1.
            curInd = curInd + 1
    W = np.vstack((W, np.identity(nCells)))
    return(W)


def fit(dpqueries, equalities = list(), inequalities=list(), bound = (None, None), init=None, maxfit=False, AnnhilatorMat=None,sumConstr=None):
    """dpqueries is [(numpy_matrix, answers) or (numpy, answers, weight)] and constraints are
    [(numpy, rhs)], inequality means >= """
    aggfun = lambda x: cp.pnorm(x, p=2)
    A = ss.csr_matrix(np.vstack(tuple([q[0] for q in dpqueries])).astype(np.double))
    b = np.hstack(tuple([q[1] for q in dpqueries])).astype(np.double)

    x = cp.Variable(A.shape[1], value=init)

    if AnnhilatorMat is not None:
        objective = aggfun(A @ x - b) + aggfun(AnnhilatorMat @ x)
    else:
        objective = aggfun(A @ x - b)

    constraints = []
    if sumConstr is not None:
        constraints.append( cp.sum(x) <= sumConstr )
    if bound[0] is not None:
        constraints.append(x >= bound[0])
    if bound[1] is not None:
        constraints.append(x <= bound[1])
    if len(equalities) != 0:
        constraints.extend([Aeq @ x == beq for (Aeq, beq) in equalities])
    if len(inequalities) != 0:
        constraints.extend([Aeq @ x >= beq for (Aeq, beq) in inequalities])

    if len(constraints) == 0:
        prob = cp.Problem(cp.Minimize(objective))
    else:
        prob = cp.Problem(cp.Minimize(objective), constraints)
    result = prob.solve(solver = cp.MOSEK) #need to add solver argument based on what is installed in your system
    optresult = OptimizeResult()
    optresult.x = x.value
    optresult.success = prob.status == "optimal"
    return optresult


def sum_error(n1d, d, rs, epsilon=1.0, bounds = (0, None), sumConstraint = False, useTest = False, data = None, W = None):

    # Change to np.zeros for first test case:
    n = n1d ** d
    sig_level = 0.05

    if useTest:

        A = np.ones((1, n))
        noisyAns = (np.sum(data) + rs.laplace(scale=1.0/epsilon, size=1)).tolist()
        likely_pos = [True] * n

        for i in range(d):
            ATemp = W[(n1d**(d-1) * i + 1 ):(n1d**(d-1) * (i+1) + 1 ),:].copy()
            ATemp[:, [not k for k in likely_pos]] = 0.
            # Remove rows that are no longer queries:
            Q = np.sum(ATemp, 1) >= 1.
            if any(Q):
                ATemp = ATemp[Q,:]

            noisyAnsTemp = ATemp.dot(data) + rs.laplace(scale=1.0/epsilon, size=ATemp.shape[0])
            noisyAns = noisyAns + noisyAnsTemp.tolist()
            A = np.vstack((A, ATemp))
            for k in range(ATemp.shape[0]):
                p_val = 1. - cdf(noisyAnsTemp[k], 0., epsilon)
                if p_val > 1. - (1.-sig_level)**(1./d):
                    remove_inds = ATemp[k,:] == 1.
                    likely_pos = [likely_pos[it] and not remove_inds[it] for it in range(n)]

        # Only use detailed cell queries for indices in likely_positive:
        ATemp = np.identity(n)[likely_pos, :]
        noisyAnsTemp = np.dot(ATemp, data) + rs.laplace(scale=1.0/epsilon, size=ATemp.shape[0])
        noisyAns = noisyAns + noisyAnsTemp.tolist()
        A = np.vstack((A, ATemp))

        sens = np.sum(A, 0)
        ATemp = 4. - sens
        # Add final query: the sum of cells that are not in likely_positive
        if any(ATemp != 0 ):
            A = np.vstack((A, ATemp))
            noisyAnsTemp = (np.sum(ATemp * data) + rs.laplace(scale=1.0/epsilon, size=1)).tolist()
            # And add our other query answers:
            noisyAns = np.array(noisyAns + noisyAnsTemp)

    else:
        A = W
        noisyAns = A.dot(data) + rs.laplace(scale=1.0/epsilon, size=A.shape[0])

    # BLUE estimate will be used for total sum query and to initialize optimization methods:
    pinv = np.linalg.pinv(A)
    BLUE_est = np.dot(pinv, noisyAns)
    if sumConstraint:
        BLUE_noisysum = np.sum(BLUE_est)
        AnnhilatorMat = np.identity(n) - pinv.dot(A)
        result = fit([(A,noisyAns)], bound=bounds, init=BLUE_est, AnnhilatorMat=AnnhilatorMat,sumConstr=BLUE_noisysum)
    else:
        result = fit([(A,noisyAns)], bound=bounds, init=BLUE_est)

    #evaluate squared error of the sum query
    errors = (W.dot(result.x) - W.dot(data))**2
    SSE_overall = errors.sum()
    SSE_sum = errors[0]
    SSE_marg_with_big = errors[1]
    SSE_marg_without_big = errors[2]
    SSE_big = errors[1+n1d*2]
    SSE_not_big = errors[-1]

    assert result.success

    return(SSE_overall, SSE_sum, SSE_marg_with_big, SSE_marg_without_big, SSE_big, SSE_not_big)


nSim = 1000
nToyExamples = 4
n1d = 10
d = 2
big = 10000

W = makeW(n1d, d)
n = n1d ** d
dataSets = [None] * nToyExamples

dataSets[0] = np.identity(n1d).reshape((n,))*16

dataSets[1] = np.zeros(n)
dataSets[1][0] = big

dataSets[2] = np.ones(n)

dataSets[3] = np.arange(-1,99)
dataSets[3][0] = big


rs = np.random.RandomState(123)
for k in range(nToyExamples):
    data = dataSets[k]
    resNN = [None] * nSim
    resOLS = [None] * nSim
    resNNSt = [None] * nSim

    for j in range(nSim):
        try:
            resNNSt[j]=sum_error(n1d, d, rs, epsilon=1./4, bounds = (0., None), sumConstraint = True, useTest = True, data = data, W = makeW(n1d, d))
            resNN[j]=sum_error(n1d, d, rs, epsilon=1./4, bounds = (0., None), sumConstraint = False, useTest = False, data = data, W = W)
            resOLS[j]=sum_error(n1d, d, rs, epsilon=1./4, bounds = (None, None), sumConstraint = False, useTest = False, data = data, W = W)
        except:
            resNNSt[j]=sum_error(n1d, d, rs, epsilon=1./4, bounds = (0., None), sumConstraint = True, useTest = True, data = data, W = makeW(n1d, d))
            resNN[j]=sum_error(n1d, d, rs, epsilon=1./4, bounds = (0., None), sumConstraint = False, useTest = False, data = data, W = W)
            resOLS[j]=sum_error(n1d, d, rs, epsilon=1./4, bounds = (None, None), sumConstraint = False, useTest = False, data = data, W = W)
            print("optimization failed", j)

    resOLS = np.mean(resOLS, 0)
    resNN = np.mean(resNN, 0)
    resNNSt = np.mean(resNNSt, 0)


    print("")
    print("Baseline:")
    print(k,":")
    print("OLS:")
    print("overall: % 5.2f"%resOLS[0])
    print("sum: % 5.2f"%resOLS[1])
    print("marginal w/ big: % 5.2f"%resOLS[2])
    print("marginal w/o big: % 5.2f"%resOLS[3])
    print("big: % 5.2f"%resOLS[4])
    print("small: % 5.2f"%resOLS[5])
    print("NNLS:")
    print("overall: % 5.2f"%resNN[0])
    print("sum: % 5.2f"%resNN[1])
    print("marginal w/ big: % 5.2f"%resNN[2])
    print("marginal w/o big: % 5.2f"%resNN[3])
    print("big: % 5.2f"%resNN[4])
    print("small: % 5.2f"%resNN[5])
    print("")
    print("After Adding Summation Constraint and Test:")
    print("overall: % 5.2f"%resNNSt[0])
    print("sum: % 5.2f"%resNNSt[1])
    print("marginal w/ big: % 5.2f"%resNNSt[2])
    print("marginal w/o big: % 5.2f"%resNNSt[3])
    print("big: % 5.2f"%resNNSt[4])
    print("small: % 5.2f"%resNNSt[5])

    # To print Latex table:

    sol = np.vstack((resOLS,resNN,resNNSt))
    print('All Queries &' + np.array2string(sol[:,0], separator = " & ", formatter = {'float_kind':lambda x: "%5.2f" % x})[1:-1] + '\tabularnewline')
    print('Sum &' + np.array2string(sol[:,1], separator = " & ",  formatter = {'float_kind':lambda x: "%5.2f" % x})[1:-1] + '\tabularnewline')
    print('$1^{st}$ Marginal &' + np.array2string(sol[:,2], separator = " & ", formatter = {'float_kind':lambda x: "%5.2f" % x})[1:-1] + '\tabularnewline')
    print('$2^{nd}$ Marginal &' + np.array2string(sol[:,3], separator = " & ", formatter = {'float_kind':lambda x: "%5.2f" % x})[1:-1] + '\tabularnewline')
    print('$1^{st}$ Det. Cell &' + np.array2string(sol[:,4], separator = " & ", formatter = {'float_kind':lambda x: "%5.2f" % x})[1:-1] + '\tabularnewline')
    print('$1^{st}$ Det. Cell &' + np.array2string(sol[:,5], separator = " & ", formatter = {'float_kind':lambda x: "%5.2f" % x})[1:-1] + '\tabularnewline')







# W/ nSim = 1000, output is:

#
#Baseline:
#0 :
#OLS:
#overall:  3195.38
#sum:  27.31
#marginal w/ big:  25.42
#marginal w/o big:  29.22
#big:  26.09
#small:  26.51
#NNLS:
#overall:  1064.64
#sum:  46.34
#marginal w/ big:  17.69
#marginal w/o big:  14.10
#big:  39.20
#small:  40.32
#
#After Adding Summation Constraint and Test:
#overall:  1614.18
#sum:  27.79
#marginal w/ big:  13.78
#marginal w/o big:  13.54
#big:  92.22
#small:  88.30
#All Queries &3195.38 & 1064.64 & 1614.18 \tabularnewline
#Sum &27.31 & 46.34 & 27.79 \tabularnewline
#$1^{st}$ Marginal &25.42 & 17.69 & 13.78 \tabularnewline
#$2^{nd}$ Marginal &29.22 & 14.10 & 13.54 \tabularnewline
#$1^{st}$ Det. Cell &26.09 & 39.20 & 92.22 \tabularnewline
#$1^{st}$ Det. Cell &26.51 & 40.32 & 88.30 \tabularnewline
#
#Baseline:
#1 :
#OLS:
#overall:  3210.61
#sum:  25.08
#marginal w/ big:  28.38
#marginal w/o big:  27.91
#big:  26.69
#small:  26.87
#NNLS:
#overall:  348.43
#sum:  113.41
#marginal w/ big:  17.45
#marginal w/o big:  5.10
#big:  36.84
#small:  0.38
#
#After Adding Summation Constraint and Test:
#overall:  112.28
#sum:  19.67
#marginal w/ big:  13.39
#marginal w/o big:  1.59
#big:  19.81
#small:  0.03
#All Queries &3210.61 & 348.43 & 112.28  abularnewline
#Sum &25.08 & 113.41 & 19.67 \tabularnewline
#$1^{st}$ Marginal &28.38 & 17.45 & 13.39 \tabularnewline
#$2^{nd}$ Marginal &27.91 &  5.10 &  1.59 \tabularnewline
#$1^{st}$ Det. Cell &26.69 & 36.84 & 19.81 \tabularnewline
#$1^{st}$ Det. Cell &26.87 &  0.38 &  0.03 \tabularnewline
#
#Baseline:
#2 :
#OLS:
#overall:  3201.12
#sum:  28.35
#marginal w/ big:  29.01
#marginal w/o big:  27.71
#big:  25.37
#small:  25.58
#NNLS:
#overall:  832.53
#sum:  38.43
#marginal w/ big:  18.90
#marginal w/o big:  18.66
#big:  3.16
#small:  4.58
#
#After Adding Summation Constraint and Test:
#overall:  342.92
#sum:  34.33
#marginal w/ big:  15.31
#marginal w/o big:  14.14
#big:  0.52
#small:  0.68
#All Queries &3201.12 & 832.53 & 342.92  \tabularnewline
#Sum &28.35 & 38.43 & 34.33 \tabularnewline
#$1^{st}$ Marginal &29.01 & 18.90 & 15.31 \tabularnewline
#$2^{nd}$ Marginal &27.71 & 18.66 & 14.14 \tabularnewline
#$1^{st}$ Det. Cell &25.37 &  3.16 &  0.52 \tabularnewline
#$1^{st}$ Det. Cell &25.58 &  4.58 &  0.68 \tabularnewline
#
#Baseline:
#3 :
#OLS:
#overall:  3212.60
#sum:  27.44
#marginal w/ big:  27.57
#marginal w/o big:  28.80
#big:  28.08
#small:  26.37
#NNLS:
#overall:  3088.98
#sum:  24.51
#marginal w/ big:  26.38
#marginal w/o big:  28.56
#big:  23.94
#small:  27.23
#
#After Adding Summation Constraint and Test:
#overall:  3055.10
#sum:  26.47
#marginal w/ big:  27.04
#marginal w/o big:  29.61
#big:  25.46
#small:  23.46
#All Queries &3212.60 & 3088.98 & 3055.10 \tabularnewline
#Sum &27.44 & 24.51 & 26.47 \tabularnewline
#$1^{st}$ Marginal &27.57 & 26.38 & 27.04 \tabularnewline
#$2^{nd}$ Marginal &28.80 & 28.56 & 29.61 \tabularnewline
#$1^{st}$ Det. Cell &28.08 & 23.94 & 25.46 \tabularnewline
#$1^{st}$ Det. Cell &26.37 & 27.23 & 23.46 \tabularnewline
#
#
#
#
#
#
