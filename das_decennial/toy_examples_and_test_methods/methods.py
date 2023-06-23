import fitter
import numpy as np
from noise import rbgeo
import scipy.sparse as ss
import copy


def ols(matlist, zlist, epslist, *, equalities=None, inequalities=None):
    queries = list(zip(matlist, zlist, [e ** 2 for e in epslist]))
    return fitter.fit(queries, equalities=equalities, inequalities=inequalities)


def nnls(matlist, zlist, epslist, *, equalities=None, inequalities=None):
    queries = list(zip(matlist, zlist, [e ** 2 for e in epslist]))
    return fitter.fit(queries, bound=(0, None), equalities=equalities, inequalities=inequalities)


def maxfit(queries, *, equalities=None, inequalities=None, slack=1.0, filterfunction=None):
    mat, z, weight = list(zip(*queries))
    # epslist = [np.sqrt(w) for w in weight]
    return maxl2ct(mat, z, weight, oldinequalities=inequalities, oldequalities=equalities, slack=slack, filterfunction=filterfunction)


def maxl2ct(matlist, zlist, epslist, oldequalities=None, oldinequalities=None, slack=1.0, filterfunction=None):
    """ find the smallest L_infty distance d between the OLS solution
    and a nonnegative solution. The find a nonnegative NNLS solution
    under the constraint that its distance from from the OLS solution
    is at most d + slack.

    filterfunction returns a set of matrices (queries) on which we do not want to differ significantly from OLS

    This only works if all queries are counting queries.
    """
    if oldinequalities is None:
        oldinequalities = []
    if oldequalities is None:
        oldequalities = []
    # get least squares solution
    olsresult = ols(matlist, zlist, epslist, equalities=oldequalities, inequalities=oldinequalities)
    estx = olsresult.x
    # get query answers from least squares solution
    if filterfunction is None:
        newm = matlist[:]
    else:
        newm = filterfunction(matlist, zlist, epslist)
    newz = [np.maximum(0, m @ estx) for m in newm]
    queries = list(zip(newm, newz))
    # do Linfinity nonnegative fit to least squares solution
    maxresult = fitter.fit(queries, bound=(0, None), lp=np.infty, equalities=oldequalities,
                           inequalities=oldinequalities)
    # now add a slack to the distance to enlarge feasible set and break
    # ties based on least squares error to original queries.
    distance = maxresult.fun  # value of objective function
    inequalities = [(m, z - slack - distance) for (m, z) in zip(newm, newz)]
    inequalities.extend([(-m, -z - slack - distance) for (m, z) in zip(newm, newz)])
    inequalities.extend(oldinequalities)
    lastresult = fitter.fit(list(zip(matlist, zlist, [e * e for e in epslist])), equalities=oldequalities,
                            inequalities=inequalities, bound=(0, None))
    lastresult.success = lastresult.success and olsresult.success and maxresult.success
    return lastresult


#####################################################################
### Filter functions for maxl2ct ##############################
#####################################################################

def filterlow(matlist, zlist, epslist):
    newm = []
    for m, z, e in zip(matlist, zlist, epslist):
        bound = threshmaxtailgeo(m, z, e)  # np.log(z.size+1)/(2.0 * e)
        indices = z > bound
        newm.append(m[indices])
    return newm


def filterbh(matlist, zlist, epslist):
    newm = []
    for m, z, e in zip(matlist, zlist, epslist):
        bound = threshbh(m, z, e)  # np.log(z.size+1)/(2.0 * e)
        indices = z > bound
        newm.append(m[indices])
    return newm


def filterhybrid(matlist, zlist, epslist):
    newm = []
    for m, z, e in zip(matlist, zlist, epslist):
        bound = threshhybrid(m, z, e)  # np.log(z.size+1)/(2.0 * e)
        indices = z > bound
        newm.append(m[indices])
    return newm


def filterwiggle(matlist, zlist, epslist):
    newm = []
    for m, z, e in zip(matlist, zlist, epslist):
        bound = threshwigglesum(m, z, e)  # np.log(z.size+1)/(2.0 * e)
        indices = z > bound
        newm.append(m[indices])
    return newm


#####################################################################
######## Routines for high/low/summary style postprocessing #########
#####################################################################

def surv(v, eps):
    """ probability goemetric mechanism starting at 0 is v or larger """
    tail = np.exp(-np.abs(v) * eps) / (1 + np.exp(-eps))
    return np.exp(-v * eps) / (1 + np.exp(-eps)) if v >= 0 else 1 - np.exp((v - 1) * eps) / (1 + np.exp(-eps))


# threshold functions
def threshhybrid(A, z, eps):
    conf = 0.01
    pvals = sorted([(surv(v, eps), i, v) for (i, v) in enumerate(z)], reverse=True)
    m = len(pvals)
    start = 0
    counter = 0
    for i, p in enumerate(pvals):
        k = m - i
        if p[0] < float(k) / m * conf:
            break
        counter = counter + 1
    counter = max(counter, 1)
    estnumzero = counter
    return -np.log((1 + np.exp(-eps)) * (1 - np.power((1 - conf), 1.0 / estnumzero))) / eps


def threshmaxtailgeo(A, z, eps):
    delta = 0.01
    numrows = A.shape[0]
    return -np.log((1 + np.exp(-eps)) * (1 - np.power((1 - delta), 1.0 / numrows))) / eps


def threshbh(A, z, eps):
    conf = 0.01
    pvals = sorted([(surv(v, eps), i, v) for (i, v) in enumerate(z)], reverse=True)
    m = len(pvals)
    start = 0
    for i, p in enumerate(pvals):
        k = m - i
        if p[0] <= float(k) / m * conf:
            break
        start = start + 1
    return pvals[start][2] - 1


def threshwigglesum(A, z, eps, mult=1.0, userb=False):
    numrows = A.shape[0]
    if not userb:
        increasing = np.array(sorted(z)).cumsum()
    else:
        rbconstant = rbgeo(eps)
        rbview = np.where(z <= 0, rbconstant, z)
        increasing = np.array(sorted(rbview)).cumsum()
    negsum = np.sum(np.minimum(z, 0))
    biasedsum = -negsum * mult
    i = 0
    while i < increasing.size and increasing[i] < biasedsum:
        i = i + 1
    i = i - 1
    if i > 0:
        thresh = increasing[i] - increasing[i - 1]
    else:
        thresh = -np.infty
    return thresh


def threshwiggleadd(A, z, eps, mult=2.0):
    numrows = A.shape[0]
    increasing = np.array(sorted(z)).cumsum()
    negsum = np.sum(np.minimum(z, 0))
    biasedsum = -negsum
    negcount = (z <= 0).sum()
    i = 0
    while i < increasing.size and increasing[i] < mult * np.log(negcount + 1) / eps:
        i = i + 1
    i = i - 1
    if i > 0:
        thresh = increasing[i] - increasing[i - 1]
    else:
        thresh = -np.infty
    return thresh


def big_resolve(Alist, zlist, epslist, threshfunction, tol=0.001, domax=False, userb=False):
    todonow = []
    todolater = []
    for (A, z, eps) in zip(Alist, zlist, epslist):
        t = threshfunction(A, z, eps)
        numqueries = A.shape[0]
        belowmask = z <= t
        numbelow = belowmask.sum()
        if numbelow > 1:
            todolater.append((A[belowmask], z[belowmask], eps ** 2))
            extraA = A[belowmask].sum(axis=0, keepdims=True)
            if not userb:
                extraZ = z[belowmask].sum(keepdims=True)
            else:
                myview = z[belowmask]
                rbconstant = rbgeo(eps)
                rbview = np.where(myview <= 0, rbconstant, myview)
                extraZ = rbview.sum(keepdims=True)
            neweight = (eps ** 2) / numbelow
            todonow.append((extraA, extraZ, neweight))
        if numqueries > 0:
            todonow.append((A[np.logical_not(belowmask)], z[np.logical_not(belowmask)], eps ** 2))
    firstnnls = fitter.fit(todonow, bound=(0, None)) if not domax else maxfit(todonow)
    if not firstnnls.success:
        print("big_resolve failed in first nnls")
    inequalities = [(T[0], (T[0] @ firstnnls.x) - tol) for T in todonow]
    inequalities.extend([(-T[0], -(T[0] @ firstnnls.x) - tol) for T in todonow])
    if len(todolater) > 0:
        answer = fitter.fit(todolater, inequalities=inequalities, bound=(0, None)) if not domax else maxfit(todolater, inequalities=inequalities)
        for A, z in inequalities:
            assert np.all(A @ answer.x - z + tol >= 0)
        if not answer.success:
            print(f"Second NNLS for in big_resolve failed")
        answer.success = answer.success and firstnnls.success
    else:
        answer = firstnnls
    return answer


# methods
def rb_maxtail(matlist, zlist, epslist):
    return big_resolve(matlist, zlist, epslist, threshfunction=threshmaxtailgeo, userb=True)


def rb_bh(matlist, zlist, epslist):
    return big_resolve(matlist, zlist, epslist, threshfunction=threshbh, userb=True)


def rb_wigglesum(matlist, zlist, epslist, mult=1.0):
    return big_resolve(matlist, zlist, epslist, userb=True,
                       threshfunction=lambda A, z, eps: threshwigglesum(A, z, eps, mult=mult, userb=True))


def maxtail(matlist, zlist, epslist):
    return big_resolve(matlist, zlist, epslist, threshfunction=threshmaxtailgeo)


def bh(matlist, zlist, epslist):
    return big_resolve(matlist, zlist, epslist, threshfunction=threshbh)


def wigglesum(matlist, zlist, epslist, mult=1.0):
    return big_resolve(matlist, zlist, epslist, threshfunction=lambda A, z, eps: threshwigglesum(A, z, eps, mult=mult))


def wadd(matlist, zlist, epslist):
    return big_resolve(matlist, zlist, epslist, threshfunction=threshwiggleadd)


def olswigglesum(matlist, zlist, epslist, mult=1.0):
    presolve = ols(matlist, zlist, epslist)
    newz = [m @ presolve.x for m in matlist]
    neweps = [1.0 for _ in epslist]
    return wigglesum(matlist, newz, neweps, mult=mult)


def olsws0(matlist, zlist, epslist):
    return olswigglesum(matlist, zlist, epslist, mult=0.0)


# def compensate(matlist, zlist, epslist):
#    presolve = ols(matlist, zlist, epslist)

def tmax(matlist, zlist, epslist):
    return maxl2ct(matlist, zlist, epslist, filterfunction=filterlow)


def bhmax(matlist, zlist, epslist):
    return maxl2ct(matlist, zlist, epslist, filterfunction=filterbh)


def wigmax(matlist, zlist, epslist):
    return maxl2ct(matlist, zlist, epslist, filterfunction=filterwiggle)


def hmax(matlist, zlist, epslist):
    return maxl2ct(matlist, zlist, epslist, filterfunction=filterhybrid)


#############################
# The following is Ryan's method, using an alternative strategy matrix.
# Note that this implementation uses a very hacky method of ensuring the inputs
# of the method correspond to Dan's simulation code; the data, x, is still
# accessed using a global variable. I'll run some tests to make sure this isn't
# problematic in practice.
############################

def cdf(x, mu, eps):
    t = np.exp(-np.abs(x - mu) * eps) / 2
    if x < mu:
        return (t)
    return (1 - t)


def makeW(ns):
    nCells = np.prod(ns)
    d = len(ns)

    if d > 1:
        W = np.vstack((np.ones((1, nCells)), np.zeros((np.sum(ns), nCells))))
        inds = np.array(list(range(nCells)), np.int32)
        inds = inds.reshape(tuple(ns))
        curInd = 1
        for k in range(d):
            for i in range(ns[k]):
                W[curInd, inds[i]] = 1.
                curInd = curInd + 1
            inds = np.rollaxis(inds, d - 1)
    else:
        W = np.ones((1, nCells))
    W = np.vstack((W, np.identity(nCells)))
    return (W)


class Oracle:
    # This is the privacy barrier. It provides DP answers to queries.
    def __init__(self, zlist, x, eps, d, maxSens, matlist):
        self.zlist, self.x, self.A = zlist, x, np.zeros((0, len(x)))
        self.eps, self.d, self.maxSens = eps, d, maxSens
        self.matlist = matlist

        # self.zlist will be a vector of iid Laplace rv's with scale
        # parameter of 4 - this is hacky and clearly not ideal:
        self.zlist = np.concatenate(self.zlist)
        wInit = np.vstack(matlist)
        self.zlist = wInit.dot(self.x) - self.zlist

    def answerQuery(self, query):
        query = np.abs(query)
        assert len(query.shape) == 2
        sensitivityA = np.sum(np.abs(self.A), 0)
        sensitivityQuery = np.sum(np.abs(query), 0)
        assert (sensitivityQuery + sensitivityA <= self.maxSens).all()

        res = query.dot(self.x)
        res = res + self.zlist[self.A.shape[0]:(self.A.shape[0] + len(res))]

        self.A = np.vstack((self.A, query))
        return res.tolist()

    def checkError(self, W, est):
        res = (W.dot(self.x - est)) ** 2
        return res


def SolRyanFindInputs(ns, eps, oracle, W, estSumConst=False, sigLevel=0.05):
    nCells = np.prod(ns)
    d = len(ns)

    criticalValue = 1. - (1. - sigLevel) ** (1. / d)
    A = np.ones((1, nCells))
    Y = oracle.answerQuery(np.ones((1, nCells)))
    likelyPositive = [True] * nCells
    curIndex = 1
    for i in range(d):
        ATemp = copy.deepcopy(W[curIndex:(ns[i] + curIndex), :])
        ATemp[:, [not k for k in likelyPositive]] = 0.
        # Remove rows that only contain zeros or that are detailed queries:
        Q = np.sum(ATemp, 1) >= 1.
        if any(Q):
            ATemp = ATemp[Q, :]
        else:
            continue
        YTemp = oracle.answerQuery(ATemp)
        Y = Y + YTemp
        A = np.vstack((A, ATemp))
        for k in range(ATemp.shape[0]):
            pValue = 1. - cdf(YTemp[k], 0., eps)
            if pValue > criticalValue:
                remove_inds = ATemp[k, :] == 1.
                likelyPositive = [likelyPositive[it] and not remove_inds[
                    it] for it in range(nCells)]
        curIndex = curIndex + ns[i]

    # Only use detailed cell queries for indices in likelyPositive:
    remainingSens = 2 + d - np.sum(A, 0)
    ATemp = np.diag(remainingSens)[likelyPositive, :]
    YTemp = oracle.answerQuery(ATemp)
    Y = Y + YTemp
    A = np.vstack((A, ATemp))

    # Add final query: the sum of cells that are not in likelyPositive
    remainingSens = 2 + d - np.sum(A, 0)
    ATemp = remainingSens.reshape((1, nCells))
    if (ATemp != 0.).any():
        A = np.vstack((A, ATemp))
        YTemp = oracle.answerQuery(ATemp)
        Y = Y + YTemp
    Y = np.array(Y)
    # In two of the following four cases, we can avoid finding the
    # pseudoinverse:
    if estSumConst:
        if np.linalg.matrix_rank(A) < nCells:
            pinv = np.linalg.pinv(A)
            BLUENoisySum = np.sum(pinv.dot(Y))
            M = np.identity(nCells) - pinv.dot(A)
        else:
            BLUEestRes = ss.linalg.lsmr(A, Y)[0]
            BLUENoisySum = np.sum(BLUEestRes)
            M = np.zeros((nCells, nCells))

        inequalities = [(-np.ones((1, nCells)), np.array([-BLUENoisySum]))]
        return A, Y, inequalities, M

    else:
        if np.linalg.matrix_rank(A) < nCells:
            pinv = np.linalg.pinv(A)
            M = np.identity(nCells) - pinv.dot(A)
        else:
            M = np.zeros((nCells, nCells))
    return A, Y, M


def altStrategy(y, matlist, zlist, epslist, sig_level=0.05):
    # This estimator is not defined when d = 1
    if len(zlist) == 2:
        return nnls(matlist, zlist, epslist)
    elif len(zlist) == 4:
        d = 2
    else:
        raise ("Cannot determine d")
    ns = []
    assert all([k == epslist[0] for k in epslist])

    # for k in range(1, 3):
    #    ns = ns + [int(np.sum(matlist[k], 1)[0])]
    # W = makeW(ns)

    # Assumption: matlist[1] and matlist[2] are marginal queries
    numInMarg1 = np.sum(matlist[1], 1)[0]
    numInMarg2 = np.sum(matlist[2], 1)[0]
    matlistcopy = matlist[:]  # to avoid overwriting matlist
    zlistcopy = zlist[:]
    epslistcopy = epslist[:]
    if numInMarg1 > numInMarg2:
        matlistcopy[1:3] = matlist[2:0:-1]
        zlistcopy[1:3] = zlist[2:0:-1]
        epslistcopy[1:3] = epslist[2:0:-1]
    ns = [matlistcopy[1].shape[0], matlistcopy[2].shape[0]]
    W = np.vstack(matlistcopy)

    oracle = Oracle(zlistcopy, y, 1. / 4, d, 4., matlistcopy)
    A, Y, inequalities, M = SolRyanFindInputs(ns, epslistcopy[0], oracle, W,
                                              True, sigLevel=sig_level)
    queries = [(A, Y), (M, np.zeros(M.shape[0]))]
    return fitter.fit(queries, inequalities=inequalities, bound=(0., None))
