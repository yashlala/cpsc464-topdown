import pytest
import numpy as np
import scipy.sparse as ss
import programs.queries.querybase as querybase
import programs.sparse as sparse


def compare_arrays(a, b, tol=0.00000001):
    """ compares two arrays by checking that their L1 distance is within a tolerance """
    return a.shape == b.shape and np.abs(a - b).sum() <= tol
    # return a.shape == b.shape and np.allclose(a, b)


@pytest.fixture
def data():
    """ sets up data for testing """
    shape = (3, 4, 5)
    size: int = np.prod(shape)
    return np.arange(size).reshape(shape)


@pytest.fixture
def sparse_data(data):
    """ set up sparse data for testing """
    return sparse.multiSparse(data)


@pytest.mark.parametrize("add_over_margins, subset",
                         [
                                ((0, 1, 2), None),
                                ((),        None),
                                ((1, ),     None),
                                ((1, ),     ([0, 2], range(0, 4, 2), [0, 4])),
                         ])
def test_sumover_query(data, add_over_margins, subset):
    """ Tests that SumoverQuery answers queries correctly """
    q1 = querybase.SumoverQuery(data.shape, subset=subset, add_over_margins=add_over_margins)
    # check for (1) correct answer, (2) answer matches the matrix representation
    if subset is None:
        answer = data.sum(axis=add_over_margins).flatten()
    else:
        answer = data[np.ix_(*subset)].sum(axis=add_over_margins).flatten()
    assert compare_arrays(q1.answer(data), answer)
    assert compare_arrays(q1.matrixRep() * data.flatten(), answer)
    # check that the query sizes are correct
    assert q1.domainSize() == data.size
    assert q1.domainSize() == q1.matrixRep().shape[1]
    assert q1.numAnswers() == q1.answer(data).size, "numAnswers is: {} but should be: {}".format(q1.numAnswers(), q1.answer(data).size)
    # check sensitivities
    assert q1.unboundedDPSensitivity() == 1
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.unboundedDPSensitivity()
    # make sure it is counting query
    assert q1.isIntegerQuery()


def test_repeated_subset_error(data):
    with pytest.raises(AssertionError):
        querybase.SumoverQuery(data.shape, subset=([2, 2], range(0, 4, 2), [0, 4]), add_over_margins=(1,))


def test_sparsekronquery(data):
    """ test the SparseKronQuery class """
    shape = data.shape

    # marginalize over first and last dimensions
    matrices1 = [ss.eye(x) for x in shape]
    matrices1[0] = np.ones(shape[0])
    matrices1[-1] = ss.csr_matrix(np.ones(shape[-1]))
    query1 = querybase.SparseKronQuery(matrices1)
    query1a = querybase.QueryFactory.makeKronQuery(matrices1, "hello")

    # marginalize over first and last dimensions, denser kron representation
    matrices2 = matrices1[1:]
    matrices2[0] = ss.kron(matrices1[0], matrices1[1])
    query2 = querybase.SparseKronQuery(matrices2)
    query2a = querybase.QueryFactory.makeKronQuery(matrices2, "hello2")

    # marginalize over everything
    matrices3 = [np.ones(x) for x in shape]
    query3 = querybase.SparseKronQuery(matrices3)
    query3a = querybase.QueryFactory.makeKronQuery(matrices3, "hello3")

    # check for (1) correct answer, (2) answer matches the matrix representation, (3) correct column size, (4) correct row size
    assert compare_arrays(query1.answer(data), data.sum(axis=(0, len(shape) - 1)).flatten())
    assert compare_arrays(query1.matrixRep() * data.flatten(), data.sum(axis=(0, len(shape) - 1)).flatten())
    assert compare_arrays(query1.answer(data), query1a.answer(data))
    assert query1.domainSize() == data.size
    assert query1.domainSize() == query1.matrixRep().shape[1]
    assert query1.numAnswers() == query1.matrixRep().shape[0]

    # check for (1) correct answer, (2) answer matches the matrix representation, (3) correct column size, (4) correct row size
    assert compare_arrays(query2.answer(data), data.sum(axis=(0, len(shape) - 1)).flatten())
    assert compare_arrays(query2.matrixRep() * data.flatten(), data.sum(axis=(0, len(shape) - 1)).flatten())
    assert query2.domainSize() == data.size
    assert query2.domainSize() == query2.matrixRep().shape[1]
    assert query2.numAnswers() == query2.matrixRep().shape[0]
    assert compare_arrays(query2.answer(data), query2a.answer(data))

    # check for (1) correct answer, (2) answer matches the matrix representation, (3) correct column size, (4) correct row size
    assert compare_arrays(query3.answer(data), data.sum().flatten())
    assert compare_arrays(query3.matrixRep() * data.flatten(), data.sum().flatten())
    assert query3.domainSize() == data.size
    assert query3.domainSize() == query3.matrixRep().shape[1]
    assert query3.numAnswers() == query3.matrixRep().shape[0]
    assert compare_arrays(query3.answer(data), query3a.answer(data))

    # check sensitivity
    assert np.abs(query1.matrixRep()).sum(axis=0).max() == query1.unboundedDPSensitivity()
    assert np.abs(query2.matrixRep()).sum(axis=0).max() == query2.unboundedDPSensitivity()
    assert np.abs(query3.matrixRep()).sum(axis=0).max() == query3.unboundedDPSensitivity()
    # make sure it is counting query
    assert query1.isIntegerQuery()
    assert query2.isIntegerQuery()
    assert query3.isIntegerQuery()


def test_stackedquery1(data):
    """ we will stack a SumoverQuery and a SparseKronQuery """
    q1 = querybase.SumoverQuery(data.shape, add_over_margins=(1,))
    matrices1 = [ss.eye(x) for x in data.shape]
    matrices1[0] = np.ones(data.shape[0])
    matrices1[-1] = ss.csr_matrix(np.ones(data.shape[-1]))
    q2 = querybase.SparseKronQuery(matrices1)
    q3 = q1
    # now stack
    q4 = querybase.StackedQuery([q1, q2, q3])
    answer = q4.answer(data)
    expected = np.concatenate([q1.answer(data), q2.answer(data), q3.answer(data)])
    assert compare_arrays(answer, expected)
    # check sensitivities
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.unboundedDPSensitivity()
    assert np.abs(q2.matrixRep()).sum(axis=0).max() == q2.unboundedDPSensitivity()
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.unboundedDPSensitivity()
    assert np.abs(q4.matrixRep()).sum(axis=0).max() == q4.unboundedDPSensitivity()
    # make sure it is counting query
    assert q1.isIntegerQuery()
    assert q2.isIntegerQuery()
    assert q3.isIntegerQuery()
    assert q4.isIntegerQuery()
    # check shape
    assert q4.domainSize() == q4.matrixRep().shape[1]
    assert q4.domainSize() == data.size
    assert q4.numAnswers() == q4.matrixRep().shape[0]
    assert q4.numAnswers() == q1.numAnswers() + q2.numAnswers() + q3.numAnswers()


def test_stackedquery2():
    """ test the subsetting part of StackedQuery """
    data = np.arange(6)
    q1 = querybase.SparseKronQuery([ss.csr_matrix(np.array([[1, 1, 1, -1, -1, -1], [1, 1, 1, 1, 1, 1]]))])
    q2 = querybase.SparseKronQuery([ss.csr_matrix(np.array([[1, 2, 3, 0, 0, 0], [0, 0, 0, 1, 1, 1]]))])
    domain_subset = [1, 2, 5]
    q3 = querybase.StackedQuery([q1, q2], domain_subset=domain_subset)
    answer = q3.answer(data[domain_subset])
    expected = np.array([-2, 8, 8, 5])
    assert compare_arrays(answer, expected)
    # check sensitivities
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.unboundedDPSensitivity()
    assert np.abs(q2.matrixRep()).sum(axis=0).max() == q2.unboundedDPSensitivity()
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.unboundedDPSensitivity()
    # make sure it is counting query
    assert q1.isIntegerQuery()
    assert q2.isIntegerQuery()
    assert q3.isIntegerQuery()
    # check shape
    assert q3.domainSize() == q3.matrixRep().shape[1]
    assert q3.domainSize() == data[domain_subset].size
    assert q3.numAnswers() == q3.matrixRep().shape[0]
    assert q3.numAnswers() == q1.numAnswers() + q2.numAnswers()


def test_queryfactory_without_collapse(data):
    shape = data.shape
    subset = ([0, shape[0] - 1], range(0, shape[1], 2), [0, shape[2] - 1])
    add_over_margins = (1,)
    q3 = querybase.QueryFactory.makeTabularQuery(shape, subset=subset, add_over_margins=add_over_margins)
    q4 = querybase.SumoverQuery(shape, subset=subset, add_over_margins=add_over_margins)
    assert compare_arrays(q3.answer(data), q4.answer(data))
    # check sensitivities
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.unboundedDPSensitivity()
    assert np.abs(q4.matrixRep()).sum(axis=0).max() == q4.unboundedDPSensitivity()
    # make sure it is counting query
    assert q3.isIntegerQuery()
    assert q4.isIntegerQuery()
    # check shape
    assert q3.domainSize() == q3.matrixRep().shape[1]
    assert q3.numAnswers() == q3.answer(data).size
    assert q4.domainSize() == q4.matrixRep().shape[1]
    assert q4.numAnswers() == q4.answer(data).size


# def test_queryfactory_with_collapse1(data):
# collapse gives the same answer as subset
#    shape = data.shape
#    subset = ( range(shape[0]), range(0, shape[1], 2), range(shape[2]))
#    axis_groupings = [(1, ((0,2)))]
#    add_over_margins = (2,)
#    q5 = cenquery.QueryFactory.makeTabularQuery(shape, subset=subset, add_over_margins=add_over_margins)
#    q6 = cenquery.QueryFactory.makeTabularQuery(shape, subset= None, add_over_margins=add_over_margins, axis_groupings = axis_groupings)
#    assert compare_arrays(q5.answer(data),q6.answer(data))
# check sensitivities
#    assert np.abs(q5.matrixRep()).sum(axis=0).max() == q5.sensitivity()
#    assert np.abs(q6.matrixRep()).sum(axis=0).max() == q6.sensitivity()
# make sure it is counting query
#    assert q5.isIntegerQuery()
#    assert q6.isIntegerQuery()

# def test_queryfactory_with_collapse2(data):
#    #a "null" collapse
#    shape = data.shape
#    subset = None
#    axis_groupings = [(1, ((0,1,2,3)))]
#    add_over_margins = (2,)
#    q5 = cenquery.QueryFactory.makeTabularQuery(shape, subset=None, add_over_margins=add_over_margins)
#    q6 = cenquery.QueryFactory.makeTabularQuery(shape, subset= None, add_over_margins=add_over_margins, axis_groupings = axis_groupings)
#    assert compare_arrays(q5.answer(data),q6.answer(data))
# check sensitivities
#    assert np.abs(q5.matrixRep()).sum(axis=0).max() == q5.sensitivity()
#    assert np.abs(q6.matrixRep()).sum(axis=0).max() == q6.sensitivity()
# make sure it is counting query
#    assert q5.isIntegerQuery()
#    assert q6.isIntegerQuery()

def test_counting_query(data):
    """ test if we can detect non-counting query """
    shape = data.shape

    # marginalize over first and last dimensions
    matrices1 = [ss.eye(x) for x in shape]
    bad = np.ones(shape[0])
    bad[0] += 0.000000001
    matrices1[0] = bad
    matrices1[-1] = ss.csr_matrix(np.ones(shape[-1]))
    query1 = querybase.SparseKronQuery(matrices1)
    assert not query1.isIntegerQuery()


@pytest.mark.parametrize("add_over_margins, subset",
                         [
                                ((0, 1, 2), None),
                                ((),        None),
                                ((1, ),     None),
                                ((1, ),     ([0, 2], range(0, 4, 2), [0, 4])),
                         ])
def test_sumover_grouped_query(data, add_over_margins, subset):
    """ Tests that SumOverGroupedQuery answers queries correctly """
    # these are the same as in test_sumover_query

    if subset is None:
        q1 = querybase.SumOverGroupedQuery(data.shape, add_over_margins=add_over_margins)
        answer = data.sum(axis=add_over_margins).flatten()
    else:
        q1 = querybase.SumoverQuery(data.shape, subset=subset, add_over_margins=add_over_margins)
        answer = data[np.ix_(*subset)].sum(axis=add_over_margins).flatten()
    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q1.answer(data), answer)
    assert compare_arrays(q1.matrixRep() * data.flatten(), answer)

    assert q1.domainSize() == data.size
    assert q1.numAnswers() == q1.answer(data).size, "numAnswers is: {} but should be: {}".format(q1.numAnswers(), q1.answer(data).size)

    # check sensitivities
    assert q1.unboundedDPSensitivity() == 1
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.unboundedDPSensitivity()

    # make sure it is counting query
    assert q1.isIntegerQuery()


def test_sumover_grouped_query2(data):
    """ Tests that SumOverGroupedQuery answers queries correctly """
    # these are queries that use groupings
    s = data.shape

    # the same as a subset
    groupings1 = {0: [[0], ]}
    # mutliple groups
    groupings2 = {0: [[0, 1], [2]]}
    # multiple dimensions
    groupings3 = {0: [[0, 1], [2]], 1: [[0], [1]]}

    q0 = querybase.SumOverGroupedQuery(data.shape, add_over_margins=(0, 1, 2))
    q1 = querybase.SumOverGroupedQuery(data.shape, groupings=groupings1)
    q2 = querybase.SumOverGroupedQuery(data.shape, groupings=groupings2)
    q3 = querybase.SumOverGroupedQuery(data.shape, groupings=groupings3)

    q5 = querybase.SumOverGroupedQuery(data.shape, groupings=groupings3, add_over_margins=(2,))

    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q0.answer(data), data.sum().flatten())
    assert compare_arrays(q0.matrixRep() * data.flatten(), data.sum().flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q1.answer(data), data[0, 0:s[1], 0:s[2]].flatten())
    assert compare_arrays(q1.matrixRep() * data.flatten(), data[0, 0:s[1], 0:s[2]].flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    right_answer = np.stack([data[0:2, 0:s[1], 0:s[2]].sum(0, keepdims=False), data[2, 0:s[1], 0:s[2]]], axis=0).flatten()
    assert compare_arrays(q2.answer(data), right_answer)
    assert compare_arrays(q2.matrixRep() * data.flatten(), right_answer)
    # check for (1) correct answer, (2) answer matches the matrix representation
    right_answer = np.stack([data[0:2, 0:2, 0:s[2]].sum(0, keepdims=False), data[2, 0:2, 0:s[2]]], axis=0).flatten()
    assert compare_arrays(q3.answer(data), right_answer)
    assert compare_arrays(q3.matrixRep() * data.flatten(), right_answer)
    # check for (1) correct answer, (2) answer matches the matrix representation
    right_answer = np.stack([data[0:2, 0:2, 0:s[2]].sum(0, keepdims=False), data[2, 0:2, 0:s[2]]], axis=0).sum(2).flatten()
    assert compare_arrays(q5.answer(data), right_answer)
    assert compare_arrays(q5.matrixRep() * data.flatten(), right_answer)

    # check that the query sizes are correct
    assert q1.domainSize() == data.size
    assert q1.numAnswers() == q1.answer(data).size, "numAnswers is: {} but should be: {}".format(q1.numAnswers(), q1.answer(data).size)

    # make a query with sensitivty 2
    groupings4 = {0: [[0, 1], [0, 1]], 1: [[0], [1]]}
    q4 = querybase.SumOverGroupedQuery(data.shape, groupings=groupings4)

    # check sensitivities
    assert q1.unboundedDPSensitivity() == 1
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.unboundedDPSensitivity()
    assert q2.unboundedDPSensitivity() == 1
    assert np.abs(q2.matrixRep()).sum(axis=0).max() == q2.unboundedDPSensitivity()
    assert q3.unboundedDPSensitivity() == 1
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.unboundedDPSensitivity()
    assert q4.unboundedDPSensitivity() == 2
    assert np.abs(q4.matrixRep()).sum(axis=0).max() == q4.unboundedDPSensitivity()
    with pytest.raises(ValueError, match="only supported when equal to 1"):
        q4.unboundedL2Sensitivity()

    # make a query with sensitivity
    groupings6 = {
        0: [[1], [2]],
        1: [[1, 2], [2, 3]],
        2: [[1, 2], [2, 3]],
    }
    q6 = querybase.SumOverGroupedQuery(data.shape, groupings=groupings6)
    assert q6.unboundedDPSensitivity() == 4
    assert np.abs(q6.matrixRep()).sum(axis=0).max() == q6.unboundedDPSensitivity()

    # make sure it is counting query
    assert q1.isIntegerQuery()
    assert q2.isIntegerQuery()
    assert q3.isIntegerQuery()


def test_error_not_disjoint(data):
    with pytest.raises(AssertionError):
        querybase.SumOverGroupedQuery(data.shape, groupings={0: [0,1]}, add_over_margins=(0,1))


@pytest.mark.parametrize("add_over_margins", [(2,), (0,), (0, 2)])
@pytest.mark.parametrize("groupings",
                         [
                             {1: [[1], ]},
                             {1: [range(2)]},
                             None
                         ])
def test_makeTabularGroupQuery(data, groupings, add_over_margins):
    q5 = querybase.QueryFactory.makeTabularGroupQuery(data.shape, groupings=groupings, add_over_margins=add_over_margins)
    q6 = querybase.SumOverGroupedQuery(data.shape, groupings=groupings, add_over_margins=add_over_margins)
    assert compare_arrays(q5.answer(data), q6.answer(data))
    # check sensitivities
    assert np.abs(q5.matrixRep()).sum(axis=0).max() == q5.unboundedDPSensitivity()
    assert np.abs(q6.matrixRep()).sum(axis=0).max() == q6.unboundedDPSensitivity()
    # make sure it is counting query
    assert q5.isIntegerQuery()
    assert q6.isIntegerQuery()


# def test_makeTabularGroupQueryWOld(data):
#     import legacy_code.cenquery as cenquery_old
#     shape = data.shape
#     add_over_margins = (2,)
#     subset = (range(3), [1, 2], range(5))
#     groupings = {1: [[1], [2]]}
#     axis_groupings = [(1, ([0, 1], [2])), (2, ([1, 3], [0, 2]))]
#     groupings2 = {1: [[0, 1], [2]], 2: [[1, 3], [0, 2]]}
#     q1 = cenquery_old.Query(shape, add_over_margins=add_over_margins).convertToQuerybase()
#     q2 = cenquery.QueryFactory.makeTabularGroupQuery(shape, add_over_margins=add_over_margins)
#     assert compare_arrays(q1.answer(data), q2.answer(data))
#     q3 = cenquery_old.Query(shape, add_over_margins=add_over_margins, subset=subset).convertToQuerybase()
#     q4 = cenquery.QueryFactory.makeTabularGroupQuery(shape, add_over_margins=add_over_margins, groupings=groupings)
#     assert compare_arrays(q3.answer(data), q4.answer(data))
#     q5 = cenquery_old.Query(shape, add_over_margins=(0,), axis_groupings=axis_groupings).convertToQuerybase()
#     q6 = cenquery.QueryFactory.makeTabularGroupQuery(shape, add_over_margins=(0,), groupings=groupings2)
#     assert compare_arrays(q5.answer(data), q6.answer(data))

@pytest.mark.parametrize("add_over_margins, answer",
                         [
                                ((1, 2),    [[190], [590], [990]]),
                                ((0, 2),    [[330], [405], [480], [555]]),
                                ((0, 1),    [[330], [342], [354], [366], [378]]),
                                ([0],       [[60], [63], [66], [69], [72], [75], [78], [81], [84], [87], [90], [93], [96], [99], [102], [105], [108], [111], [114], [117]]),
                                ([1],       [[30], [34], [38], [42], [46], [110], [114], [118], [122], [126], [190], [194], [198], [202], [206]]),
                                ([2],       [[10], [35], [60], [85], [110], [135], [160], [185], [210], [235], [260], [285]]),
                         ])
def test_SumOverGroupedQuery_answerSparse(sparse_data, add_over_margins, answer):
    shape = sparse_data.shape
    query = querybase.QueryFactory.makeTabularGroupQuery(shape, add_over_margins=add_over_margins)
    ans = query.answerSparse(sparse_data.sparse_array.transpose())
    assert isinstance(ans, ss.csr_matrix), "Not a sparse matrix"
    ans_dense_true = np.array(answer)
    ans_dense = ans.toarray()
    assert ans_dense.shape == ans_dense_true.shape, "Incorrect shape"
    assert np.all(ans_dense == ans_dense_true), "Answers don't match"


@pytest.mark.parametrize("add_over_margins, subset",
                         [
                                ((0, 1, 2), None),
                                ((),        None),
                                ((1, ),     None),
                                ((1, ),     ([0, 2], range(0, 4, 2), [0, 4])),
                         ])
def test_eq(data, subset, add_over_margins):
    q1 = querybase.QueryFactory.makeTabularQuery(data.shape, subset=subset, add_over_margins=add_over_margins)
    q2 = querybase.QueryFactory.makeTabularQuery(data.shape, subset=subset, add_over_margins=add_over_margins)
    qn2 = querybase.QueryFactory.makeTabularQuery(data.shape, subset=subset, add_over_margins=(2,))
    assert q1 == q2
    assert q1 != qn2

    q3 = querybase.QueryFactory.makeTabularGroupQuery(data.shape, add_over_margins=add_over_margins, name="")
    q4 = querybase.QueryFactory.makeTabularGroupQuery(data.shape, add_over_margins=add_over_margins, name="")
    qn4 = querybase.QueryFactory.makeTabularQuery(data.shape, subset=subset, add_over_margins=(2,))

    assert q3 == q4
    assert q3 != qn4


def test_eq_kron_combo():
    q1 = querybase.QueryFactory.makeKronQuery((np.array([False, True]), np.array([False, True])), name="")
    q2 = querybase.QueryFactory.makeKronQuery((np.array([False, True]), np.array([False, True])), name="")
    qn2 = querybase.QueryFactory.makeKronQuery((np.array([False, True]), np.array([True, False])), name="")
    assert q1 == q2
    assert q1 != qn2

    q3 = querybase.QueryFactory.makeComboQuery((q1, q2, qn2), linear_combo=[1, 2, 3], name="")
    q4 = querybase.QueryFactory.makeComboQuery((q1, qn2, q2), linear_combo=[1, 3, 2], name="")
    qn4 = querybase.QueryFactory.makeComboQuery((q1, q2, qn2), linear_combo=[1, 3, 3], name="")

    assert q3 == q4
    assert q3 != qn4


class TestMultiHistQuery:

    data1 = np.arange(12).reshape((3, 4))
    data2 = np.arange(12, 24).reshape((3, 2, 2))
    q1 = querybase.SumOverGroupedQuery(data1.shape, groupings={1: [[0, 1]]})
    q2 = querybase.SumOverGroupedQuery(data2.shape, groupings={1: [[0, 1]], 2: [[0, 1]]})

    def test___init__(self):
        querybase.MultiHistQuery((self.q1, self.q2))

    def test___init__fail(self):
        q1 = querybase.SumOverGroupedQuery(self.data1.shape, groupings={1: [[0, 1]]}, name='q1')
        q2 = querybase.SumOverGroupedQuery(self.data2.shape, groupings={0: [[0, 1]], 2: [[0, 1]]}, name='q2')
        with pytest.raises(ValueError):
            querybase.MultiHistQuery((q1, q2))

    def test_numAnswers(self):
        q = querybase.MultiHistQuery((self.q1, self.q2))
        assert q.numAnswers() == 3

    @pytest.mark.parametrize("coeff, ans", [((1, 1), [55, 79, 103]),  ((1,-1), [-53, -61, -69])])
    def test_answerByMem(self, coeff, ans):
        q = querybase.MultiHistQuery((self.q1, self.q2), coeff)
        #assert np.array_equal(q.answer((self.data1, self.data2)), np.array(ans))
        assert np.array_equal(q.answer(np.hstack((self.data1.ravel(), self.data2.ravel()))), np.array(ans))

    @pytest.mark.parametrize("coeff, ans", [((1, 1), [55, 79, 103]), ((1, -1), [-53, -61, -69])])
    def test_answer(self, coeff, ans):
        q = querybase.MultiHistQuery((self.q1, self.q2), coeff)
        assert np.array_equal(q.answerByMatRep(np.hstack((self.data1.ravel(), self.data2.ravel()))), np.array(ans))


    def test_matrix_rep(self):
        self.q1.matrixRep().toarray().dot(self.data1.ravel()) + self.q2.matrixRep().toarray().dot(self.data2.ravel())
        q = querybase.MultiHistQuery((self.q1, self.q2))
        from_m_r = q.matrixRep().toarray().dot(np.hstack((self.data1.ravel(), self.data2.ravel())))
        from_indiv = self.q1.matrixRep().toarray().dot(self.data1.ravel()) + self.q2.matrixRep().toarray().dot(self.data2.ravel())
        assert np.array_equal(from_indiv, from_m_r)


def test_L2_sensitivity(data):
    shape = data.shape
    subset = ([0, shape[0] - 1], range(0, shape[1], 2), [0, shape[2] - 1])
    add_over_margins = (1,)
    q1 = querybase.QueryFactory.makeTabularQuery(data.shape, subset=subset, add_over_margins=add_over_margins)
    q2 = querybase.QueryFactory.makeTabularQuery(data.shape, subset=subset, add_over_margins=add_over_margins)
    q3 = querybase.SparseKronQuery([ss.csr_matrix(np.array([[1, 2, 3, 0, 0, 0], [0, 0, 0, 1, 1, 1]]))])

    assert q1.unboundedL2Sensitivity() == 1.
    assert q2.unboundedDPSensitivity() == 1.
    assert q1.unboundedDPSensitivity() == 1.
    assert q2.unboundedL2Sensitivity() == 1.
    assert q3.unboundedDPSensitivity() == 3.

    with pytest.raises(NotImplementedError, match="L2 sensitivity calculation only implemented"):
        q3.unboundedL2Sensitivity()