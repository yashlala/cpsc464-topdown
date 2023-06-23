import pytest
import numpy as np
from programs.sparse import multiSparse
import scipy.sparse as ss
import programs.sparse as sparse
import warnings

warnings.filterwarnings("ignore", message="numpy.ufunc size changed")


@pytest.fixture
def dataint():
    shape = (6, 4, 5)
    size = np.prod(shape)
    return np.arange(1, size + 1).reshape(shape)

@pytest.fixture
def datafloat():
    shape = (6, 4, 5)
    size = np.prod(shape)
    return np.arange(1, size + 1).reshape(shape) + 0.3


def test_build(dataint):
    a = sparse.multiSparse(dataint)
    assert np.array_equal(a.toDense(), dataint)

def test_equal_int(dataint):
    assert sparse.multiSparse(dataint) == sparse.multiSparse(dataint)

def test_equal_float(datafloat):
    assert sparse.multiSparse(datafloat) == sparse.multiSparse(datafloat + 1e-6)
    shape = (6, 4, 5)
    size = np.prod(shape)
    a = np.arange(1, size + 1).reshape(shape).astype(np.float)
    a[0,0,0] = np.nan
    b = a + 1e-6
    assert sparse.multiSparse(a) == sparse.multiSparse(b)

def test_add(dataint, datafloat):
    assert sparse.multiSparse(dataint) + sparse.multiSparse(dataint) == sparse.multiSparse(dataint*2)
    assert sparse.multiSparse(datafloat) + sparse.multiSparse(datafloat) == sparse.multiSparse(datafloat * 2)

def test_sub(dataint, datafloat):
    assert sparse.multiSparse(dataint) - sparse.multiSparse(dataint) == sparse.multiSparse(dataint*0)
    assert sparse.multiSparse(datafloat) - sparse.multiSparse(datafloat) == sparse.multiSparse(datafloat * 0)

def test_abs(dataint, datafloat):
    assert sparse.multiSparse(dataint).abs() == sparse.multiSparse(np.abs(dataint))
    assert sparse.multiSparse(datafloat).abs() == sparse.multiSparse(np.abs(datafloat))

def test_sqrt(dataint, datafloat):
    assert sparse.multiSparse(dataint).sqrt() == sparse.multiSparse(np.sqrt(dataint))
    assert sparse.multiSparse(datafloat).sqrt() == sparse.multiSparse(np.sqrt(datafloat))

def test_square(dataint, datafloat):
    assert sparse.multiSparse(dataint).square() == sparse.multiSparse(np.square(dataint))
    assert sparse.multiSparse(datafloat).square() == sparse.multiSparse(np.square(datafloat))

def test_sum(dataint, datafloat):
    assert sparse.multiSparse(dataint).sum() == np.sum(dataint)
    assert np.isclose(sparse.multiSparse(datafloat).sum(), np.sum(datafloat))
    assert np.array_equal(sparse.multiSparse(dataint).sum(dims = (1,2)), dataint.sum((1,2)))
    assert np.isclose(sparse.multiSparse(datafloat).sum(dims=(1, 2)), datafloat.sum((1, 2))).all()



def test_init():
    good_array = np.array([[0, 1, 2], [2, 0, 4]])
    bad_obj = {"a":"bad", "dict":"obj"}
    spar = multiSparse(good_array)
    assert spar.shape == (2, 3)
    assert isinstance(spar.sparse_array,  ss.csr_matrix)
    assert spar.sparse_array.count_nonzero() == 4

    try:
        bad_spar = multiSparse(bad_obj)
        assert False
    except TypeError:
        assert True

def test_toDense():
    good_array = np.array([[[0, 1], [2, 0], [0, 3]],
                           [[4, 0], [0, 5], [6, 0]]])
    spar = multiSparse(good_array)
    assert spar.shape == (2, 3, 2)
    undo = spar.toDense()
    assert (undo == good_array).all()


def test_add2():
    good_array_A = np.array([[0, 1], [2, 3]])
    good_array_B = np.array([[3, 2], [1, 0]])
    bad_array_C = np.array([[2, 4, 4]])

    spar_A = multiSparse(good_array_A)
    spar_B = multiSparse(good_array_B)
    spar_C = multiSparse(bad_array_C)

    spar = spar_A + spar_B
    assert spar.shape == (2, 2)
    assert spar.sparse_array.count_nonzero() == 4
    assert (spar.toDense() != spar_A.toDense()).any()
    assert (spar.toDense() == np.array([[3, 3], [3, 3]])).all()

    try:
        bad_spar = spar_A + spar_B
        assert False
    except AssertionError:
        assert True
