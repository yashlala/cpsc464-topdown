import numpy as np
import scipy.sparse as ss
import pytest

from programs.optimization.geo_optimizer_decomp import secondCoarsensFirst

@pytest.fixture
def csr1():
    # Matrix is:
    # [1, 0, 1, 0, 0]
    # [0, 1, 0, 1, 1]
    return ss.csr_matrix((np.ones(5), ([0, 1, 0, 1, 1], list(range(5)))))


@pytest.fixture
def csr2():
    # Matrix is 5 x 5 identity:
    return ss.eye(5).tocsr()


@pytest.fixture
def csr3():
    # Matrix is:
    # [1, 0, 0, 0, 0]
    # [0, 1, 0, 0, 0]
    # [0, 0 ,1, 0, 0]
    # [0, 0, 0, 1, 1]
    return ss.csr_matrix((np.ones(5), ([0, 1, 2, 3, 3], list(range(5)))))


def test_secondCoarsensFirst1(csr1, csr2, csr3):
    assert secondCoarsensFirst(csr1, csr2) == (False, None)
    assert secondCoarsensFirst(csr3, csr2) == (False, None)
    assert secondCoarsensFirst(csr1, csr3) == (False, None)

    res_a = secondCoarsensFirst(csr3, csr1)
    assert res_a[0]
    assert res_a[1] == [[0, 2], [1, 3]]

    res_b = secondCoarsensFirst(csr2, csr3)
    assert res_b[0]
    assert res_b[1] == [[0], [1], [2], [3, 4]]
