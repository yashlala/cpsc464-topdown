import pytest
import numpy as np
from programs.metrics.accuracy_metrics import infNoneFloatToStr

def test_infNoneFloatToStr():
    assert infNoneFloatToStr(None) == 'None'
    assert infNoneFloatToStr(np.inf) == 'inf'
    assert infNoneFloatToStr(-np.inf) == '-inf'
    assert infNoneFloatToStr(10) == '10'
    assert infNoneFloatToStr(1000.0) == '1000'
    with pytest.warns(UserWarning, match='Float converted'):
        infNoneFloatToStr(1.3)
    with pytest.raises(ValueError):
        infNoneFloatToStr(np.nan)