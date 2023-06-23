import pytest
from programs.engine.budget import checkDyadic
from exceptions import DASValueError


class TestBudgetChecks:

    @pytest.mark.parametrize("values", [
        (.25, .5),
        (".25", ".375"),
        (13 / 1024, 3 / 512),
        ("13/1024", "3/512")
    ] )
    def test_dyadic(self, values: float) -> None:
        checkDyadic(values)

    @pytest.mark.parametrize("values", [
        (.1, .2),
        (.1, .4),
        (".258", ".375"),
        (13 / 1024, 3 / 2048)
    ])
    def test_error_dyadic(self, values: float) -> None:
        with pytest.raises(DASValueError, match="Non-dyadic-rational factor"):
            checkDyadic(values)
