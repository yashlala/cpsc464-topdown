import pytest
from programs.engine.budget import Budget
from exceptions import DASConfigValdationError


def test_check_unique() -> None:
    with pytest.raises(DASConfigValdationError, match="are slated to be measured more than once in config section/options"):
        Budget.QueryBudget.checkUnique(("a * b", "b * a"),'option')
