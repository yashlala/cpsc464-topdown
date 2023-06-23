import pytest
import numpy as np
import programs.schema.schema as sk
from das_constants import CC

SCHEMA_CROSS_JOIN_DELIM = CC.SCHEMA_CROSS_JOIN_DELIM


@pytest.fixture
def data():
    """
    set up data in the form of a numpy array
    the data will be an array of ones with the shape (2,3,5)
    """
    return np.ones((2, 3, 5))


@pytest.fixture
def schema():
    """
    build a simple schema that is compatible with 'data'
    Notes:
        the levels for 'a' are incorrect; there are too many levels, so this
        should default to ['a 0', 'a 1']

        the levels for 'b' are not specified; this should default to ['b 0', 'b 1', 'b 2']
    """
    name = "schema"
    dimnames = ['a', 'b', 'c']
    shape = (2, 3, 5)
    levels = {
        'a': ['a0', 'a1', 'a2'],
        'c': ['oranges', 'apples', 'bananas', 'kiwis', 'pineapples']
    }
    return sk.Schema(name, dimnames, shape, levels=levels)


def test_SchemaIsASchema(schema):
    assert isinstance(schema, sk.Schema), "Not a Schema object"


def test_SchemaHasCorrectName(schema):
    assert schema.name == "schema", "Name incorrect"


def test_SchemaHasCorrectDimnames(schema):
    assert np.all(schema.dimnames == ["a", "b", "c"]), "Dimnames are incorrect"


def test_SchemaHasCorrectShape(schema):
    assert np.all(schema.shape == (2, 3, 5)), "Shape incorrect"


def test_SchemaHasCorrectDefaultLevels(schema):
    dimnames = ["a", "b", "c"]
    shape = (2, 3, 5)
    default_levels = {}
    for d, dim in enumerate(dimnames):
        default_levels[dim] = [f"{dim}_{level}" for level in range(shape[d])]

    assert sk.getDefaultLevels(schema.dimnames, schema.shape) == default_levels, "Levels are incorrect"


def test_SchemaCorrectlyHandlesCustomLevels(schema):
    correct_levels = {
        'a': ['a_0', 'a_1'],
        'b': ['b_0', 'b_1', 'b_2'],
        'c': ['oranges', 'apples', 'bananas', 'kiwis', 'pineapples']
    }

    assert schema.levels == correct_levels, "Levels are incorrect"


@pytest.mark.parametrize("name, answer",
                         [
                             ('total', np.array([30])),
                             ('a', np.array([15, 15])),
                             ('b', np.array([10, 10, 10])),
                             ('c', np.array([6, 6, 6, 6, 6])),
                             # Cross as string
                             (f'a {SCHEMA_CROSS_JOIN_DELIM} b', np.array([5, 5, 5, 5, 5, 5])),
                             # Cross as tuple
                             (('a', 'b'), np.array([5, 5, 5, 5, 5, 5])),
                             # Cross as set
                             ({'a', 'b'}, np.array([5, 5, 5, 5, 5, 5])),
                             # Cross as dictionary
                             ({'a': 'whatever', 'b': None}, np.array([5, 5, 5, 5, 5, 5])),
                             (f'a {SCHEMA_CROSS_JOIN_DELIM} c', np.array([3, 3, 3, 3, 3, 3, 3, 3, 3, 3])),
                             # Cross as list
                             (['a', 'c'], np.array([3, 3, 3, 3, 3, 3, 3, 3, 3, 3])),
                             (f'b {SCHEMA_CROSS_JOIN_DELIM} c', np.array([2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2])),
                             ('detailed', np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])),
                         ])
def test_SchemaCorrectlyBuildsQueries(schema, data, name, answer):
    query = schema.getQuery(name)
    ans = query.answer(data)
    assert np.all(ans == answer), f"{name} query incorrect"


@pytest.mark.parametrize("queries", [
    ["total", "a", "b", "c", "a * b", "a * c", "b * c", "detailed"],
    ("total", "a", "b", "c", "a * b", "a * c", "b * c", "detailed"),
    {"total":0, "a":1, "b":2, "c":3, "a * b":4, "a * c":5, "b * c":6, "detailed":7},
    ("total", "a", "b", "c", ("a", "b"), ["a", "c"], {"a", "c"}, {"a":0, "c":0}, "b * c", "detailed"),
    {"total", "a", "b", "c", ("a", "b"), "b * c", "detailed"},
    ["total", "a", "b", "c", ("a", "b"), "b * c", "detailed"],
])
def test_SchemaCorrectlyBuildsQueryDictionary(schema, data, queries):
    """
    test that the query dictionary contains all the same queries as the individual getQuery calls
    """
    querydict = schema.getQueries(queries)
    for queryname, query in querydict.items():
        query_true = schema.getQuery(queryname)
        assert np.all(query.answer(data) == query_true.answer(data)), "Answers aren't the same"


@pytest.mark.parametrize("querynames, levels_true",
                         [
                             (('total',), np.array(['total'])),
                             (('a',), np.array(['a_0', 'a_1'])),
                             (('b',), np.array(['b_0', 'b_1', 'b_2'])),
                             (('c',), np.array(['oranges', 'apples', 'bananas', 'kiwis', 'pineapples'])),
                             # Cross as string
                             ((f'a {SCHEMA_CROSS_JOIN_DELIM} b',), np.array(['a_0 x b_0', 'a_0 x b_1', 'a_0 x b_2', 'a_1 x b_0', 'a_1 x b_1', 'a_1 x b_2'])),
                             # Cross as tuple
                             ((('a', 'b'),), np.array(['a_0 x b_0', 'a_0 x b_1', 'a_0 x b_2', 'a_1 x b_0', 'a_1 x b_1', 'a_1 x b_2'])),

                             ((('b', 'c'),), np.array(['b_0 x oranges', 'b_0 x apples', 'b_0 x bananas', 'b_0 x kiwis', 'b_0 x pineapples',
                                                    'b_1 x oranges', 'b_1 x apples', 'b_1 x bananas', 'b_1 x kiwis', 'b_1 x pineapples',
                                                    'b_2 x oranges', 'b_2 x apples', 'b_2 x bananas', 'b_2 x kiwis', 'b_2 x pineapples'])),

                             ((('a', 'c'),), np.array(['a_0 x oranges', 'a_0 x apples', 'a_0 x bananas', 'a_0 x kiwis', 'a_0 x pineapples',
                                                    'a_1 x oranges', 'a_1 x apples', 'a_1 x bananas', 'a_1 x kiwis', 'a_1 x pineapples'])),

                             (('detailed',), np.array(['a_0 x b_0 x oranges', 'a_0 x b_0 x apples', 'a_0 x b_0 x bananas', 'a_0 x b_0 x kiwis', 'a_0 x b_0 x pineapples',
                                                    'a_0 x b_1 x oranges', 'a_0 x b_1 x apples', 'a_0 x b_1 x bananas', 'a_0 x b_1 x kiwis', 'a_0 x b_1 x pineapples',
                                                    'a_0 x b_2 x oranges', 'a_0 x b_2 x apples', 'a_0 x b_2 x bananas', 'a_0 x b_2 x kiwis', 'a_0 x b_2 x pineapples',
                                                    'a_1 x b_0 x oranges', 'a_1 x b_0 x apples', 'a_1 x b_0 x bananas', 'a_1 x b_0 x kiwis', 'a_1 x b_0 x pineapples',
                                                    'a_1 x b_1 x oranges', 'a_1 x b_1 x apples', 'a_1 x b_1 x bananas', 'a_1 x b_1 x kiwis', 'a_1 x b_1 x pineapples',
                                                    'a_1 x b_2 x oranges', 'a_1 x b_2 x apples', 'a_1 x b_2 x bananas', 'a_1 x b_2 x kiwis', 'a_1 x b_2 x pineapples'])),
                         ])
def test_SchemaCorrectlyConstructsQueryLevels(schema, data, querynames, levels_true):
    """
    test that the querylevel function returns the correct labels for each level in a query
    """
    schema.getQueryLevels(querynames, cross_marker=' x ')
    levels = schema.getQueryLevels(querynames, cross_marker=' x ')[schema.marginal_name(querynames[0])]
    assert np.all(levels == levels_true), f"{querynames}'s levels are incorrect"


def test_getGroupings():
    dimnames = ["Apple", "Orange", "Strawberry", "Banana"]
    grouping = {"Apple": [[0], [1, 2, 3]], "Banana": [[4]]}
    assert sk.getGroupings(dimnames, grouping) == {0: [[0], [1, 2, 3]], 3: [[4]]}, "Grouping dictionary is incorrect"


def test_getAddOverMargins():
    dimnames = ["Apple", "Orange", "Strawberry", "Banana"]
    keepdims = ["Orange", "Banana"]
    assert sk.getAddOverMargins(dimnames, keepdims) == (0, 2), "Incorrect margins for adding over"


def test_crossLevels():
    """
    test crossLevels

    1. check that it returns the expected numpy array
    2. check that the order affects which dimensions are used
    3. check that the order dictates the order in which the levels appear within each crossed string
    """
    levels = {"Apple": ["Fuji", "Gala", "Honeycrisp"], "Berry": ["Blueberry", "Blackberry"], "Banana": ["Banana"]}
    order = ["Apple", "Berry", "Banana"]
    levels_true = np.zeros((3, 2, 1), dtype="object")
    for a, b, c in np.ndindex((3, 2, 1)):
        levels_true[(a, b, c)] = f"{levels['Apple'][a]} x {levels['Berry'][b]} x {levels['Banana'][c]}"
    assert np.all(sk.crossLevels(levels, order, cross_marker=' x ') == levels_true), "Levels incorrect"

    order = ["Apple", "Banana"]
    levels_true = np.zeros((3, 1), dtype="object")
    for a, b in np.ndindex((3, 1)):
        levels_true[(a, b)] = f"{levels['Apple'][a]} x {levels['Banana'][b]}"
    assert np.all(sk.crossLevels(levels, order, cross_marker=' x ') == levels_true), "Levels incorrect"

    order = ["Banana", "Berry"]
    levels_true = np.zeros((1, 2), dtype="object")
    for a, b in np.ndindex((1, 2)):
        levels_true[(a, b)] = f"{levels['Banana'][a]} x {levels['Berry'][b]}"
    assert np.all(sk.crossLevels(levels, order, cross_marker=' x ') == levels_true), "Levels incorrect"


def test_createRaiseError():
    with pytest.raises(ValueError) as err:
        sk.Schema("test", ['a', 'b', 'a'], (2, 2, 2))
    assert "Repeated variable name" in err.value.args[0]
