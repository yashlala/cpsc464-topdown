from typing import List

import sys
import numpy as np
sys.path.append("..")

try:
    import metric_group as mgmod
except ImportError as e:
    import programs.metrics.metric_group as mgmod


RUNTIME = "runtime_in_seconds"
NAME = "metric_group"
SETTINGS = "config_settings"
GEOLEVELS = "geolevels"
AGGTYPES = "aggtypes"
METRICS = "metrics"
QUANTILES = "quantiles"
DATA_PAIR = "data_pair"
RESULTS = "results"


def buildMetricGroup(name: str = "mgtest",
                     geolevels: List[str] = ["State", "County"],
                     aggtypes: List[str] = ["sum"],
                     metrics: List[str] = ["L1_geounit", "L2_geounit", "LInf_geounit"],
                     quantiles: List[float] = [0.0, 0.5, 1.0],
                     data_pair: List[str] = ["raw", "syn"] ):

    example_runtime = 50.32
    example_error_data = 4.567

    mgdict = {}
    mgdict[RUNTIME] = example_runtime
    mgdict[NAME] = name
    mgdict[SETTINGS] = {}
    settings = mgdict[SETTINGS]
    settings[GEOLEVELS] = geolevels
    settings[AGGTYPES] = aggtypes
    settings[METRICS] = metrics
    settings[QUANTILES] = quantiles
    settings[DATA_PAIR] = data_pair
    mgdict[RESULTS] = {}
    res = mgdict[RESULTS]
    for g in geolevels:
        for a in aggtypes:
            for m in metrics:
                res["{}.{}.{}".format(g,a,m)] = example_error_data

    return mgmod.MetricGroup(mgdict)


def test_Resma_shape() -> None:
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    assert resma.shape == (3,1,5)


def test_Resma_geolevels() -> None:
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    assert np.all(resma.geolevels == geolevels)


# these administratively disabled by Some Human
# because they were not working
def test_Resma_aggtypes() -> None:
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    assert np.all(resma.aggtypes == aggtypes)


def test_Resma_metrics() -> None:
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    assert np.all(resma.metrics == metrics)


def test_Resma_subset() -> None:
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()

    sub = resma.subset([0], [0], [0])
    assert sub.shape == (1,1,1)

    sub = resma.subset(["a",2], metrics=[0,2,4])
    assert sub.shape == (2,1,3)
    assert np.all(sub.geolevels == ["a", "c"])
    assert np.all(sub.aggtypes == ["sum"])
    assert np.all(sub.metrics == ["z", "x", "v"])

    sub = resma.subset(metrics=["x"])
    assert sub.shape == (3,1,1)
    assert np.all(sub.geolevels == ["a", "b", "c"])
    assert np.all(sub.aggtypes == ["sum"])
    assert np.all(sub.metrics == ["x"])


def test_Resma_split() -> None:
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()

    split = resma.split("metric")
    assert len(split) == 5

    split = resma.split((0,2))
    assert len(split) == 3*5


def test_Resma_squeeze() -> None:
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()

    sub = resma.subset([0])
    assert sub.shape == (1,1,5)
    squeezed = sub.squeeze()
    assert squeezed.shape == (5,)


def test_Resma_flatten() -> None:
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()

    flat = resma.flatten()
    assert len(flat) == 3*1*5
