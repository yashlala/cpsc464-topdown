from programs.nodes.nodes import GeounitNode
from programs.sparse import multiSparse
import das_utils as du
import numpy as np
import pandas
import analysis.tools.mappers as mappers
import programs.sparse as sp
import scipy.sparse as ss


def getToyGeounit_SparseMatrixGenerator(geocode, schema, density=0.01, scale=10):
    raw_mat = np.round(ss.random(1, schema.size, format='csr', density=density) * scale)
    syn_mat = np.round(ss.random(1, schema.size, format='csr', density=density) * scale)
    raw_sparse = sp.multiSparse(raw_mat, schema.shape)
    syn_sparse = sp.multiSparse(syn_mat, schema.shape)
    return { 'geocode': geocode, 'raw': raw_sparse, 'syn': syn_sparse }


def getToyGeounitData_dict(num_geocodes, schema, density=0.01, scale=10):
    geocodes = [str(x).zfill(16) for x in range(num_geocodes)]
    geounits = [getToyGeounit_SparseMatrixGenerator(x, schema, density, scale) for x in geocodes]
    return geounits


def getToyGeounitData_GeounitNode(
        schema,
        geocodes=['000', '001', '002', '003', '010', '011', '012', '020', '022'],
        geocode_dict={3: 'block', 2: 'county'},
        raw_params={'low': 0, 'high': 2},
        syn_params={'low': 0, 'high': 5}
                     ):
    geounits = []
    for geocode in du.aslist(geocodes):
        if raw_params is not None:
            raw = np.random.randint(low=raw_params['low'], high=raw_params['high'], size=schema.size).reshape(schema.shape)
        if syn_params is not None:
            syn = np.random.randint(low=syn_params['low'], high=syn_params['high'], size=schema.size).reshape(schema.shape)
        geounits.append(GeounitNode(geocode=geocode, geocode_dict=geocode_dict, raw=multiSparse(raw), syn=multiSparse(syn)))
    return geounits


def getToySparseHistDF(geounit_data, schema):
    records = []
    for geounit in du.aslist(geounit_data):
        records += mappers.getSparseDF_mapper(geounit, schema)
    df = pandas.DataFrame(records)

    column_order = ['geocode'] + schema.dimnames
    df = df[column_order + [x for x in df.columns if x not in column_order]]
    return df


def getToyExample(schema):
    geounits = getToyGeounitData_GeounitNode(schema)
    df = getToySparseHistDF(geounits, schema)
    return geounits, df
