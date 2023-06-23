import pandas

def getDefaultLevels(dimnames, shape):
    leveldict = {}
    for d,dim in enumerate(dimnames):
        labels = [f"{dim}_{i}" for i in range(shape[d])]
        indices = [[i] for i in range(shape[d])]
        leveldict[dim] = dict(zip(labels, indices))

    return leveldict


def getLevels(labeldict):
    leveldict = {}
    dimnames = list(labeldict.keys())
    for d,dim in enumerate(dimnames):
        labels = labeldict[dim]
        indices = [[i] for i in range(len(labels))]
        leveldict[dim] = dict(zip(labels, indices))

    return leveldict


def pretty(leveldict):
    items = ["leveldict = {"]
    for dim,lev in leveldict.items():
        items += [f'    "{dim}": ' + "{"]
        lens = [len(x) for x in lev.keys()]
        maxlen = max(lens)
        diffs = [maxlen - x for x in lens]
        items += [",\n".join([f'        "{k}"' + f" "*diffs[i] + f": {v}" for i,(k,v) in enumerate(lev.items())])]
        items += ["    },\n"]

    items += ["}"]

    return "\n".join(items)


def prettyList(thelist):
    return ",\n".join([f'"{x}"' for x in thelist])



def getSchemaQueryInfo(schema, max_num=None):
    """ generates a pandas dataframe of the base queries in the schema """
    if max_num is None:
        seed_names = schema.base_queries
    else:
        seed_names = schema.base_queries[0:max_num]

    seeds = [schema.getQuerySeed(x) for x in seed_names]
    seed_dicts = []
    for seed in seeds:
        dimensions = ", ".join(seed.keepdims)

        if seed.name in schema.dimnames:
            qtype = "dim"
        elif seed.name in ['total', 'detailed']:
            qtype = seed.name
        else:
            qtype = "recode"

        seed_dicts.append(
            {
                'query'     : seed.name,
                'dimensions': dimensions,
                'type'      : qtype,
                'crossable' : False if seed.name in ['total', 'detailed'] else True,
                'size'      : seed.getShape(size=True),
            }
        )

    return pandas.DataFrame(seed_dicts)[['query', 'size', 'dimensions', 'type', 'crossable']]

# def getQueryDimDict(queryinfo_df):
#     dimdict = {}
#     for dim in queryinfo_df['dimensions'].unique():
#         dimdict[dim] = queryinfo_df[queryinfo_df['dimensions'] == dim]
#     return dimdict


def wikidims(schema):
    """ formats the schema's info into the mediawiki table format """
    items = [
        '{| class="wikitable" style="text-align:center;"',
        '! Dimension / Attribute || Number of Levels || Total Size of Histogram'
    ]
    dimshape = [(schema.dimnames[i], schema.shape[i]) for i in range(len(schema.dimnames))]
    for d,dims in enumerate(dimshape):
        items.append("|-")
        if d == 0:
            items.append(f'| {dims[0]} || {dims[1]} || rowspan="{len(dimshape)}" | {schema.size}')
        else:
            items.append(f'| {dims[0]} || {dims[1]}')
    items.append("|}")
    return "\n".join(items)


def wikiqueries(queryinfo_df):
    """ formats the query dataframe (from getSchemaQueryInfo) into the mediawiki table format """
    order = ['query', 'size', 'dimensions', 'type']
    items = [
        '{| class="wikitable" style="text-align:center;"',
        '! Query || Size || Dimensions || Type'
    ]
    df = queryinfo_df
    records = df.to_dict('records')
    for r,rec in enumerate(records):
        items.append("|-")
        row = " || ".join([str(rec[x]) for x in order])
        items.append(f'| {row}')
    items.append("|}")
    return "\n".join(items)
