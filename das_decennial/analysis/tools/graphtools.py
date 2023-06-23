import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas
import seaborn as sns
import analysis.constants as AC
from das_constants import CC
import das_utils as du

GEOLEVEL_CATEGORIES = [CC.US, CC.STATE, CC.COUNTY, CC.TRACT_GROUP, CC.TRACT, CC.BLOCK_GROUP, CC.BLOCK, CC.CD, CC.SLDU, CC.SLDL]


def geolevel_tvd_lineplot(df, saveloc, product, state):
    unique_geolevels = df.geolevel.unique()
    geolevel_categories = [x for x in GEOLEVEL_CATEGORIES if x in unique_geolevels]
    df.geolevel = pandas.Categorical(df.geolevel, categories=geolevel_categories)
    queries = df[AC.QUERY].unique()
    print(queries)
    df.plb = df.plb.astype('float')
    for query in queries:
        data = df[df[AC.QUERY].isin([query])]
        print(data)
        columns = [AC.GEOLEVEL, AC.PLB, AC.RUN_ID, "1-TVD"]
        data = data[columns]
        data = data.sort_values([AC.PLB, AC.RUN_ID, AC.GEOLEVEL])

        minval = 0.0
        maxval = 1.01
        title = f"Statistic: {query}\nAccuracy as a Fxn of Privacy-Loss Budget (for {state}), Geolevel\n(Data Product: {product})"
        fig, ax = plt.subplots()

        plbs = [float(x) for x in data.plb.unique()]
        plb_max = max(plbs)

        data = data.groupby([AC.GEOLEVEL, AC.PLB], as_index=False).mean()
        print(data)
        for label, group in data.groupby(AC.GEOLEVEL):
            plot = group.plot(x='plb',
                              y='1-TVD',
                              ylim=(minval, maxval),
                              style=".-",
                              fontsize=6,
                              alpha=0.85,
                              ax=ax,
                              xlim=(-0.5, plb_max+0.5),
                              markersize=3,
                              linewidth=1.0,
                              label=label)
            #plot.set_xticks(group.plb)
            #plot.set_xticklabels(group.plb)
            plot.set_ylabel("1-TVD", fontsize=7)
            plot.set_xlabel("Privacy Loss Budget (PLB)", fontsize=7)
            ax.set_title(title, {"fontsize":8})

        legend = plt.legend(loc='lower right', frameon=False, fontsize=6, ncol=4, title="Geolevels")
        legend.get_title().set_fontsize(7)

        path = du.addslash(saveloc)
        fsqueryname = ".".join(query.split(" * "))
        query_saveloc = f"{path}geolevel_tvd_{fsqueryname}_lineplot.pdf"
        print(query_saveloc)
        plt.savefig(query_saveloc)
        plt.clf()

    plt.close()


def geolevel_tvd_heatmap(df, saveloc, product, state):
    unique_geolevels = df.geolevel.unique()
    geolevel_categories = [x for x in GEOLEVEL_CATEGORIES if x in unique_geolevels]
    df.geolevel = pandas.Categorical(df.geolevel, categories=geolevel_categories)
    queries = df[AC.QUERY].unique()
    print(queries)
    df.plb = df.plb.astype("float")
    for query in queries:
        data = df[df[AC.QUERY].isin([query])]
        print(data)
        columns = [AC.GEOLEVEL, AC.PLB, AC.RUN_ID, "1-TVD"]
        data = data[columns]
        data = data.sort_values([AC.GEOLEVEL])
        data = data.groupby([AC.GEOLEVEL, AC.PLB], as_index=False).mean()
        print(data)
        data = data.pivot(index=AC.GEOLEVEL, columns=AC.PLB, values="1-TVD")
        print(data.to_string())
        sns.set(font_scale=0.4)
        fig, ax = plt.subplots()
        title = f"Statistic: {query}\nAccuracy as a Fxn of Privacy-Loss Budget (for {state}), Geolevel\n(Data Product: {product})"
        plt.title(title, fontsize=6)
        snsplot = sns.heatmap(data, annot=True, linewidths=0.5, ax=ax, cbar=False, vmin=0.0, vmax=1.0, cmap="Blues", fmt=".4f")
        plot = snsplot.get_figure()
        path = du.addslash(saveloc)
        fsqueryname = ".".join(query.split(" * "))
        query_saveloc = f"{path}geolevel_tvd_{fsqueryname}_heatmap.pdf"
        print(query_saveloc)
        plot.savefig(query_saveloc)
        plt.clf()

    plt.close()


def age_quantile_lineplot(df, saveloc, product, state):
    unique_geolevels = df.geolevel.unique()
    geolevel_categories = [x for x in GEOLEVEL_CATEGORIES if x in unique_geolevels]
    df.geolevel = pandas.Categorical(df.geolevel, categories=geolevel_categories)
    query = df[AC.QUERY].unique().tolist().pop()
    category = df['category'].unique().tolist().pop()
    category = " * ".join(category.split("."))
    df.plb = df.plb.astype('float')
    for percentile in df['percentile'].unique():
        data = df[df['percentile'].isin([percentile])]
        print(data)
        columns = [AC.GEOLEVEL, AC.PLB, AC.RUN_ID, 'avg(quantile_L1)']
        print(columns)
        data = data[columns]
        data = data.sort_values(columns)
        data['negative_avg(quantile_L1)'] = data['avg(quantile_L1)'] * -1

        minval = min(data['negative_avg(quantile_L1)'])
        maxval = max(data['negative_avg(quantile_L1)']) + 1
        title = f"Statistic: {query} | Category: {category}\nAccuracy as a Fxn of Privacy-Loss Budget (for {state}), Geolevel\n(Data Product: {product})"
        fig, ax = plt.subplots()

        plbs = [float(x) for x in data.plb.unique()]
        plb_max = max(plbs)

        data = data.groupby([AC.PLB, AC.GEOLEVELS], as_index=False).mean()
        print(data)
        for label, group in data.groupby(AC.GEOLEVELS):
            plot = group.plot(x='plb',
                              y='negative_avg(quantile_L1)',
                              ylim=(minval, maxval),
                              style=".-",
                              fontsize=6,
                              alpha=0.85,
                              ax=ax,
                              xlim=(-0.5, plb_max+0.5),
                              markersize=3,
                              linewidth=1.0,
                              label=label)
            #plot.set_xticks(group.plb)
            #plot.set_xticklabels(group.plb)
            plot.set_ylabel("", fontsize=7)
            plot.set_xlabel("Privacy Loss Budget (PLB)", fontsize=7)
            ax.set_title(title, {"fontsize":8})

        legend = plt.legend(loc='lower right', frameon=False, fontsize=6, ncol=4, title='Geolevels')
        legend.get_title().set_fontsize(7)

        path = du.addslash(saveloc)
        fsqueryname = ".".join(query.split(" * "))
        query_saveloc = f"{path}age_quantile_{fsqueryname}_lineplot_percentile_{percentile}.pdf"
        print(query_saveloc)
        plt.savefig(query_saveloc)
        plt.clf()

    plt.close()
