Most of the DAS processing revolves around **queries** which are
selections of (optionally, coarsened) variables applied to a
particular geographic region (geounit) to yield a histogram for
different combinations of the variables' values. For example, the
query `votingage * cenrace` creates a histogram with 126 individual
scalar "slots", one for each combination of `votingage` (1 or 0) and
`cenrace` (63 possible values for the 2020 Census). The value in each
slot is the number of records in the region that have those particular
values (the as-enumerated number, when the query is computed on the
CEF; the estimated number, when the query is a noisy measurement, or
is computed from a Nonnegative Least Squares output, a Rounder output,
or the final MDF).

As implemented in the DAS, the strategy queries currently used are
implemented as matrices that can be efficiently represented by
Kronecker products, and which have a known sensitivity bound (as
described in "DAS Implementation Details"). The as-enumerated query
answers are then perturbed by the
[Discrete Gaussian Mechanism](https://arxiv.org/abs/2004.00010) with
a noise scale determined by the privacy-loss budget associated with
the specific query and geographic level (and by the sensitivity bound
known *a priori*). These perturbed measurements are then compressed and
saved in the geographic node's attributes for use in optimization to
generate privacy-protected histograms.
