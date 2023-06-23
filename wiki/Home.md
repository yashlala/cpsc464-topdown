This wiki contains technical documentation for the Census Bureau's Top-Down Algorithm (TDA), the
Disclosure Avoidance System (DAS) used in protecting confidential
information provided by respondents during the 2020 Decennial Census
of Population and Housing, during the first two major product releases (Redistricting, Demographic and Housing Characteristics). This particular release consists of the
code and parameters used to generate the privacy-protected Microdata
Detail File (using
[zero-Concentrated Differential Privacy](https://arxiv.org/abs/1605.02065)
via
[an exact sampler for the Discrete Gaussian Mechanism](https://arxiv.org/abs/2004.00010)
for its primary confidentiality protection primitives, which were post-processed into microdata by solving a complex sequence of quadratic and mixed-integer linear programs), which was in
turn used to produce the
[2020 Census Demographic and Housing Characteristics File](https://www.census.gov/data/tables/2023/dec/2020-census-dhc.html)
(DHC).

The Census Bureau uses a variety of different disclosure avoidance
methods for its various products and two distinct formally private
methods for the 2020 Decennial products. For this wiki, we will
interchangably use the terms DAS (Disclosure Avoidance System) and TDA
(Top Down Algorithm) to refer to the system used for privacy
protection of the 2020 Redistricting and DHC products.

This documentation applies to the particular version of the DAS/TDA
source code and parameters used to generate the privacy-protected
microdata file over which tabulations were calculated to generate the
Census DHC publication. Additional supporting documentation will be
added as it becomes available.

# Annotated Table of Contents

* [Background](Background.md) describes the purpose of the 2020 Census 
  and motivates the need for disclosure avoidance
  in published tabulations. It also summarizes the history of
  disclosure avoidance methods in past censuses and the underlying
  design decisions for the 2020 DAS.

* [DAS Implementation](DAS-Implementation-Overview) describes the
  implementation of the 2020 DAS and in particular of the TopDown
  Algorithm (TDA) used to produce privacy-protected block-level
  microdata used for generating the published tabulations,

* [Implementation Details](DAS-Implementation-Details) goes into more
	in-depth description of the individual phases of the TDA,

* [DAS Components](DAS-Components) provides a guide to the source code and
   modules constituting the DAS. The published source code collapses different
   submodules into a single directory tree.

* [The Query Object](DAS-Queries) describes a key component
    of the DAS software architecture.

* [DAS Infrastructure](DAS-Decennial-Infrastructure) describes the
  computational infrastructure used for processing the 331 million
  individuals and 156 million living quarters (housing units and occupied group quarters) enumerated by the 2020
  Census. The DAS ran in a secure cloud environment and took advantage
  of a high-performance computing platform called "Elastic Map Reduce" (EMR).
    * [EMR Configuration](DAS-EMR-Configuration) describes the configuration of
      individual nodes within the EMR clusters used for the DAS.

* [Authors](Authors) lists some key contributors to the creation and
application of the Decennial DAS whose contributions are gratefully acknowledged.

# Publications

Here are scientific papers about the semantics, mathematics, and design of the DAS system. 

* [Census TopDown Algorithm: Differentially Private Data, Incremental Schemas, and Consistency with Public Knowledge](https://hdsr.mitpress.mit.edu/pub/7evz361i/release/2?readingCollection=63678f6d), published in the Harvard Data Science Review (note that this article concentrates on the algorithm as of its use for the Redistricting release; an article update is in preparation to reflect changed made to TDA for production of the DHC product).
* [Geographic Spines in the 2020 Census Disclosure Avoidance System TopDown Algorithm](https://arxiv.org/abs/2203.16654v1), which describes the dynamically constructed geographic hierarchy over which TDA operates; readers should note that this geographic hierarchy differs from those used in other Census applications.
* [An Uncertainty Principle is a Price of Privacy-Preserving Microdata](https://proceedings.neurips.cc/paper/2021/file/639d79cc857a6c76c2723b7e014fccb0-Paper.pdf), published in the proceedings of the NeurIPS 2021 Conference.
* [Bayesian and Frequentist Semantics for Common Variations of Differential Privacy: Applications to the 2020 Census](https://arxiv.org/abs/2209.03310), a paper draft exploring several important ways in which the privacy guarantees associated with TDA can be stated in concrete terms.

As papers become available and new relevant papers are added, this
page will be updated and electronic copies will be placed in the
`docs` subdirectory of this repository. To receive regular updates on
new papers, releases, and results, subscribe to our
[newsletter](https://www.census.gov/programs-surveys/decennial-census/decade/2020/planning-management/process/disclosure-avoidance/2020-das-updates.html).
