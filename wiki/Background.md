In 2020, the United States Census Bureau conducted the 2020
Census. Known formally as the Decennial Census of Population and
Housing, the census aims to enumerate every person residing in the
United States, covering all 50 states, the District of Columbia, and
Puerto Rico. (A separate census was conducted for the Island Areas,
and those data are out-of-scope for this disclosure avoidance
system. All persons alive on April 1, 2020 residing in these places,
according to residency criteria finalized in 2018, must be counted.

Following completion of the census, the Census Bureau must submit
state population totals to the Secretary of Commerce, who then conveys
them to the President. The United States Constitution mandates this
decennial enumeration be used to determine each state’s Congressional
representation.

Public Law 94-171 directs the Census Bureau to provide data to the
governors and legislative leadership in each of the 50 states for
redistricting purposes. This data, with disclosure-avoidance noise infused by TDA, was released
in August of 2021 and the source code used for those privacy
protections was publicly released in
[September 2021](https://github.com/uscensusbureau/DAS_2020_Redistricting_Production_Code).

The Census Bureau also uses the data gathered through the 2020 census
to release geographically organized statistical information regarding
both individuals and households/housing units across the United States and Puerto
Rico. The 2020 Census Demographic Profile and Housing characteristics
release includes tabulations around individual age, sex, race, and
ethnicity as well as household/housing unit characteristics. These tabulations are
organized around detailed geographic areas with
[Census Blocks](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_5)
forming the finest degree of geographic resolution).

This software release consists of the source code used to
generate the privacy-protected microdata files which were the basis of
the tabulations included in the 2020 DHC release.

[Title 13](https://www.govinfo.gov/content/pkg/USCODE-2007-title13/pdf/USCODE-2007-title13.pdf)
Sections 8(b) and 9 of the U.S. Code require the Census Bureau to
ensure the confidentiality of individuals' census
responses. Specifically, the Census Bureau must not "make any
publication whereby the data furnished by any particular establishment
or individual under this title can be identified." The 2020 Top-Down Algorithm embodied in this code release formalizes and
quantifies this goal: by using zero-Concentrated Differential Privacy
primitives, TDA is able to provide provable bounds on how
much more an "attacker" could learn about a target individual than
could have been possible even if most features of the individual's
data had been replaced with placeholder
values.<sup>[1](#footnote1)</sup> The DAS also guarantees bounds on
the amount that can be learned about a specific characteristic of an
individual respondent, beyond what could already have been guessed
about that characteristic if the individual's response for that
characteristic had been replaced with a placeholder value.

In previous decennial censuses, a variety of techniques were used to
protect the confidentiality of responses, including the use of
household swapping and, for group quarters records, partially
synthetic data (as documented in
[Section 7-6 of the 2010 PL94-171 Redistricting Data Summary File technical documentation](https://www2.census.gov/prod2/cen2010/doc/pl94-171.pdf),
and
[on page 10 of "Disclosure Avoidance Techniques" Used for the 1970 through 2010 Decennial Censuses of Population and Housing"](https://www.census.gov/library/working-papers/2018/adrm/cdar2018-01.html)). For
the 2020 Census, the Census Bureau applied the latest science in
developing the 2020 Census Disclosure Avoidance System
(DAS). Following the instructions of the
[Data Stewardship Executive Policy Committee](https://www.census.gov/about/policies/privacy/data_stewardship/dsep_committee.html)
(DSEP), the Census Bureau implemented a sequence of algorithms whose
privacy-conferring primitives were differentially private, including
in particular the DAS code contained in the current
repository. Differential privacy is a confidentiality protection
framework that allows for a quantitative, general accounting of the
privacy loss to respondents associated with any set of tabulation
releases, and so allows for the Census Bureau to bound how much
privacy loss is allowed<sup>[2](#footnote2)</sup>, consistent with its
Title 13 responsibilities.

This public release of source code used in protecting the 2020 Census
Demographic and Housing Data Characteristics release is intended to
help sophisticated external users by providing a transparent view into
the exact computer code and parameter settings used in the 2020 DHC
production run of the DAS. It is notable that this transparency is a
feature specifically enabled by the use of differentially private
algorithms: the reader will note that the 2010 documentation on
household swapping previously linked is intentionally vague about its
implementation. The 2010 documentation does not include algorithmic
pseudo-code, nor a public code release. This lack of detail is
important, because swapping does not provide mathematical proofs of
privacy guarantees against general attackers that hold even if the
attacker knows the algorithm in use. Differentially private (and
zero-Concentrated Differentially Private) algorithms do, however, have
this property, allowing the transparent publication of comprehensive
algorithmic detail, including this code repository, without
endangering the worst-case privacy loss bounds. Note that the exact
values of the random noise used by the differentially private
mechanism remain confidential.

# Overview

Article 1 Section 2 of the U.S. Constitution directs Congress to
conduct an “actual enumeration” of the population every ten years. In
2020, the Census Bureau conducted the 24th Decennial Census of
Population and Housing with reference date April 1, 2020 and has begun
producing public-use data products that conform to the requirements of
Title 13 of the U.S. Code. The goal of the census is to count everyone
once, only once, and in the right place.  All residents must be
counted. After the data have been collected by the Census Bureau, but
before the data are tabulated to produce statistical products for
dissemination, the confidential data must undergo statistical
disclosure limitation so that the impact of statistical data releases
on the confidentiality of individual census responses can be
quantified and controlled. The Census Bureau calls the application of
statistical disclosure limitation methods "disclosure avoidance."

In the 2010 Census, the trade-off between accuracy and privacy
protection was viewed as a technical matter to be determined by
disclosure avoidance statisticians. Disclosure avoidance was performed
primarily using household-level record swapping and was supported by
maintaining the secrecy of key disclosure avoidance
parameters. Disclosure avoidance algorithm code was not released,
though high-level summaries were provided, and expanded upon later, as
described above.

However, there is a growing recognition in the scientific community
that record-level household swapping fails to provide provable
confidentiality guarantees when the side information and computational
power to attackers is unknown. In the absence of these properties, and
as a consequence of the rapid growth outside the Census Bureau in
access to high-powered computing, sophisticated algorithms, and
external databases, the Census Bureau acknowledged that it may be
possible to reconstruct a significant portion of the confidential data
that underlies the census data releases using a so-called database
reconstruction attack, as originally outlined by
[Dinur and Nissim](https://dl.acm.org/doi/10.1145/773153.773173)
(2003). Once reconstructed, microdata can easily be used to attempt to
re-identify individual respondents' records, and to attempt to infer
characteristics about those individuals that may only be learnable
because they participated in the
Census<sup>[3](#footnote3)</sup>. Indeed, in 2019 the Census Bureau
announced that it had performed a database reconstruction attack using
just a portion of the publicly available 2010 decennial census
publications, and had been able to reconstruct microdata that was
overwhelmingly consistent with the 2010 confidential microdata. The
reconstructed 2010 microdata violated the in-place 2010 disclosure
avoidance procedures for microdata releases
([McKenna 2019](https://www2.census.gov/adrm/CED/Papers/CY19/2019-04-McKenna-Six%20Decennial%20Censuses.pdf))
that were applied to the 2010 Public-Use Microdata Sample in the
following ways. First, the reconstructed 2010 microdata constitute one
record for every person enumerated in the census, not a sample as
required by the 2010 disclosure avoidance procedures for
microdata. Second, the minimum population in the geographic identifier
(census block) in the reconstructed 2010 microdata was a single
person, not the 100,000 person minimum population size specified in
the 2010 disclosure avoidance procedures for microdata. Third, the
reconstructed 2010 microdata identified individuals in basic
demographic categories with fewer than 10,000 members nationwide even
though the 2010 disclosure avoidance standard for microdata specified
a minimum national population of 10,000 for basic demographic marginal
tables. These violations of the 2010 disclosure avoidance standards
occurred because the possibility of reconstructing the record-level
microdata from the 150 billion published tabulations was not
explicitly considered when clearing the tabular summaries.

In order to fulfill its requirements to produce an accurate count and
to protect personally identifiable information, the Disclosure
Avoidance System (DAS) for the 2020 Census implements mathematically
rigorous disclosure avoidance controls based primarily on the
mathematical framework known as differential privacy (and,
specifically, zero-Concentrated Differential Privacy). In its
production use, the DAS reads the Census Edited File (CEF) and applies
formally private algorithms to produce a Microdata Detail File
(MDF),<sup>[4](#footnote4)</sup>. This MDF is then used to generate
published tabulations and extracts, take advantage of post-processing
formal privacy guarantees.

The DAS can be thought of as a filter that allows some aspects of data
to pass through the filter with high fidelity, while controlling the
leakage of confidential information to no more than the level
permitted by the differential privacy budget parameters. By policy,
all data that are publicly released by the U.S. Census Bureau based on
the 2020 Census must go through some form of mathematically defensible
formal privacy mechanism.

# DAS Design Decisions

Many of the principal features, requirements, and parameters of the
Census Bureau’s implementation of the DAS for production of the 2020
DHC microdata were policy decisions made by the Census Bureau’s Data
Stewardship Executive Policy Committee (DSEP).  The DSEP policy
decisions impacting DAS design include: the list of invariants (those
data elements to which no noise is added); the overall privacy-loss
budget; and the allocation of the privacy-loss budget across
geographic levels and across queries. While DSEP is responsible for
significant decisions, actions, and accomplishments of the 2020 Census
Program, the Associate Director for Decennial Programs publicly
documents these policies in the 2020 Census Decision Memorandum Series
for the purpose of informing stakeholders, coordinating
interdivisional efforts, and documenting important historical
changes. This memorandum series is available at the
[2020 Census Memorandum Series public website](https://www.census.gov/programs-surveys/decennial-census/decade/2020/planning-management/plan/memo-series.html).

<a name="footnote1">1</a>: Although provable guarantees are also
possible when comparing to a counterfactual world in which the
individual respondent's entire record was replaced by placeholder
values, these guarantees are made more complicated by the presence of
invariants -- statistics that the DAS may not perturb by policy. Note
that the use of a counterfactual world in which the reference person's
data was not present, in part or whole, is a common way of *defining*
privacy (and privacy loss) in the differential privacy literature; the
intuition behind this definition is that, if a person's data was not
collected or was replaced with placeholder values, then any inferences
enabled about that person by the publication are sociological or
scientific inferences, and do not violate their
privacy. [Dwork and Roth](https://www.cis.upenn.edu/~aaroth/Papers/privacybook.pdf)
(pages 9-10) illustrate this with a common "smoking causes cancer"
example: scientific research on the link between smoking and cancer
can be used to make improved inferences about whether a person may
have or develop cancer (if whether they smoke or not is known as side
information), but this inference is not typically viewed as a privacy
violation. A confidentiality breach occurs, in the course of such a
study, if the specific smoker/nonsmoker status of a participant in the
study can be inferred from the published summary statistics, but could
not have been inferred had the individual not participated in the
study. Differentially private confidentiality protection mechanisms
permit accurate inferences of the form "smoking causes cancer" while
limiting the accuracy of inferences of the form "John Doe is a
smoker", relative to how accurate such an inference could be in a
world where John Doe had not participated in the study. The
smoker/nonsmoker status of John Doe must be inferred from information
not specific to data contributed as a direct result of John Doe's
participation in the study.

<a name="footnote2">2</a>: It would, of course, be convenient if
*zero* privacy loss could be achieved. One of the key lessons of the
differential privacy literature, however, is that this is impossible,
unless significant knowledge is available about possible attackers, so
that strong assumptions can be made regarding their side information
and computational abilities. These assumptions are difficult to
justify when considering general-purpose, public data releases like
the DHC data release, the PL94-171 Redistricting Data Summary File and
the other decennial census data products.

<a name="footnote3">3</a>: This distinction is important: even if a
respondent does not participate in the decennial census, it may be
easy for an attacker to use published census tabulations to infer some
of their characteristics. For example, an attacker may know that
Person A resides in the state of Montana. In the 2010 Census, nearly
90% of Montana residents were tabulated as "White Alone;" hence, even
if Person A had not participated in the 2010 Census, just by observing
the published tabulations and knowing that Person A lives in Montana,
an attacker could form a high-probability guess about the respondent's
census-reported race. This kind of inference is not protected by the
DAS because it is a statistical inference about the characteristics of
the population of Montana, not an identifying inference about Person
A's race. The DAS is designed to control how much *more* an attacker
could have learned than if a respondent had not participated in the
decennial census, or if some of their data were replaced with
placeholder values, compared to what is learned from the statistics
that use the respondent's actual data. Or, put somewhat differently,
the DAS takes as its definition of "private (or confidential)
information" the information that is unique to a respondent, in the
sense of only being learnable because of their data's presence and use
in the published tabulations. Information that can be learned without
a respondent participating, classified as scientific or sociological
inference (which may desirable or undesirable) is outside the scope of
the DAS to control. In effect, the differential privacy literature
identifies "private (or confidential) information" as that information
that is directly attached to the respondent's identity and it cannot
be learned from studying other individuals' data (though, as a
framework, differential privacy is flexible enough to allow this
definition to encompass family members as well, if desired). This
method of defining private information naturally strengthens
pre-differential-privacy notions of privacy, which often, for example,
focused on the frequency of sample or population "uniques" (records
that are the only one of their kind in the response data) -- often,
exactly the records that are most difficult for an attacker to learn,
if they are not used in the statistical tabulations, and so also
emphasized by differential privacy.

<a name="footnote4">4</a>: Generation of privacy-protected microdata
is not necessary, and is arguably unusual, when working within a
differentially private framework, but was regarded as an important
design requirement by DSEP for the 2020 data releases.
