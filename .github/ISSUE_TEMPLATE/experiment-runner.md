---
name: Experiment Runner
about: Experiment will generate all permutations of the experiment parameters, and
  run multiple experiments.
title: Experiment
labels: Cowan, Experiment
assignees: cowan321

---

**Parameters not defined here on the template will be left as default**

#Description:
**Provide a quick description of the experiment**

#ClusterConfiguration
Release: **030**
EmrVersion: **emr-6.2.0 | emr-6.2.1**

#SparkParameters
ExecutorCores: **40**
ExecutorsPerNode: **1**
DriverCores: **10**
DriversMemory: **40g**
ExecutorMemory: **423g**
ExecutorMemoryOverhead: **330g**
ShufflePartitions: **2400**
DynamicAllocation: **False**

#DAS End-to-end Run Options
DataSource: **[2010 | Production]**
SingleState: **[72]**
States: **[01 02 04 05 06 08 09 10 11 12 13 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 44 45 46 47 48 49 50 51 53 54 55 56] | NONE**

#Multi-Experiment Runner Options
GitBrangh: **specific branch or left blank for master**


Spines (Select one or more): **["aian_spine", "opt_spine", "non_aian_spine", "ALL"]**
PLBS (Select one or more): **(plbs ints or fraction-parsable) Valid example values are 1/10 1/1 2/1 4/1 8/1 263/100**
Steps (Select one or more): **List of Steps to run, as a string. Valid values are 1,2,3,4,5,6**
Configuration: **The name of the set of configs.**
Geolevel: **The geolevel to target for an experiment run. [Block | Block_Group | Tract | Country | State]**
IdealOrPLBRatio: **Select either [ Ideal | PLB Ratio]**
Ratio: **The geolevel ratios to use for an experiment run. The value specified here will only be used if the IdealOrPLBRatio is configured to PLBRatio, otherwise it is disregarded.**

Custom Parameters are the following format: section:subsection
Custom Values can be space seperated will generate combinations of experiments
CustomParameter01:
CustomValue01:
CustomParameter02:
CustomValue02:
CustomParameter03:
CustomValue03:
CustomParameter04:
CustomValue04:
CustomParameter05:
CustomValue05:
