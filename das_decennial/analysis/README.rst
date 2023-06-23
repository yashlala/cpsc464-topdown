==================
Spark SQL Analysis
==================

Analysis is built on top of the Spark DataFrame (SQL-based) framework. It allows for complex analyses of experiment data from the Disclosure Avoidance System (DAS).

====================
Instructions for Use
====================

1. Create a script that is similar to any of the `vignettes <https://{GIT_HOST_NAME}/CB-DAS/das_decennial/tree/master/analysis/vignettes>`_ or `analysis scripts <https://{GIT_HOST_NAME}/CB-DAS/das_decennial/tree/master/analysis/analysis_scripts>`_.
   * These scripts don't need to be stored anywhere special. But, for organization purposes, I have chosen to store them in `analysis/vignettes/` and `analysis/analysis_scripts/`.
2. Run the script using the command: ``analysis=[your analysis script] bash run_analysis.sh``
   * Example: ``analysis=vignettes/simple_analysis_script.py bash run_analysis.sh``
   * Note: To run Analysis, you'll have to be in (i.e. ``cd`` into) the ``das_decennial/analysis/`` directory.
3. That's it! You can check out the log file and/or analysis results (which need to be manually saved in the script) in their respective locations.
   * The log file will be printed out along with the ``tail -f [log file]`` statement so you can just copy and paste it into the command line to watch the log file as analysis runs.

-------------------------------
Using Analysis Tools in the DAS
-------------------------------

It's also possible to use Analysis's various toolsets from within the DAS, as it is running. The tools can be used anywhere in the DAS, but the place that makes the most sense is in the **error_metrics** module.

Note that analyzing from within the DAS is best suited for evaluating single runs or new experimental methods. When analyzing experiments (e.g. averages across different runs of the DAS), it's best to use the standalone version of Analysis as described above in the **Instructions for Use** section of this readme.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Example of Using Analysis within the DAS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The example uses the **PL94 DAS Schema** and **Rhode Island CEF data** and computes the **Geolevel 1-TVD metric** for `these geolevels <https://{GIT_HOST_NAME}/CB-DAS/das_decennial/blob/master/configs/PL94/topdown_RI_using_analysis.ini#L168>`_ and `these queries <https://{GIT_HOST_NAME}/CB-DAS/das_decennial/blob/master/configs/PL94/topdown_RI_using_analysis.ini#L169>`_.
* `config.ini <https://{GIT_HOST_NAME}/CB-DAS/das_decennial/blob/master/configs/PL94/topdown_RI_using_analysis.ini#L164>`_ - This configuration file is the same as any other config file; it just uses a different error_metrics module that now includes the analysis tools.
* `error_metrics module <https://{GIT_HOST_NAME}/CB-DAS/das_decennial/blob/master/programs/metrics/using_analysis.py>`_ - In this example, we're going to compute (and print to the output/log) the Geolevel 1-TVD metric for the geolevels and queries listed in the config file.
