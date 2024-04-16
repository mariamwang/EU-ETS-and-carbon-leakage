clear all
set more off
global PATH "write path here"

***************** Carbon leakage analysis **********************

***** 1. Create carbon intensity data
do "${PATH}\carbon_intensity_ee"

***** 2. Edit Comtrade data (HS1996 -> ISIC Rev.3, add RTA data, create EU ETS dummies...)
do "${PATH}\data_edit_ee"

***** 3. Run the different regressions (reghdfe & ppml)
do "${PATH}\regressions_ee_new"

***** 4. Countrfactual analysis
do "${PATH}\counterfactual_ee"

***** 5. Extensive margin
do "${PATH}\extensive_margin_ee"