* Clear memory and set parameters
	clear all
	set more off
	set maxvar 30000
	
* Set directory path
	global data "write data path here"
	global results "write results path here"

***************************PPML analysis**********************************
	
	*Baseline regressions
	
	cd "$data"

	use "comtrade_isicrev3_2000_2018_intra_trade_final2.dta", clear
	
	egen pair_id = group(country partner)
	egen country_time = group(country year)
	egen partner_time = group(partner year)
	egen sector_time = group(isic_rev3 year)
	egen imp_sector_time = group(country isic_rev3 year)
	egen exp_sector_time = group(partner isic_rev3 year)
	egen pair_time = group(country partner year)
	encode isic_rev3, gen(isic_rev3_id)
	
	foreach v of var eu_ets eu_ets_imp eu_ets_exp all_ets all_ets_imp all_ets_exp swiss_ets korea_ets eu_ets_placebo eu_ets_imp_placebo eu_ets_exp_placebo {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "24", "26", "27")
	replace `v' = 0 if isic_rev3 == "24" & inlist(year, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012)
	}

	foreach v of var eu_ets_phase1 eu_ets_phase2 eu_ets_imp_phase1 eu_ets_imp_phase2 eu_ets_exp_phase1 eu_ets_exp_phase2 {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "26", "27")
	}
	
	foreach v of var eu_ets_phase3 eu_ets_imp_phase3 eu_ets_exp_phase3 {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "24", "26", "27")
	}
	
	drop year_*
	
	replace co2_intensity = . if co2_intensity == 0
	replace co2_content = . if co2_content == 0
	
	cd "$results"
	
	cap erase "ppmlhdfe_intra.xml"
	cap erase "ppmlhdfe_intra.txt"
	
	cap erase "ppmlhdfe_intra_option1.xml"
	cap erase "ppmlhdfe_intra_option1.txt"
	
	*Review round 2, this goes to robustness: separate ETS intra-trade dummy, change the ETS variable to equal 0 for internal trade
	gen eu_ets_intra = 0
	replace eu_ets_intra = 1 if (eu_ets_exp == 1 | eu_ets_imp == 1) & country == partner
	
	gen eu_ets_phase1_intra = 0
	replace eu_ets_phase1_intra = 1 if (eu_ets_imp_phase1 == 1 | eu_ets_exp_phase1 == 1) & country == partner
	gen eu_ets_phase2_intra = 0
	replace eu_ets_phase2_intra = 1 if (eu_ets_imp_phase2 == 1 | eu_ets_exp_phase2 == 1) & country == partner
	gen eu_ets_phase3_intra = 0
	replace eu_ets_phase3_intra = 1 if (eu_ets_imp_phase3 == 1 | eu_ets_exp_phase3 == 1) & country == partner
	
	replace eu_ets_exp = 0 if country == partner
	replace eu_ets_exp_phase1 = 0 if country == partner
	replace eu_ets_exp_phase2 = 0 if country == partner
	replace eu_ets_exp_phase3 = 0 if country == partner
	
	********
	*also only change the ETS exporter variable to equal 0 for internal trade
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp eu_ets_exp, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store base3_`v'
	outreg2 using "ppmlhdfe_intra_option1", append excel nocon dec(3) ctitle(`v') label
	}
	
	********
	
	replace eu_ets_imp = 0 if country == partner
	replace eu_ets_imp_phase1 = 0 if country == partner
	replace eu_ets_imp_phase2 = 0 if country == partner
	replace eu_ets_imp_phase3 = 0 if country == partner
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp eu_ets_exp eu_ets_intra, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store base3_`v'
	outreg2 using "ppmlhdfe_pooled_intra", append excel nocon dec(3) ctitle(`v') label
	}
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp_phase* eu_ets_exp_phase* eu_ets_phase1_intra eu_ets_phase2_intra eu_ets_phase3_intra, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store base4_`v'
	outreg2 using "ppmlhdfe_pooled_intra", append excel nocon dec(3) ctitle(`v') label
	}
	
	esttab base3_imports base3_co2_intensity base3_co2_content base4_imports base4_co2_intensity base4_co2_content using ppmlhdfe_pooled2.tex, title(Baseline regressions\label{tab_baseline2}) b(%5.3f) se(%5.3f) replace label
	
	*With globalization dummies
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp eu_ets_exp eu_ets_intra intl_2001_1-intl_2018_14, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store intl3_`v'
	outreg2 using "ppmlhdfe_intl_imp_exp", append excel nocon dec(3) ctitle(`v') label
	}
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp_phase* eu_ets_exp_phase* eu_ets_phase1_intra eu_ets_phase2_intra eu_ets_phase3_intra intl_2001_1-intl_2018_14, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store intl4_`v'
	outreg2 using "ppmlhdfe_intl_imp_exp_phases", append excel nocon dec(3) ctitle(`v') label
	}
	
	*Main analysis: No intra-trade data
	
	cd "$data"

	use "comtrade_isicrev3_2000_2018_intra_trade_final2.dta", clear
	
	drop if country == partner
	
	egen pair_id = group(country partner)
	egen country_time = group(country year)
	egen partner_time = group(partner year)
	egen sector_time = group(isic_rev3 year)
	egen imp_sector_time = group(country isic_rev3 year)
	egen exp_sector_time = group(partner isic_rev3 year)
	egen pair_time = group(country partner year)
	encode isic_rev3, gen(isic_rev3_id)
	
	foreach v of var eu_ets_imp eu_ets_exp {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "24", "26", "27")
	replace `v' = 0 if isic_rev3 == "24" & inlist(year, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012)
	}

	foreach v of var eu_ets_imp_placebo eu_ets_exp_placebo eu_ets_imp_phase1 eu_ets_imp_phase2 eu_ets_exp_phase1 eu_ets_exp_phase2 {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "26", "27")
	}
	
	foreach v of var eu_ets_imp_phase3 eu_ets_exp_phase3 {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "24", "26", "27")
	}
	
	drop year_*
	
	replace co2_intensity = . if co2_intensity == 0
	replace co2_content = . if co2_content == 0
	
	cd "$results"
	
	cap erase "ppmlhdfe_pooled_imp_exp.xml"
	cap erase "ppmlhdfe_pooled_imp_exp.txt"
	
	cap erase "ppmlhdfe_pooled_placebo.xml"
	cap erase "ppmlhdfe_pooled_placebo.txt"
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp eu_ets_exp, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store nointra1_`v'
	outreg2 using "ppmlhdfe_pooled_imp_exp", append excel nocon dec(3) ctitle(`v') label
	}
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp_phase* eu_ets_exp_phase*, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id) d(fe_`v')
	estimates store nointra2_`v'
	outreg2 using "ppmlhdfe_pooled_imp_exp", append excel nocon dec(3) ctitle(`v') label
	}
	
	esttab nointra1_imports nointra1_co2_intensity nointra1_co2_content nointra2_imports nointra2_co2_intensity nointra2_co2_content using ppmlhdfe_nointra.tex, title(Results without intra-trade\label{reg_no_intra}) b(%5.3f) se(%5.3f) replace label

	*Robustness: Placebo included
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp_placebo eu_ets_exp_placebo eu_ets_imp eu_ets_exp, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store placebo_`v'
	outreg2 using "ppmlhdfe_pooled_placebo", append excel nocon dec(3) ctitle(`v') label
	}
	
	*when making placebo for 2003-2004 only:
	replace eu_ets_imp_placebo = 0 if year == 2002
	replace eu_ets_exp_placebo = 0 if year == 2002
	
	rename eu_ets_imp_placebo eu_ets_imp_placebo2
	rename eu_ets_exp_placebo eu_ets_exp_placebo2
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp_placebo2 eu_ets_exp_placebo2 eu_ets_imp eu_ets_exp, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store placebo2_`v'
	outreg2 using "ppmlhdfe_pooled_placebo", append excel nocon dec(3) ctitle(`v') label
	}
	
	esttab placebo_imports placebo_co2_intensity placebo_co2_content placebo2_imports placebo2_co2_intensity placebo2_co2_content using ppmlhdfe_placebo2.tex, title(Placebo regressions\label{tab_placebo}) b(%5.3f) se(%5.3f) replace label