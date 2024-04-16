* Clear memory and set parameters
	clear all
	set more off
	clear matrix
	set matsize 11000
	set maxvar 30000
	
* Set directory path
	global data "write data path here"

***** Setup *****

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
	
	foreach v of var eu_ets eu_ets_imp eu_ets_exp all_ets all_ets_imp all_ets_exp swiss_ets korea_ets eu_ets_placebo {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "24", "26", "27")
	replace `v' = 0 if isic_rev3 == "24" & inlist(year, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012)
	}

	foreach v of var eu_ets_placebo eu_ets_phase1 eu_ets_phase2 eu_ets_imp_phase1 eu_ets_imp_phase2 eu_ets_exp_phase1 eu_ets_exp_phase2 {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "26", "27")
	}
	
	foreach v of var eu_ets_phase3 eu_ets_imp_phase3 eu_ets_exp_phase3 {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "24", "26", "27")
	}
	
	drop year_*
	
	replace co2_intensity = . if co2_intensity == 0
	replace co2_content = . if co2_content == 0
	drop if co2_content == .
	
***** Counterfactual estimation ********
	
	egen country_partner_sector = group(country partner isic_rev3)
	
	ppmlhdfe co2_content eu rta eu_ets_imp eu_ets_exp, absorb(country_time partner_time sector_time country_partner_sector, savefe) d vce(cluster country_partner_sector)
	
	predict mu_pred, mu
	
	rename __hdfe1 country_time_fe
	rename __hdfe2 partner_time_fe
	rename __hdfe3 sector_time_fe
	rename __hdfe4 country_partner_sector_fe

	gen mu_cf = exp(_b[_cons] + _b[eu]*eu + _b[rta]*rta + country_time_fe + partner_time_fe + sector_time_fe + country_partner_sector_fe)
	
	egen total_co2_content = total(co2_content)
	egen total_co2_content_pred = total(mu_pred)
	egen total_co2_content_cf = total(mu_cf)
	
	gen co2_content_pred_eu = mu_pred if eu_country == 1 | eu_partner == 1 | ets_non_eu_country == 1 | ets_non_eu_partner == 1
	egen total_co2_content_pred_eu = total(co2_content_pred_eu)
	
	gen diff_co2_content = total_co2_content_pred - total_co2_content_cf
	format diff_co2_content %20.0f
	format total_co2_content %20.0f
	format total_co2_content_pred %20.0f
	format total_co2_content_cf %20.0f
	gen frac_co2_content = diff_co2_content/total_co2_content_pred
	gen frac_co2_content_eu = diff_co2_content/total_co2_content_pred_eu
	
	***********
	
	egen country_partner_sector = group(country partner isic_rev3)
	
	ppmlhdfe imports eu rta eu_ets_imp eu_ets_exp, absorb(country_time partner_time sector_time country_partner_sector, savefe) d vce(cluster country_partner_sector)
	
	predict mu_pred, mu
	
	rename __hdfe1 country_time_fe
	rename __hdfe2 partner_time_fe
	rename __hdfe3 sector_time_fe
	rename __hdfe4 country_partner_sector_fe

	gen mu_cf = exp(_b[_cons] + _b[eu]*eu + _b[rta]*rta + country_time_fe + partner_time_fe + sector_time_fe + country_partner_sector_fe)
	
	egen total_imports = total(imports)
	replace total_imports = total_imports/1000
	egen total_imports_cf = total(mu_cf)
	replace total_imports_cf = total_imports_cf/1000
	egen total_imports_pred = total(mu_pred)
	replace total_imports_pred = total_imports_pred/1000
	
	gen diff_imports = total_imports_pred - total_imports_cf
	format diff_imports %20.0f
	gen frac_imports = diff_imports/total_imports_pred