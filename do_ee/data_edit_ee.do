	* Clear memory and set parameters
	clear all
	set more off
	clear matrix
	set matsize 11000
	set maxvar 30000
	
* Set directory path

	global data "write path to data folder here"
		
************** COMTRADE data from 2000 to 2019 ******************
	
	cd "$data"
	
	*****The original data from Comtrade is at HS6 level in multiple large files, so I only add the combined data in ISIC Rev 3 2-digit level here:****
	
	
*Generate export data for intra-trade calculations
	
	import delimited using "comtrade_isicrev3_2000_2018.csv", varnames(1) clear
	keep if tradeflowcode == 2
	drop if partneriso == "WLD"
	collapse(sum) tradevalueus, by(year reporteriso isic_rev3)
	rename tradevalueus exports
	
	save "comtrade_isicrev3_2000_2018_exp.dta", replace
	
	*Exports - re-exports
	use "comtrade_isicrev3_2000_2018.dta", clear
	keep if tradeflowcode == 3
	drop if partneriso == "WLD"
	collapse(sum) tradevalueus, by(year reporteriso isic_rev3)
	rename tradevalueus re_exports
	joinby year reporteriso isic_rev3 using "comtrade_isicrev3_2000_2018_exp.dta", unmatched(using)
	drop _merge
	gen final_exports = exports - re_exports
	replace final_exports = exports if final_exports == . & exports != .
	drop exports re_exports
	rename reporteriso country
	rename final_exports exports
	sort country year isic_rev3
	
	save "comtrade_isicrev3_2000_2018_exp_temp.dta", replace
	
* Edit the country-level production data from UNIDO to get intra-trade
	
	import delimited ctable country year isic isiccomb value utable source unit using "unido_indstat2_data.csv", varnames(nonames) clear
	label variable ctable "Table Code"
	label variable country "Country Code"
	label variable year "Year"
	label variable isic "ISIC Code"
	label variable isiccomb "ISIC Combination Code"
	label variable value "Value"
	label variable utable "Table Definition code"
	label variable source "Source Code"
	label variable unit "Unit"
	keep if ctable == 14

	destring value, gen(production) ignore(".")
	drop value
	joinby country using "iso3.dta"
	
	save "output_unido.dta", replace
	
	use "output_unido.dta", clear
	drop ctable country source
	rename iso3 country
	order year country isic isiccomb production
	replace country = "ROU" if country == "ROM"
	drop if isic == "D"
	keep if year > 1999 & year < 2019
	
	gen zero = 1 if production == 0
	
	replace isic = "15-16" if isic == "15" & isiccomb == "15A"
	drop if isic == "16" & isiccomb == "15A"
	
	replace isic = "15-16" if isic == "16" & isiccomb == "16B"
	drop if isic == "15" & isiccomb == "16B"
	
	replace isic = "15-16" if inlist(isic, "15", "16") & length(isiccomb) < 3
	
	replace isic = "17-19" if isic == "17" & inlist(isiccomb, "17B", "17C", "17D")
	drop if inlist(isic, "18", "19") & inlist(isiccomb, "17B", "17C", "17D")
	
	replace isic = "17-19" if isic == "18" & isiccomb == "18A"
	drop if isic == "17" & isiccomb == "18A"
	
	replace isic = "17-19" if isic == "18" & isiccomb == "18B"
	drop if isic == "19" & isiccomb == "18A"
	
	replace isic = "17-19" if inlist(isic, "17", "18", "19") & length(isiccomb) < 3
	
	replace isic = "21-22" if isic == "21" & isiccomb == "21A"
	drop if isic == "22" & isiccomb == "21A"
	
	replace isic = "21-22" if isic == "22" & isiccomb == "22B"
	drop if isic == "21" & isiccomb == "22B"
	
	replace isic = "21-22" if inlist(isic, "21", "22") & length(isiccomb) < 3
	
	replace isic = "30-33" if isic == "30" & inlist(isiccomb, "30A", "30B", "30C", "30E", "30F", "30G")
	drop if inlist(isic, "31", "32", "33") & inlist(isiccomb, "30A", "30B", "30C", "30E", "30F", "30G")
	
	replace isic = "30-33" if isic == "31" & inlist(isiccomb, "31A", "31B")
	drop if inlist(isic, "32", "33") & inlist(isiccomb, "31A", "31B")
	
	replace isic = "30-33" if inlist(isic, "30", "31", "32", "33") & length(isiccomb) < 3
	
	*Leave only the combination ISICs
	
	bysort year country isic: egen prod = total(production)
	bysort year country isic: gen dup = cond(_N==1,0,_n)
	drop if dup > 1
	drop dup production zero
	replace prod = . if prod == 0
	
	/*
	drop if inlist(isiccomb, "15A", "16B", "17B", "17C", "17D", "21A", "31B") ///
	| inlist(isiccomb, "22B", "30A", "30B", "30C", "30E", "30F", "30G", "31A") ///
	| isiccomb == "32A"
	*/
	
	*replace isiccomb = "99" if length(isic) > 2
	*drop if length(isiccomb) > 2
	drop utable unit
	
	save "output_unido2.dta", replace
	
	keep if length(isic) > 2 | length(isiccomb) == 2

	save "output_unido2_solved.dta", replace
	
*Use export data to generate possible ratios for production in different ISIC categories
	
	use "output_unido2.dta", clear
	
	keep if length(isic) == 2 & length(isiccomb) > 2
	rename isic isic_rev3
	joinby country year isic_rev3 using "comtrade_isicrev3_2000_2018_exp_temp.dta", unmatched(master)
	drop _merge
	drop if prod == . & exports == .
	bysort country year isiccomb: egen total_exports = total(exports)
	bysort country year isiccomb: gen ratio_exports = exports/total_exports
	bysort country year isiccomb: egen max_prod = max(prod)
	replace prod = max_prod*ratio_exports if ratio_exports < 1 & ratio_exports != 0
	drop exports-max_prod
	
	rename isic_rev3 isic
	replace isic = "15-16" if inlist(isic, "15", "16")
	replace isic = "17-19" if inlist(isic, "17", "18", "19")
	replace isic = "21-22" if inlist(isic, "21", "22")
	replace isic = "30-33" if inlist(isic, "30", "31", "32", "33")
	collapse (sum) prod, by(year country isic)
	
	append using "output_unido2_solved.dta"
	drop if prod == .
	
	collapse (sum) prod, by(year country isic)
	
	save "output_unido3.dta", replace
	
*Generate the intra-trade data
	
	use "comtrade_isicrev3_2000_2018_exp_temp.dta", clear
	
	bysort country year: egen isic_15_16 = total(exports) if inlist(isic_rev3, "15", "16") & exports != .
	bysort country year: egen isic_17_19 = total(exports) if inlist(isic_rev3, "17", "18", "19") & exports != .
	bysort country year: egen isic_21_22 = total(exports) if inlist(isic_rev3, "21", "22") & exports != .
	bysort country year: egen isic_30_33 = total(exports) if inlist(isic_rev3, "30", "31", "32", "33") & exports != .

	drop if inlist(isic_rev3, "16", "18", "19")
	drop if inlist(isic_rev3, "22", "31", "32", "33")
	replace exports = isic_15_16 if isic_rev3 == "15"
	replace exports = isic_17_19 if isic_rev3 == "17"
	replace exports = isic_21_22 if isic_rev3 == "21"
	replace exports = isic_30_33 if isic_rev3 == "30"

	replace isic_rev3 = "15-16" if isic_rev3 == "15"
	replace isic_rev3 = "17-19" if isic_rev3 == "17"
	replace isic_rev3 = "21-22" if isic_rev3 == "21"
	replace isic_rev3 = "30-33" if isic_rev3 == "30"
	drop isic_15_16-isic_30_33
	
	*Take the exports out of production
	*collapse (sum) final_exports, by(year exporter isic_rev3)
	rename isic_rev3 isic
	joinby isic country year using "output_unido3.dta"
	drop if isic == "36" | isic == "37"
	
	gen intra_trade = prod - exports
	replace intra_trade = prod if intra_trade == .
	replace intra_trade = 0 if prod == 0
	sort country year isic
	save "intra_trade_2000_2018.dta", replace
	
	use "intra_trade_2000_2018.dta", clear
	drop if intra_trade < 0
	drop exports prod
	rename intra_trade imports
	joinby country using "country_list.dta"
	gen partner = country
	order country partner year
	rename isic isic_rev3
	tab country isic_rev3
	save "intra_trade_2000_2018_final.dta", replace
	
* Back to Comtrade data: Choose the countries that also have CO2 intensity data
	
	use "comtrade_isicrev3_2000_2018.dta", clear
	drop if reporteriso == "WLD"
	drop if partneriso == "WLD"
	*Use exports to get FOB prices
	keep if tradeflowcode == 2
	
	*Keep only countries with carbon intensity data from OECD
	rename reporteriso country
	joinby country using "country_list.dta"
	rename country reporteriso
	rename partneriso country
	joinby country using "country_list.dta"
	rename country partneriso
	
*Edit ISIC rev. 3 variable
	
	rename partneriso country
	rename reporteriso partner
	rename tradevalueus imports
	
	bysort partner country year: egen isic_1_5 = total(imports) if inlist(isic_rev3, "01", "02", "05") & imports != .
	bysort partner country year: egen isic_10_14 = total(imports) if inlist(isic_rev3, "10", "11", "12", "13", "14") & imports != .
	bysort partner country year: egen isic_15_16 = total(imports) if inlist(isic_rev3, "15", "16") & imports != .
	bysort partner country year: egen isic_17_19 = total(imports) if inlist(isic_rev3, "17", "18", "19") & imports != .
	bysort partner country year: egen isic_21_22 = total(imports) if inlist(isic_rev3, "21", "22") & imports != .
	bysort partner country year: egen isic_30_33 = total(imports) if inlist(isic_rev3, "30", "31", "32", "33") & imports != .

	drop if inlist(isic_rev3, "02", "05", "11", "12", "13", "14", "16", "18", "19")
	drop if inlist(isic_rev3, "22", "31", "32", "33")
	replace imports = isic_1_5 if isic_rev3 == "01"
	replace imports = isic_10_14 if isic_rev3 == "10"
	replace imports = isic_15_16 if isic_rev3 == "15"
	replace imports = isic_17_19 if isic_rev3 == "17"
	replace imports = isic_21_22 if isic_rev3 == "21"
	replace imports = isic_30_33 if isic_rev3 == "30"

	replace isic_rev3 = "01-05" if isic_rev3 == "01"
	replace isic_rev3 = "10-14" if isic_rev3 == "10"
	replace isic_rev3 = "15-16" if isic_rev3 == "15"
	replace isic_rev3 = "17-19" if isic_rev3 == "17"
	replace isic_rev3 = "21-22" if isic_rev3 == "21"
	replace isic_rev3 = "30-33" if isic_rev3 == "30"

	drop isic_1_5-isic_30_33
	
	keep if inlist(isic_rev3, "15-16", "17-19", "20", "21-22", "23", "24") | inlist(isic_rev3, "25", "26", "27", "28", "29", "30-33", "34", "35")
	
*Add intra-trade and co2 intensity data to Comtrade data
	
	append using "intra_trade_2000_2018_final.dta"
	joinby country partner year isic_rev3 using "co2_intensity_intra.dta", unmatched(both)
	drop _merge
	drop unit_co2_intensity
	
	rename country partneriso
	rename partner reporteriso
	rename imports tradevalueus
	
	egen panelid = group(reporteriso partneriso isic_rev3)
	xtset panelid year
	tsfill, full
	bysort panelid (reporteriso): replace reporteriso = reporteriso[_N]
	bysort panelid (partneriso): replace partneriso = partneriso[_N]
	bysort panelid (isic_rev3): replace isic_rev3 = isic_rev3[_N]
	drop panelid
	order reporteriso partneriso
	sort reporteriso partneriso isic_rev3 year
	rename reporteriso exporter
	rename partneriso importer
	
	save "comtrade_isicrev3_2000_2018_temp.dta", replace
	
*Add RTA data

	cd "$data"

	use "rta_20200520.dta", clear
	
	label data
	
	*check which agreements are only PSA
	replace psa = 0 if (cu == 1 | fta == 1 | eia == 1 | cueia == 1 | ftaeia == 1 | psaeia == 1)
	drop cu fta eia-psaeia
	keep if year > 1999 & year < 2019
	
	*add the trade data
	joinby importer exporter year using "comtrade_isicrev3_2000_2018_temp.dta", unmatched(using)
	drop _merge tradeflowcode
	
	*generate EU dummy
	gen eu = 0
	
	gen eu_country = 0
	replace eu_country = 1 if inlist(importer, "AUT", "BEL", "BGR", "CYP", "CZE", "DNK", "EST", "FIN", "HRV") | inlist(importer, "FRA", "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX") | inlist(importer, "MLT", "NLD", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE") | importer == "GBR"
	replace eu_country = 0 if (inlist(importer, "CYP", "LVA", "LTU", "MLT", "POL", "SVK", "SVN", "CZE") | inlist(importer, "HUN", "EST")) & year < 2004
	replace eu_country = 0 if inlist(importer, "BGR", "ROU") & year < 2007
	replace eu_country = 0 if importer == "HRV" & year < 2013

	gen eu_partner = 0
	replace eu_partner = 1 if inlist(exporter, "AUT", "BEL", "BGR", "CYP", "CZE", "DNK", "EST", "FIN", "HRV") | inlist(exporter, "FRA", "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX") | inlist(exporter, "MLT", "NLD", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE") | exporter == "GBR"
	replace eu_partner = 0 if (inlist(exporter, "CYP", "LVA", "LTU", "MLT", "POL", "SVK", "SVN", "CZE") | inlist(exporter, "HUN", "EST")) & year < 2004
	replace eu_partner = 0 if inlist(exporter, "BGR", "ROU") & year < 2007
	replace eu_partner = 0 if exporter == "HRV" & year < 2013

	replace eu = 1 if eu_country == 1 & eu_partner == 1
	replace rta = 0 if psa == 1
	replace rta = 0 if eu == 1
	
	* Only keep RTAs that have been made after 2000
	gen RTA_bef_2000 = 0
	bysort importer exporter: replace RTA_bef_2000 = 1 if rta > 0 & year == 2000
	bysort importer exporter: egen sum_RTA_bef_2000 = total(RTA_bef_2000)
	replace rta = 0 if sum_RTA_bef_2000 > 0
	drop RTA_bef_2000 sum_RTA_bef_2000
	
	gen PSA_bef_2000 = 0
	bysort importer exporter: replace PSA_bef_2000 = 1 if psa > 0 & year == 2000
	bysort importer exporter: egen sum_PSA_bef_2000 = total(PSA_bef_2000)
	replace psa = 0 if sum_PSA_bef_2000 > 0
	drop PSA_bef_2000 sum_PSA_bef_2000

	gen EU_bef_2000 = 0
	bysort importer exporter: replace EU_bef_2000 = 1 if eu > 0 & year == 2000
	bysort importer exporter: egen sum_EU_bef_2000 = total(EU_bef_2000)
	replace eu = 0 if sum_EU_bef_2000 > 0
	drop EU_bef_2000 sum_EU_bef_2000
	*Note: EU is similar as ETS variable for countries that joined the EU after 2005 (Bulgaria, Romania & Croatia), but the opposite so that it equals 1 for countries outside the EU. Norway & Iceland are the exception though, since both EU and ETS dummies are 0 after Bulgaria and Croatia join the EU.

*Add the EU ETS dummies

	*Include the countries that joined the ETS in the Phase 3 (note: we don't actually have data for Liechtenstein)
	
	rename importer country
	rename exporter partner
	rename tradevalueus imports
	
	gen ets_non_eu_country = 0
	replace ets_non_eu_country = 1 if inlist(country, "NOR", "ISL", "LIE")
	gen ets_non_eu_partner = 0
	replace ets_non_eu_partner = 1 if inlist(partner, "NOR", "ISL", "LIE")
	
	gen eu_ets_imp = 0

	replace eu_ets_imp = 1 if inlist(year, 2005, 2006, 2007) & eu_country == 1
	replace eu_ets_imp = 1 if inlist(year, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015) & (eu_country == 1 | ets_non_eu_country == 1)
	replace eu_ets_imp = 1 if inlist(year, 2016, 2017, 2018) & (eu_country == 1 | ets_non_eu_country == 1)
	
	gen eu_ets_exp = 0

	replace eu_ets_exp = 1 if inlist(year, 2005, 2006, 2007) & eu_partner == 1
	replace eu_ets_exp = 1 if inlist(year, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015) & (eu_partner == 1 | ets_non_eu_partner == 1)
	replace eu_ets_exp = 1 if inlist(year, 2016, 2017, 2018) & (eu_partner == 1 | ets_non_eu_partner == 1)
	
	gen eu_ets_imp_placebo = 0
	gen eu_ets_exp_placebo = 0

	replace eu_ets_imp_placebo = 1 if inlist(year, 2002, 2003, 2004) & eu_country == 1
	replace eu_ets_exp_placebo = 1 if inlist(year, 2002, 2003, 2004) & eu_partner == 1
	
	gen eu_ets_imp_phase1 = 0
	gen eu_ets_exp_phase1 = 0

	replace eu_ets_imp_phase1 = 1 if inlist(year, 2005, 2006, 2007) & eu_country == 1
	replace eu_ets_exp_phase1 = 1 if inlist(year, 2005, 2006, 2007) & eu_partner == 1

	gen eu_ets_imp_phase2 = 0
	gen eu_ets_exp_phase2 = 0

	replace eu_ets_imp_phase2 = 1 if inlist(year, 2008, 2009, 2010, 2011, 2012) & (eu_country == 1 | ets_non_eu_country == 1) 
	replace eu_ets_exp_phase2 = 1 if inlist(year, 2008, 2009, 2010, 2011, 2012) & (eu_partner == 1 | ets_non_eu_partner == 1)

	gen eu_ets_imp_phase3 = 0
	gen eu_ets_exp_phase3 = 0

	replace eu_ets_imp_phase3 = 1 if inlist(year, 2013, 2014, 2015, 2016, 2017, 2018) & (eu_country == 1 | ets_non_eu_country == 1)
	replace eu_ets_exp_phase3 = 1 if inlist(year, 2013, 2014, 2015, 2016, 2017, 2018) & (eu_partner == 1 | ets_non_eu_partner == 1)
	
	label variable eu_ets_imp "Importer in the EU ETS"
	label variable eu_ets_exp "Exporter in the EU ETS"
	label variable eu_ets_imp_phase1 "Importer in the EU ETS, Phase 1"
	label variable eu_ets_imp_phase2 "Importer in the EU ETS, Phase 2"
	label variable eu_ets_imp_phase3 "Importer in the EU ETS, Phase 3"
	label variable eu_ets_exp_phase1 "Exporter in the EU ETS, Phase 1"
	label variable eu_ets_exp_phase2 "Exporter in the EU ETS, Phase 2"
	label variable eu_ets_exp_phase3 "Exporter in the EU ETS, Phase 3"

	gen eu_ets = eu_ets_imp-eu_ets_exp
	
	gen eu_ets_placebo = eu_ets_imp_placebo-eu_ets_exp_placebo
	gen eu_ets_phase1 = eu_ets_imp_phase1-eu_ets_exp_phase1
	gen eu_ets_phase2 = eu_ets_imp_phase2-eu_ets_exp_phase2
	gen eu_ets_phase3 = eu_ets_imp_phase2-eu_ets_exp_phase3
	
	label variable eu "EU membership"
	label variable rta "RTA"
	label variable eu_ets "EU ETS (2005-2018)"
	label variable eu_ets_placebo "EU ETS Placebo (2002-2004)"
	label variable eu_ets_phase1 "EU ETS Phase 1 (2005-2007)"
	label variable eu_ets_phase2 "EU ETS Phase 2 (2008-2012)"
	label variable eu_ets_phase3 "EU ETS Phase 3 (2013-2018)"
		
	*Other ETS
	
	gen swiss_ets_imp = 0
	replace swiss_ets_imp = 1 if country == "CHE" & year > 2012
	gen swiss_ets_exp = 0
	replace swiss_ets_exp = 1 if country != "CHE" & year > 2012
	gen swiss_ets = swiss_ets_imp - swiss_ets_exp
	
	gen korea_ets_imp = 0
	replace korea_ets_imp = 1 if country == "KOR" & year > 2014
	gen korea_ets_exp = 0
	replace korea_ets_exp = 1 if country != "KOR" & year > 2014
	gen korea_ets = korea_ets_imp - korea_ets_exp
	
	*Total ETS
	
	gen all_ets_imp = eu_ets_imp + swiss_ets_imp + korea_ets_imp
	gen all_ets_exp = eu_ets_exp + swiss_ets_exp + korea_ets_exp
	
	gen all_ets = eu_ets + swiss_ets + korea_ets
	
	replace all_ets = 0 if country == "CHE" & eu_ets == -1
	replace all_ets = 0 if country == "KOR" & eu_ets == -1
	
	replace all_ets = 0 if partner == "CHE" & eu_ets == 1
	replace all_ets = 0 if partner == "KOR" & eu_ets == 1
	
	replace all_ets = 0 if country == "CHE" & partner == "KOR"
	replace all_ets = 0 if country == "KOR" & partner == "CHE"
		
* Globalization dummies
	
	gen intl = 0
	replace intl = 1 if country != partner
	
	forvalues i = 2000/2018 {
	    gen year_`i' = 0
		replace year_`i' = 1 if year == `i'
		gen intl_`i' = intl*year_`i'
		}
		
	*varying by sector
	
	egen sector_num = group(isic_rev3)
	tab sector_num
		
	forvalues y = 1/14 {
		gen sector_`y' = 0
		replace sector_`y' = 1 if sector_num == `y'
		}
		
	forvalues i = 2000/2018 {
	forvalues y = 1/14 {
		gen intl_`i'_`y' = intl*year_`i'*sector_`y'
			}
		}
	
		
*Generate the co2 content variable

	replace imports = imports/1000000
	gen co2_content = imports*co2_intensity_imports
	order country partner year isic_rev3 imports co2_intensity_imports co2_content
	sort country partner year isic_rev3
	
	rename co2_intensity_imports co2_intensity
	
* Drop countries with a lot of missing data

	drop if country == "TWN" | partner == "TWN"
	drop if country == "CRI" | partner == "CRI"
	drop if country == "LAO" | partner == "LAO"
	drop if country == "MMR" | partner == "MMR"
	drop if country == "BRN" | partner == "BRN"
	drop if country == "KHM" | partner == "KHM"
	
* Final dataset

	save "comtrade_isicrev3_2000_2018_intra_trade_final2.dta", replace