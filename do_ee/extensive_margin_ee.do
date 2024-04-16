* Clear memory and set parameters
	clear all
	set more off
	set maxvar 30000
	
* Set directory path
	global data "write data path here"
	global results "write results path here"
	
************** BACI data from 2000 to 2018 ******************
	
	cd "$data"
	
*****This takes a long time to run and the original files are really big, so I just leave it here as a reference and give the dataset this creates:****

	/*
	
	import delimited using "cepii_country_codes.csv", clear
	rename iso_3digit_alpha iso3
	rename country_code country
	keep country iso3
	save "iso3_cepii.dta", replace

	foreach num of numlist 2000/2018 {
		import delimited "BACI_HS96/BACI_HS96_Y`num'_V202102.csv", stringcols(4) clear
		rename t year
		rename k hs1996
		rename i exporter
		rename j importer
		rename v trade
		save "BACI_HS96/baci_hs96_`num'.dta", replace
       }
	   
	foreach num of numlist 2000/2018 {
		use "BACI_HS96/baci_hs96_`num'.dta", clear
		joinby hs1996 using "hs1996_isic_rev3.dta"
		bysort year importer exporter isic_rev3: egen total_trade = total(trade)
		bysort year importer exporter isic_rev3: egen count_trade = count(trade)
		bysort year importer exporter isic_rev3: gen dup = cond(_N==1,0,_n)
		drop if dup > 1
		drop dup
		save "BACI_HS96/baci_`num'_isic_rev3.dta", replace
      } 
	
	use "BACI_HS96/baci_2000_isic_rev3.dta", clear
	foreach num of numlist 2001/2018 {
		append using "BACI_HS96/baci_`num'_isic_rev3.dta"
       }
	rename importer country
	joinby country using "iso3_cepii.dta"
	drop country
	rename iso3 importer
	rename exporter country
	joinby country using "iso3_cepii.dta"
	drop country
	rename iso3 exporter
	drop if importer==exporter
	save "baci_isicrev3_2000_2018.dta", replace
	
	*/
	
	use "comtrade_isicrev3_2000_2018_intra_trade_final2.dta", clear
	keep country
	bysort country: gen dup = cond(_N==1,0,_n)
	drop if dup > 1
	drop dup
	save "country_list_cepii.dta", replace
	
	use "baci_isicrev3_2000_2018.dta", clear
	rename importer country
	joinby country using "country_list_cepii.dta"
	rename country importer
	rename exporter country
	joinby country using "country_list_cepii.dta"
	rename country exporter
	
	*Edit ISIC rev. 3 variable
	
	rename importer country
	rename exporter partner
	drop imports
	rename total_trade imports
	
	bysort partner country year: egen isic_1_5 = total(imports) if inlist(isic_rev3, "01", "02", "05")
	bysort partner country year: egen isic_1_5_count = total(count_trade) if inlist(isic_rev3, "01", "02", "05")
	bysort partner country year: egen isic_10_14 = total(imports) if inlist(isic_rev3, "10", "11", "12", "13", "14")
	bysort partner country year: egen isic_10_14_count = total(count_trade) if inlist(isic_rev3, "10", "11", "12", "13", "14")
	bysort partner country year: egen isic_15_16 = total(imports) if inlist(isic_rev3, "15", "16")
	bysort partner country year: egen isic_15_16_count = total(count_trade) if inlist(isic_rev3, "15", "16")
	bysort partner country year: egen isic_17_19 = total(imports) if inlist(isic_rev3, "17", "18", "19")
	bysort partner country year: egen isic_17_19_count = total(count_trade) if inlist(isic_rev3, "17", "18", "19")
	bysort partner country year: egen isic_21_22 = total(imports) if inlist(isic_rev3, "21", "22")
	bysort partner country year: egen isic_21_22_count = total(count_trade) if inlist(isic_rev3, "21", "22")
	bysort partner country year: egen isic_30_33 = total(imports) if inlist(isic_rev3, "30", "31", "32", "33")
	bysort partner country year: egen isic_30_33_count = total(count_trade) if inlist(isic_rev3, "30", "31", "32", "33")

	drop if inlist(isic_rev3, "02", "05", "11", "12", "13", "14", "16", "18", "19")
	drop if inlist(isic_rev3, "22", "31", "32", "33")
	replace imports = isic_1_5 if isic_rev3 == "01"
	replace imports = isic_10_14 if isic_rev3 == "10"
	replace imports = isic_15_16 if isic_rev3 == "15"
	replace imports = isic_17_19 if isic_rev3 == "17"
	replace imports = isic_21_22 if isic_rev3 == "21"
	replace imports = isic_30_33 if isic_rev3 == "30"
	
	replace count_trade = isic_1_5_count if isic_rev3 == "01"
	replace count_trade = isic_10_14_count if isic_rev3 == "10"
	replace count_trade = isic_15_16_count if isic_rev3 == "15"
	replace count_trade = isic_17_19_count if isic_rev3 == "17"
	replace count_trade = isic_21_22_count if isic_rev3 == "21"
	replace count_trade = isic_30_33_count if isic_rev3 == "30"

	replace isic_rev3 = "01-05" if isic_rev3 == "01"
	replace isic_rev3 = "10-14" if isic_rev3 == "10"
	replace isic_rev3 = "15-16" if isic_rev3 == "15"
	replace isic_rev3 = "17-19" if isic_rev3 == "17"
	replace isic_rev3 = "21-22" if isic_rev3 == "21"
	replace isic_rev3 = "30-33" if isic_rev3 == "30"

	drop isic_1_5-isic_30_33_count
	
	keep if inlist(isic_rev3, "15-16", "17-19", "20", "21-22", "23", "24") | inlist(isic_rev3, "25", "26", "27", "28", "29", "30-33", "34", "35")
	
	replace imports = imports*1000
	
	save "baci_isicrev3_2000_2018_temp.dta", replace
	
	************* Add intra-trade ***********

	cd "$data"
	
*Get export data from BACI

	use "baci_isicrev3_2000_2018_temp.dta", clear
	keep country partner year isic_rev3 imports
	collapse (sum) imports, by(year partner isic)
	rename partner country
	rename imports exports
	save "baci_isicrev3_2000_2018_exp.dta", replace
	
*Use export data to generate possible ratios for production in different ISIC categories
	
	use "output_unido2.dta", clear
	
	keep if length(isic) == 2 & length(isiccomb) > 2
	rename isic isic_rev3
	joinby country year isic_rev3 using "baci_isicrev3_2000_2018_exp.dta", unmatched(master)
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
	
	save "output_unido3_cepii.dta", replace
	
*Generate the intra-trade data
	
	use "baci_isicrev3_2000_2018_exp.dta", clear

	*Take the exports out of production
	rename isic_rev3 isic
	joinby isic country year using "output_unido3_cepii.dta"
	drop if isic == "36" | isic == "37"
	
	gen intra_trade = prod - exports
	replace intra_trade = prod if intra_trade == .
	replace intra_trade = 0 if prod == 0
	sort country year isic
	drop if intra_trade < 0
	drop exports prod
	rename intra_trade imports
	joinby country using "country_list.dta"
	gen partner = country
	order country partner year
	rename isic isic_rev3
	tab country isic_rev3
	save "baci_intra_trade_2000_2018.dta", replace
	
*Add intra-trade and co2 intensity data

	use "baci_isicrev3_2000_2018_temp.dta", clear
	append using "baci_intra_trade_2000_2018.dta"
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
	
	save "baci_isicrev3_2000_2018_temp.dta", replace
	
	use "rta_20200520.dta", clear
	
	*check which agreements are only PSA
	replace psa = 0 if (cu == 1 | fta == 1 | eia == 1 | cueia == 1 | ftaeia == 1 | psaeia == 1)
	drop cu fta eia-psaeia
	keep if year > 1999 & year < 2019
	
	*add the trade data
	joinby importer exporter year using "baci_isicrev3_2000_2018_temp.dta", unmatched(using)
	drop _merge
	
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

	*Sector dummies
	gen isic_d_01_05 = 0
	gen isic_d_10_14 = 0
	gen isic_d_15_16 = 0
	gen isic_d_17_19 = 0
	gen isic_d_20 = 0
	gen isic_d_21_22 = 0
	gen isic_d_23 = 0
	gen isic_d_24 = 0
	gen isic_d_25 = 0
	gen isic_d_26 = 0
	gen isic_d_27 = 0
	gen isic_d_28 = 0
	gen isic_d_29 = 0
	gen isic_d_30_33 = 0
	gen isic_d_34 = 0
	gen isic_d_35 = 0
	
	replace isic_d_01_05 = 1 if isic_rev3 == "01-05"
	replace isic_d_10_14 = 1 if isic_rev3 == "10-14"
	replace isic_d_15_16 = 1 if isic_rev3 == "15-16"
	replace isic_d_17_19 = 1 if isic_rev3 == "17-19"
	replace isic_d_20 = 1 if isic_rev3 == "20"
	replace isic_d_21_22 = 1 if isic_rev3 == "21-22"
	replace isic_d_23 = 1 if isic_rev3 == "23"
	replace isic_d_24 = 1 if isic_rev3 == "24"
	replace isic_d_25 = 1 if isic_rev3 == "25"
	replace isic_d_26 = 1 if isic_rev3 == "26"
	replace isic_d_27 = 1 if isic_rev3 == "27"
	replace isic_d_28 = 1 if isic_rev3 == "28"
	replace isic_d_29 = 1 if isic_rev3 == "29"
	replace isic_d_30_33 = 1 if isic_rev3 == "30-33"
	replace isic_d_34 = 1 if isic_rev3 == "34"
	replace isic_d_35 = 1 if isic_rev3 == "35"	
	
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
	
	gen eu_ets_phase1 = eu_ets_imp_phase1-eu_ets_exp_phase1
	gen eu_ets_phase2 = eu_ets_imp_phase2-eu_ets_exp_phase2
	gen eu_ets_phase3 = eu_ets_imp_phase2-eu_ets_exp_phase3
	
	label variable eu "EU membership"
	label variable rta "RTA"
	label variable eu_ets "EU ETS (2008-2015)"
	label variable eu_ets_phase1 "EU ETS Phase 1 (2005-2007)"
	label variable eu_ets_phase2 "EU ETS Phase 2 (2008-2012)"
	label variable eu_ets_phase3 "EU ETS Phase 3 (2013-2015)"

	foreach v of varlist isic_d_01_05-isic_d_35 {
		gen `v'_ets = `v'*eu_ets
		}
		
	label variable isic_d_01_05_ets "01-05: Agriculture, forestry and fishing"
	label variable isic_d_10_14_ets "10-14: Mining & quarrying"
	label variable isic_d_15_16_ets "15-16: Food products, beverages and tobacco"
	label variable isic_d_17_19_ets "17-19: Textiles, wearing apparel, leather and related products"
	label variable isic_d_20_ets "20: Wood and products of wood and cork"
	label variable isic_d_21_22_ets "21-22: Paper products and printing"
	label variable isic_d_23_ets "23: Coke and refined petroleum products (and nuclear fuel)"
	label variable isic_d_24_ets "24: Chemicals and chemical (/pharmaceutical) products"
	label variable isic_d_25_ets "25: Rubber and plastic products"
	label variable isic_d_26_ets "26: Other non-metallic mineral products"
	label variable isic_d_27_ets "27: Basic metals"
	label variable isic_d_28_ets "28: Fabricated metal products, except machinery and equipment"
	label variable isic_d_29_ets "29: Machines and equipment n.e.c."
	label variable isic_d_30_33_ets "30-33: Computers, electronic and electrical equipment"
	label variable isic_d_34_ets "34: Motor vehicles, trailers and semi-trailers"
	label variable isic_d_35_ets "35: Other transport equipment"

	foreach v of varlist isic_d_01_05-isic_d_35 {
		gen `v'_phase1 = `v'*eu_ets_phase1
		gen `v'_phase2 = `v'*eu_ets_phase2
		gen `v'_phase3 = `v'*eu_ets_phase3
		}

	forvalues v = 1/3 {
		label variable isic_d_01_05_phase`v' "Phase `v' 01-05: Agriculture, forestry and fishing"
		label variable isic_d_10_14_phase`v' "Phase `v' 10-14: Mining & quarrying"
		label variable isic_d_15_16_phase`v' "Phase `v' 15-16: Food products, beverages and tobacco"
		label variable isic_d_17_19_phase`v' "Phase `v' 17-19: Textiles, wearing apparel, leather and related products"
		label variable isic_d_20_phase`v' "Phase `v' 20: Wood and products of wood and cork"
		label variable isic_d_21_22_phase`v' "Phase `v' 21-22: Paper products and printing"
		label variable isic_d_23_phase`v' "Phase `v' 23: Coke and refined petroleum products (and nuclear fuel)"
		label variable isic_d_24_phase`v' "Phase `v' 24: Chemicals and chemical (/pharmaceutical) products"
		label variable isic_d_25_phase`v' "Phase `v' 25: Rubber and plastic products"
		label variable isic_d_26_phase`v' "Phase `v' 26: Other non-metallic mineral products"
		label variable isic_d_27_phase`v' "Phase `v' 27: Basic metals"
		label variable isic_d_28_phase`v' "Phase `v' 28: Fabricated metal products, except machinery and equipment"
		label variable isic_d_29_phase`v' "Phase `v' 29: Machines and equipment n.e.c."
		label variable isic_d_30_33_phase`v' "Phase `v' 30-33: Computers, electronic and electrical equipment"
		label variable isic_d_34_phase`v' "Phase `v' 34: Motor vehicles, trailers and semi-trailers"
		label variable isic_d_35_phase`v' "Phase `v' 35: Other transport equipment"
		}
		
	*Globalization dummies
	gen intl = 0
	replace intl = 1 if country != partner
	
	forvalues i = 2000/2018 {
	    gen year_`i' = 0
		replace year_`i' = 1 if year == `i'
		gen intl_`i' = intl*year_`i'
		}
		
	*Generate the co2 content variable
	replace imports = imports/1000000
	gen co2_content = imports*co2_intensity_imports
	order country partner year isic_rev3 imports co2_intensity_imports co2_content
	sort country partner year isic_rev3
	
	rename co2_intensity_imports co2_intensity
	
	drop if country == "TWN" | partner == "TWN"
	drop if country == "CRI" | partner == "CRI"
	drop if country == "LAO" | partner == "LAO"
	drop if country == "MMR" | partner == "MMR"
	drop if country == "BRN" | partner == "BRN"
	drop if country == "KHM" | partner == "KHM"
	
	save "baci_isicrev3_2000_2018_intra_trade_final.dta", replace
	
	cd "$data"
	
	use "baci_isicrev3_2000_2018_intra_trade_final.dta", clear
	
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

	foreach v of var eu_ets_imp_phase1 eu_ets_imp_phase2 eu_ets_exp_phase1 eu_ets_exp_phase2 {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "26", "27")
	}
	
	foreach v of var eu_ets_imp_phase3 eu_ets_exp_phase3 {
	replace `v' = 0 if !inlist(isic_rev3, "21-22", "23", "24", "26", "27")
	}
	
	drop year_*
	
	cd "$results"
	
	cap erase "ppmlhdfe_baci.xml"
	cap erase "ppmlhdfe_baci.txt"
	
	cap erase "ppmlhdfe_baci2.xml"
	cap erase "ppmlhdfe_baci2.txt"
	
	*BASELINE: Importer and exporter ETS separate
	*Test how using different data impacts results
	
	*When testing how including zeros impacts results:
	*replace imports = 0 if imports == .
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp eu_ets_exp, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store baci1_`v'
	outreg2 using "ppmlhdfe_baci", append excel nocon dec(3) ctitle(`v') label
	}
	
	foreach v of var imports co2_intensity co2_content {
	ppmlhdfe `v' eu rta eu_ets_imp_phase* eu_ets_exp_phase*, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store baci2_`v'
	outreg2 using "ppmlhdfe_baci", append excel nocon dec(3) ctitle(`v') label
	}
	
	esttab baci1_imports baci1_co2_intensity baci2_imports baci2_co2_intensity using ppmlhdfe_baci.tex, title(New imports\label{tab_bacii_data}) b(%5.3f) se(%5.3f) replace label
	
	*Extensive margin
	
	drop if country == partner
	replace count_trade = 0 if count_trade == .
	replace imports = 0 if imports == .
	
	foreach v of var imports count_trade {
	ppmlhdfe `v' eu rta eu_ets_imp eu_ets_exp, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store baci3_`v'
	outreg2 using "ppmlhdfe_baci2", append excel nocon dec(3) ctitle(`v') label
	}
	
	foreach v of var imports count_trade {
	ppmlhdfe `v' eu rta eu_ets_imp_phase* eu_ets_exp_phase*, absorb(country_time partner_time sector_time pair_id#isic_rev3_id) vce(cluster pair_id#isic_rev3_id)
	estimates store baci4_`v'
	outreg2 using "ppmlhdfe_baci2", append excel nocon dec(3) ctitle(`v') label
	}
	
	esttab baci3_imports baci3_count_trade baci4_imports baci4_count_trade using ppmlhdfe_baci2.tex, title(New imports\label{tab_extensive_margin}) b(%5.3f) se(%5.3f) replace label