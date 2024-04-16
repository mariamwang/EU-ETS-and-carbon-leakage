* Clear memory and set parameters
	clear all
	set more off
	clear matrix
	set matsize 11000
	set maxvar 30000
	
* Set directory path
	global data "write path to data folder here"
	
	
	cd "$data"
	
	foreach num of numlist 1 2 3 {
		import delimited "oecd_co2_intensity_`num'.csv", clear
		save "oecd_co2_intensity_`num'.dta", replace
       }
	   
	use "oecd_co2_intensity_1.dta"
	foreach num of numlist 2 3 {
		append using "oecd_co2_intensity_`num'.dta"
       }
	  
	drop var country partner v10 referenceperiodcode referenceperiod flagcodes flags unitcode powercodecode
	
	*Generate the ISIC rev. 3 categories as well as possible with this data
	gen isic_rev3 = ""
	replace isic_rev3 = "15-16" if ind == "D10T12"
	replace isic_rev3 = "17-19" if ind == "D13T15"
	replace isic_rev3 = "20" if ind == "D16"
	replace isic_rev3 = "21-22" if ind == "D17T18"
	replace isic_rev3 = "23" if ind == "D19"
	replace isic_rev3 = "24" if ind == "D20T21"
	replace isic_rev3 = "25" if ind == "D22"
	replace isic_rev3 = "26" if ind == "D23"
	replace isic_rev3 = "27" if ind == "D24"
	replace isic_rev3 = "28" if ind == "D25"
	replace isic_rev3 = "30-33" if ind == "D26T27"
	replace isic_rev3 = "29" if ind == "D28"
	replace isic_rev3 = "34" if ind == "D29"
	replace isic_rev3 = "35" if ind == "D30"
	drop if isic_rev3 == ""
	
	keep cou par time unit value isic_rev3
	rename cou country
	rename par partner
	rename time year
	rename unit unit_co2_intensity
	rename value co2_intensity_imports
	drop if country == partner
	drop if year < 2000
	
	save "co2_intensity.dta", replace
	
	*Generate co2 intensity of domestic production
	use "co2_intensity.dta", clear
	drop if co2_intensity_imports == 0
	collapse (mean) co2_intensity_imports, by(partner year isic_rev3)
	save "co2_intensity_temp.dta", replace
	gen country = partner
	order country partner
	append using "co2_intensity.dta"
	
	save "co2_intensity_intra.dta", replace
	
	*Generate list of countries that we can use in this analysis
	use "co2_intensity.dta", clear
	keep country
	bysort country: gen dup = cond(_N==1,0,_n)
	drop if dup > 1
	drop dup
	save "country_list.dta", replace
