-- BC2402 S08 Group 4
-- Group Members: Annabel, Anthony, Xing Ming
-- Group Project 
-- Scope Adjustment: Q4, Q7, Q9, Q11, Q12 are omitted

USE bc2402_gp;

# Q1. How many vehicle classes are in [co2_emissions_canada]?
SELECT COUNT(DISTINCT VehicleClass) AS Number_of_VehicleClasses
FROM co2_emissions_canada;

# Q2. [c02_emissions_canada]
# 	  What is the average engine size, fuel consumption in city and highway, 
#	  and CO2 emission for each vehicle class and transmission?
SELECT 
	VehicleClass, 
    Transmission, 
    ROUND(AVG(EngineSize_L), 2) AS Average_EngineSize, 
    ROUND(AVG(FuelConsumptionCity_L_100km), 2) AS Average_FuelConsumption_City, 
    ROUND(AVG(FuelConsumptionHwy_L_100km), 2) AS Average_FuelConsumption_Highway, 
    ROUND(AVG(CO2Emissions_g_km), 2) AS Average_CO2Emissions
FROM co2_emissions_canada
GROUP BY VehicleClass, Transmission;

# Q3. [ev_stations_v1] 
#	  For each zip code and each EV network, 
#	  display the number of stations being last confirmed between 2010 and 2022. 
SELECT
	ZIP, 
    EVNetwork, 
    COUNT(DISTINCT ID) AS No_of_Stations
FROM ev_stations_v1
WHERE YEAR(STR_TO_DATE(DateLastConfirmed, '%m/%d/%Y')) BETWEEN 2010 AND 2022
GROUP BY ZIP, EVNetwork;

# Q5. [electric_vehicle_population]
#	  For each state and model, display the number of Tesla cars in descending order. 
SELECT 
	State, 
    Model, 
    COUNT(DISTINCT DOLVehicleID) AS No_of_TeslaCars
FROM electric_vehicle_population 
WHERE Make = 'TESLA'
GROUP BY State, Model
ORDER BY No_of_TeslaCars DESC;

# Q6. [electric_vehicle_population]
#	  For each electric vehicle type and each clean alternative fuel vehicle eligibility, 
#	  display the average electric range value. 
SELECT 
	ElectricVehicleType, 
    CleanAlternativeFuelVehicleEligibility, 
    ROUND(AVG(ElectricRange), 2) AS Average_ElectricRange
FROM electric_vehicle_population
GROUP BY ElectricVehicleType, CleanAlternativeFuelVehicleEligibility;

# Q8(i). [ev_stations_v1] and [electric_vehicle_population]
#		 For each state, display the number of electric vehicles, 
#		 the number of EV stations, and the vehicle:station ratio in descending ratio order. 
CREATE TEMPORARY TABLE ev_count_state AS
SELECT 
	State, 
    COUNT(DISTINCT DOLVehicleID) AS ev_count
FROM electric_vehicle_population
GROUP BY State;

CREATE TEMPORARY TABLE stn_count_state AS
SELECT 
	State, 
    COUNT(*) AS stn_count # DISTINCT ID?
FROM ev_stations_v1
GROUP BY State;

SELECT 
	ev.State, 
    ev.ev_count, 
    s.stn_count, 
    ev.ev_count / s.stn_count AS EV_Station_Ratio 
FROM ev_count_state ev
JOIN stn_count_state s ON ev.State = s.State
ORDER BY EV_Station_Ratio DESC;

# Q8(ii). [ev_stations_v1] and [electric_vehicle_population]
#		  For each postalcode(zip), display the number of electric vehicles, 
#		  the number of EV stations, and the vehicle:station ratio in descending ratio order. 
CREATE TEMPORARY TABLE ev_count_zip AS
SELECT 
	PostalCode, 
    COUNT(DISTINCT DOLVehicleID) AS ev_count
FROM electric_vehicle_population
GROUP BY PostalCode;

CREATE TEMPORARY TABLE stn_count_zip AS
SELECT 
	ZIP, 
    COUNT(*) AS stn_count # DISTINCT ID?
FROM ev_stations_v1
GROUP BY ZIP;

SELECT 
	ev.PostalCode, 
    ev.ev_count, s.stn_count, 
    ev.ev_count / s.stn_count AS EV_Station_Ratio 
FROM ev_count_zip ev
JOIN stn_count_zip s ON ev.PostalCode = s.ZIP
ORDER BY EV_Station_Ratio DESC;

# Q10. [nei_2017_full_data]
#	   For each state, display the total emissions for the following suppliers: 
#	   dana, emerson, nucor, micron, allegheny, albemarle, schneider, and veatch. 
SELECT 
	State, 
    CompanyName,
    ROUND(SUM(CASE 
			WHEN emissionsUom = "LB" THEN totalEmissions / 2000
			ELSE totalEmissions
		END), 2) AS Total_Emissions_Tons
FROM nei_2017_full_data
WHERE 
	naicsCode LIKE '33%'
    AND (
		companyName LIKE '%dana%'
        OR companyName LIKE '%emerson%'
        OR companyName LIKE '%nucor%'
        OR companyName LIKE '%micron%'
        OR companyName LIKE '%allegheny%'
        OR companyName LIKE '%albemarle%'
        OR companyName LIKE '%schneider%'
        OR companyName LIKE '%veatch%'
	)
GROUP BY State, CompanyName;

# Q13. Where should these charging stations be located?
SELECT 
    FacilityType,
    SUM(CASE WHEN GroupsWithAccessCode LIKE '%Public%' THEN 1 ELSE 0 END) AS Public_Count,
    SUM(CASE WHEN GroupsWithAccessCode LIKE '%Private%' THEN 1 ELSE 0 END) AS Private_Count,
    COUNT(*) AS Total_Count,
    ROUND((SUM(CASE WHEN GroupsWithAccessCode LIKE '%Public%' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS Public_Percentage,
    ROUND((SUM(CASE WHEN GroupsWithAccessCode LIKE '%Private%' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS Private_Percentage
FROM 
    ev_stations_v1
WHERE
	FacilityType <> ''
GROUP BY 
    FacilityType;

# Q14(i). Would a general, robust adoption of EVs be adequate to turn around the climate crisis?
SELECT 
    LEFT(Period, 4) AS Year, 
	Anzsic_description AS Sector, 
    ROUND((SUM(Data_value) / (
		SELECT 
			SUM(Data_Value) AS Total_CO2Emissions
		FROM greenhouse_gas_emissions
		WHERE 
			Variable = 'Actual'
			AND LEFT(Period, 4) = 2022
			AND Gas = 'Carbon dioxide equivalents'
		GROUP BY 
			LEFT(Period, 4))) * 100, 2)
	AS Percentage_of_CO2Emissions
FROM greenhouse_gas_emissions
WHERE 
	Variable = 'Actual'  
    AND LEFT(Period, 4) = 2022  
    AND Gas = 'Carbon dioxide equivalents'
GROUP BY 
	LEFT(Period, 4), 
    Anzsic_Description
ORDER BY
	Percentage_of_CO2Emissions DESC;
    
# Q14(ii). Would electricity generation remain to be largely fossil-based, nonetheless?
SELECT 
	YEAR(STR_TO_DATE(Date, '%d/%m/%Y')) AS Year, 
    Variable, 
    ROUND(SUM(Value), 0) AS Electricity_Generated,
    Unit
FROM electricity
WHERE 
	YEAR(STR_TO_DATE(Date, '%d/%m/%Y')) BETWEEN 2015 AND 2022
	AND Category = 'Electricity generation'
    AND Unit = 'TWh'
GROUP BY 
	YEAR(STR_TO_DATE(Date, '%d/%m/%Y')),
    Variable, 
    Unit
ORDER BY 
	YEAR(STR_TO_DATE(Date, '%d/%m/%Y'));

SELECT 
	Year, 
	SubCategory,
    ROUND(SUM(Amount), 1) AS Amount_of_Investment,
    Units
FROM energy_investment
WHERE
	Sector = 'Power'
    AND Category = 'Generation'
GROUP BY 
	Year, 
    SubCategory, 
    Units
ORDER BY 
	Year;