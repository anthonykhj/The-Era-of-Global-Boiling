-- BC2402 S08 Group 4
-- Group Members: Annabel, Anthony, Xing Ming
-- Group Project: Reworked Data

# Reworked Data for Q1 to Q10
# Dropping Columns in [co2_emissions_canada]
ALTER TABLE `co2_emissions_canada`
DROP COLUMN `Make`, 
DROP COLUMN `Model`,
DROP COLUMN `Cylinders`,
DROP COLUMN `FuelType`,
DROP COLUMN `FuelConsumptionComb_L_100km`,
DROP COLUMN `FuelConsumptionComb_mpg`;

# Dropping Columns in [electric_vehicle_population]
ALTER TABLE `electric_vehicle_population`
DROP COLUMN `VIN`,
DROP COLUMN `County`,
DROP COLUMN `City`,
DROP COLUMN `ModelYear`,
DROP COLUMN `BaseMSRP`,
DROP COLUMN `LegislativeDistrict`;

# Dropping Columns in [ev_stations_v1]
ALTER TABLE `ev_stations_v1`
DROP COLUMN `FuelTypeCode`,
DROP COLUMN `StreetAddress`,
DROP COLUMN `City`,
DROP COLUMN `StationPhone`,
DROP COLUMN `StatusCode`,
DROP COLUMN `AccessDaysTime`,
DROP COLUMN `EVNetworkWeb`,
DROP COLUMN `Latitude`,
DROP COLUMN `Longitude`,
DROP COLUMN `OwnerTypeCode`,
DROP COLUMN `OpenDate`,
DROP COLUMN `EVConnectorTypes`,
DROP COLUMN `Country`,
DROP COLUMN `EVPricing`;

# Dropping Columns in [nei_2017_full_data]
ALTER TABLE `nei_2017_full_data`
DROP COLUMN `epaRegionCode`,
DROP COLUMN `fipsStateCode`,
DROP COLUMN `tribalName`,
DROP COLUMN `fipsCode`,
DROP COLUMN `county`,
DROP COLUMN `eisFacilityId`,
DROP COLUMN `programSystemCode`,
DROP COLUMN `agencyFacilityId`,
DROP COLUMN `triFacilityId`,
DROP COLUMN `siteName`,
DROP COLUMN `facilitySourceType`,
DROP COLUMN `siteLatitude`,
DROP COLUMN `siteLongitude`,
DROP COLUMN `address`,
DROP COLUMN `city`,
DROP COLUMN `zipCode`,
DROP COLUMN `postalAbbreviation`,
DROP COLUMN `reportingPeriod`,
DROP COLUMN `emissionsOperatingType`,
DROP COLUMN `dataSet`;

# Reworked Data for Q14(i)
# [greenhouse_gas_emissions] To remove subtotal rows which will result in double counting
DELETE FROM greenhouse_gas_emissions
WHERE Anzsic_description IN (
	'Total', 
    'Total industry',
    'Primary industries',
    'Service industries',
	'Goods-producing industries'
);

# [greenhouse_gas_emissions] To combine 'Transport' and 'Transport, postal, and warehousing'
UPDATE greenhouse_gas_emissions
SET Anzsic_description = 'Transport, postal, and warehousing'
WHERE Anzsic_description = 'Transport';

# Reworked Data for Q14(ii)
# [electricity] To remove subtotal rows which will result in double counting
DELETE FROM electricity
WHERE Variable IN (
	'Clean', 
    'Gas and Other Fossil', 
    'Hydro, Bioenergy and Other Renewables', 
    'Total Generation', 
    'Wind and Solar'
);

# [electricity] To combine 'Bioenergy', 'Hydro', 'Solar', 'Wind', 'Renewables' and 'Other Renewables'
UPDATE electricity
SET Variable = 'Renewables'
WHERE Variable IN (
	'Bioenergy', 
    'Hydro', 
    'Solar', 
    'Wind',
    'Renewables',
    'Other Renewables'
);

# [electricity] To combine 'Coal', 'Gas', 'Fossil', 'Other Fossil'
UPDATE electricity
SET Variable = 'Fossil Fuel'
WHERE Variable IN (
	'Coal',
    'Gas', 
    'Fossil', 
    'Other Fossil'
);

# [energy_investment] To combine 'Coal' and 'Oil and natural gas'
UPDATE energy_investment
SET SubCategory = 'Fossil Fuel'
WHERE SubCategory IN (
	'Coal',
    'Oil and natural gas'
);
