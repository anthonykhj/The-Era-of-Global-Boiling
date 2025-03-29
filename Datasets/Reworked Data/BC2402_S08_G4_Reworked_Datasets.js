// BC2402 S08 Group 4
// Group Members: Annabel, Anthony, Xing Ming 
// Group Project
// Reworking of Datasets

use s8g4

// Reworked Data for Q1 to Q10
// Reworking of [ev_stations_v1]
// Keeping columns: StationName, State, ZIP, EVNetwork, DateLastConfirmed
db.ev_stations_v1.aggregate([
    {
        $project: {
            StationName: 1, 
            State: 1, 
            ZIP: 1,
            GroupsWithAccessCode: 1,
            EVNetwork: 1, 
            DateLastConfirmed: {$convert: { input: "$DateLastConfirmed", to: "date", onError:"onErrorExpr", onNull:"onNullExpr"}},
            FacilityType: 1
        }
    },
    {
        $out: "ev_stations_reworked"
    }
])

// Reworking of [electric_vehicle_population]
// Keeping columns: State, PostalCode, Make, Model, ElectricVehicleType, CleanAlternativeFuelVehicleEligibility, DOLVehicleID, ElectricRange
db.electric_vehicle_population.aggregate([
    {
        $project: {
            State: 1, 
            PostalCode: 1, 
            Make: 1, 
            Model: 1, 
            ElectricVehicleType: 1, 
            CleanAlternativeFuelVehicleEligibility: 1, 
            DOLVehicleID: {$replaceAll: { input: "$DOLVehicleID", find: "\r", replacement: "" }},
            ElectricRange: {$convert: { input: "$ElectricRange", to: "double", onError:"onErrorExpr", onNull:"onNullExpr"}}
        }
    },
    {
        $out: "ev_population_reworked"
    }
])

// Reworking of [co2_emissions_canada]
// Keeping columns: VehicleClass, Transmission, EngineSize_L, FuelConsumptionCity_L_100km, FuelConsumptionHwy_L_100km, CO2Emissions_g_km
db.co2_emissions_canada.aggregate([
    {
        $project: {
            VehicleClass: 1, 
            Transmission: 1, 
            EngineSize_L: {$convert: { input: "$EngineSize_L", to: "double", onError:"onErrorExpr", onNull:"onNullExpr"}},
            FuelConsumptionCity_L_100km: {$convert: { input: "$FuelConsumptionCity_L_100km", to: "double", onError:"onErrorExpr", onNull:"onNullExpr"}},
            FuelConsumptionHwy_L_100km: {$convert: { input: "$FuelConsumptionHwy_L_100km", to: "double", onError:"onErrorExpr", onNull:"onNullExpr"}},
            CO2Emissions_g_km: {$convert: { input: {$replaceAll: { input: "$CO2Emissions_g_km", find: "\r", replacement: "" }}, to: "double", onError:"onErrorExpr", onNull:"onNullExpr"}}
        }
    }, 
    {
        $out: "co2_emissions_reworked"
    }
])

// Reworking of [nei_2017_full_data]
// Keeping columns: State, companyName, naicsCode, pollutantType(s), totalEmissions, emissionsUom
db.nei_2017_full_data.aggregate([
    {
        $project: {
            State: "$state", 
            CompanyName: "$companyName",
            naicsCode: 1, 
            PollutantType: "$pollutantType(s)",
            TotalEmissions: {$convert: { input: "$totalEmissions", to: "double", onError:"onErrorExpr", onNull:"onNullExpr"}}, 
            EmissionUnits: "$emissionsUom"
        }
    },
    {
        $out: "nei_reworked"
    }
])

// Reworked Data for Q14(i)
// Reworking of [greenhouse_gas_emissions]
// To remove subtotal rows which will result in double counting
db.greenhouse_gas_emissions.deleteMany({
    $or: [
        {Anzsic_description: "Total"},
        {Anzsic_description: "Total industry"},
        {Anzsic_description: "Primary industries"},
        {Anzsic_description: "Service industries"},
        {Anzsic_description: "Goods-producing industries"}
    ]
})

// To combine 'Transport' and 'Transport, postal, and warehousing'
db.greenhouse_gas_emissions.updateMany(
    {Anzsic_description: "Transport"},
    {$set: {Anzsic_description: "Transport, postal, and warehousing"}}
)

// To extract relevant columns 
db.greenhouse_gas_emissions.aggregate([
    {
        $project: {
            Sector: "$Anzsic_description", 
            Year: {$substr: [ "$Period", 0, 4 ]},
            Emissions: "$Data_value",
            Variable: 1, 
            Units: 1, 
            Gas: 1
        }
    },
    {
        $out: "greenhouse_gas_emissions_reworked"
    }
])

// Reworked Data for Q14(ii)
// Reworking of [electricity]
// To remove subtotal rows which will result in double counting
db.electricity.deleteMany({
    $or: [
        {Variable: "Clean"},
        {Variable: "Gas and Other Fossil"},
        {Variable: "Hydro, Bioenergy and Other Renewables"},
        {Variable: "Total Generation"},
        {Variable: "Wind and Solar"}
    ]
})

// To combine 'Bioenergy', 'Hydro', 'Solar', 'Wind', 'Renewables' and 'Other Renewables'
db.electricity.updateMany(
    {
        $or: [
            {Variable: "Bioenergy"},
            {Variable: "Hydro"},
            {Variable: "Solar"},
            {Variable: "Wind"},
            {Variable: "Renewables"},
            {Variable: "Other Renewables"}
        ]
    },
    {
        $set: {
            Variable: "Renewables"
        }
    }
)

// To combine 'Coal', 'Gas', 'Fossil', 'Other Fossil'
db.electricity.updateMany(
    {
        $or: [
            {Variable: "Coal"},
            {Variable: "Gas"},
            {Variable: "Fossil"},
            {Variable: "Other Fossil"}
        ]
    },
    {
        $set: {
            Variable: "Fossil Fuel"
        }
    }
)

// To extract relevant columns 
db.electricity.aggregate([
    {
      $match: {
            Category: "Electricity generation",
             Date: {
                $gte: ISODate("2015-01-01T00:00:00Z"),
                $lt: ISODate("2023-01-01T00:00:00Z")              
          }
      }  
    },
    {
        $project: {
            Area: 1,
            Country_code: "$Country code",
            Date: 1, 
            Variable: 1, 
            Value: 1, 
            Unit: 1
        }
    },
    {
        $out: "electricity_generation_reworked"
    }
])

// Reworking of [energy_investment]
// To combine 'Coal' and 'Oil and natural gas'
db.energy_investment.updateMany(
    {
        $or: [
            {SubCategory: "Coal"},
            {SubCategory: "Oil and natural gas"}
        ]
    },
    {
        $set: {
            SubCategory: "Fossil Fuel"
        }
    }
)
