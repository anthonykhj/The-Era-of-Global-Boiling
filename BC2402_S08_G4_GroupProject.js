// BC2402 S08 Group 4
// Group Members: Annabel, Anthony, Xing Ming 
// Group Project
// Scope Adjustment: Q4, Q7, Q9, Q11, Q12 are omitted

use s8g4

// Q1. How many vehicle classes are in [co2_emissions_canada]?
db.co2_emissions_reworked.distinct("VehicleClass").length

// Q2. [co2_emissions_canada]
//     What is the average engine size, fuel consumption in city and highway, 
//     and CO2 emission for each vehicle class and transmission?
db.co2_emissions_reworked.aggregate([
    {
        $group: {
            _id: {groupByVehicleClass: "$VehicleClass", groupByTransmission: "$Transmission"},
            Average_EngineSize: {$avg: "$EngineSize_L"}, 
            Average_FuelConsumption_City: {$avg: "$FuelConsumptionCity_L_100km"},
            Average_FuelConsumption_Highway: {$avg: "$FuelConsumptionHwy_L_100km"}, 
            Average_CO2Emissions: {$avg: "$CO2Emissions_g_km"}
        }
    },
    {
        $project: {
            Average_EngineSize: {$round: ["$Average_EngineSize", 2]},
            Average_FuelConsumption_City: {$round: ["$Average_FuelConsumption_City", 2]},
            Average_FuelConsumption_Highway: {$round: ["$Average_FuelConsumption_Highway", 2]},
            Average_CO2Emissions: {$round: ["$Average_CO2Emissions", 2]}
        }
    }
])

// Q3. [ev_stations_v1]
//     For each zip code and each EV network, 
//     display the number of stations being last confirmed between 2010 and 2022. 
db.ev_stations_reworked.aggregate([
    {
        $match: {
            DateLastConfirmed: {
                $gte: ISODate("2010-01-01T00:00:00Z"),
                $lt: ISODate("2023-01-01T00:00:00Z")
            }
        }
    },
    {
        $group: {
            _id: {groupByZIP: "$ZIP", groupByEVNetwork: "$EVNetwork"},
            No_of_Stations: {$sum: 1}
        }
    }
])

// Q5. [electric_vehicle_population]
//     For each state and model, display the number of Tesla cars in descending order.
db.ev_population_reworked.aggregate([
    {
        $match: {
            Make: "TESLA"
        }
    },
    {
        $group: {
            _id: {groupByState: "$State", groupByModel: "$Model"},
            No_of_TeslaCars: {$sum: 1}
        }
    },
    {
        $sort: {
            No_of_TeslaCars: -1    
        }
    }
])

// Q6. [electric_vehicle_population]
//     For each electric vehicle type and each clean alternative fuel vehicle eligibility, 
//     display the average electric range value. 
db.ev_population_reworked.aggregate([
    {
        $group: {
            _id: {groupByEVType: "$ElectricVehicleType", groupByCAFVE: "$CleanAlternativeFuelVehicleEligibility"},
            Average_ElectricRange: {$avg: "$ElectricRange"}
        }
    },
    {
        $project: {
            Average_ElectricRange: {$round: ["$Average_ElectricRange", 2]}
        }
    }
])

// Q8(i). [ev_stations_v1] and [electric_vehicle_population]
//        For each state, display the number of electric vehicles, 
//        the number of EV stations, and the vehicle:station ratio in descending order. 
db.ev_population_reworked.aggregate([
    {
        $group: {
            _id: "$State", 
            groupByDOL: {$addToSet: "$DOLVehicleID"}
        }
    },
    {
        $project: {
            No_of_EVs: {$size: "$groupByDOL"}
        }
    }, 
    {
        $lookup: {
               from: "ev_stations_reworked",
               localField: "_id",
               foreignField: "State",
               as: "Station_Info"
             }
    },
    {
        $project: {
            No_of_EVs: 1, 
            No_of_Stations: {$size: "$Station_Info"},
            EV_Stations_Ratio: {
                $cond: {
                    if: {$eq: [{$size: "$Station_Info"}, 0]},
                    then: 0, 
                    else: {$round: [{$divide: ["$No_of_EVs", {$size: "$Station_Info"}]}, 4]}
                }
            }
        }
    },
    {
        $sort: {EV_Stations_Ratio: -1}
    }
])

// Q8(ii). [ev_stations_v1] and [electric_vehicle_population]
//         For each postalcode(zip), display the number of electric vehicles, 
//         the number of EV stations, and the vehicle:station ratio in descending ratio order. 
db.ev_population_reworked.aggregate([
    {
        $group: {
            _id: "$PostalCode",
            groupByDOL: {$addToSet: "$DOLVehicleID"}
        }
    },
    {
        $project: {
            No_of_EVs: {$size: "$groupByDOL"}
        }
    },
    {
        $lookup: {
               from: "ev_stations_reworked",
               localField: "_id",
               foreignField: "ZIP",
               as: "Station_Info"
             }
    },
    {
        $project: {
            No_of_EVs: 1, 
            No_of_Stations: {$size: "$Station_Info"},
            EV_Stations_Ratio: {
                $cond: {
                    if: {$eq: [{$size: "$Station_Info"}, 0]},
                    then: 0, 
                    else: {$round: [{$divide: ["$No_of_EVs", {$size: "$Station_Info"}]}, 4]}
                }
            }
        }
    },
    {
        $sort: {EV_Stations_Ratio: -1}
    }
])

// Q10. [nei_2017_full_data]
//      For each state, display the total emissions for the following suppliers: 
//      dana, emerson, nucor, micron, allegheny, albemarle, schneider, and veatch. 
db.nei_reworked.aggregate([
    {
        $match: {
            CompanyName: {$regex: /(dana|emerson|nucor|micron|allegheny|albemarle|schneider|veatch)/i},
            naicsCode: {$regex: "^33"}
        }
    },
    {
        $project: {
            State: 1, 
            CompanyName: 1, 
            TotalEmissions_Tons: {
                $cond: { 
                    if: {$eq: ["$EmissionUnits", "LB"]}, 
                    then: {$divide: ["$TotalEmissions", 2000]}, 
                    else: "$TotalEmissions"
                }
            }        
        }
    },
    {
        $group: {
            _id: {groupByState: "$State", groupByCompany: "$CompanyName"},
            TotalEmissions_Tons: {$sum: "$TotalEmissions_Tons"}
        }
    },
    {
        $project: {
            TotalEmissions_Tons: {$round: ["$TotalEmissions_Tons", 2]}
        }
    }
])

// Q13. Where should these charging stations be located?
db.ev_stations_reworked.aggregate([
    {
        $match: { FacilityType: { $ne: "" } }
    },
    {
        $group: {
            _id: "$FacilityType",
            Public_Count: { $sum: { $cond: [{ $regexMatch: { input: "$GroupsWithAccessCode", regex: "Public" } }, 1, 0] } },
            Private_Count: { $sum: { $cond: [{ $regexMatch: { input: "$GroupsWithAccessCode", regex: "Private" } }, 1, 0] } },
            Total_Count: { $sum: 1 }
        }
    },
    {
        $project: {
            Public_Count: 1,
            Private_Count: 1,
            Total_Count: 1,
            Public_Percentage: {$round: [{ $multiply: [{ $divide: ["$Public_Count", "$Total_Count"]}, 100]}, 2] },
            Private_Percentage: {$round: [{ $multiply: [{ $divide: ["$Private_Count", "$Total_Count"]}, 100]}, 2] },
        }
    }
]);

// Q14(i). Would a general, robust adoption of EVs be adequate to turn around the climate crisis?
db.greenhouse_gas_emissions_reworked.aggregate([
    {
        $match: {
            Variable: "Actual",
            Year: {$regex: /^2022/}, 
            Gas: "Carbon dioxide equivalents"
        }
    }, 
    {
        $group: {
            _id: "$Sector",
            Total_CO2Emissions: {$sum: "$Emissions"}
        }
    },
    {
        $group: {
            _id: null,
            Total_Emissions_AllSectors: {$sum: "$Total_CO2Emissions"},
            sectors: {$push: {sector: "$_id", Total_CO2Emissions: "$Total_CO2Emissions"}}
        }
    },
    {
        $unwind: "$sectors"
    },
    {
        $project: {
            _id: 0,
            Sector: "$sectors.sector",
            Percentage_of_CO2Emissions: {$round: [{$multiply: [{$divide: ["$sectors.Total_CO2Emissions", "$Total_Emissions_AllSectors"]}, 100]}, 2]}
        }
    },
    {
        $sort: {Percentage_of_CO2Emissions: -1}
    }
])

// Q14(ii). Would electricity generation remain to be largely fossil-based, nonetheless?
db.electricity_generation_reworked.aggregate([
    {
        $match: {
            Unit: "TWh"
        }
    },
    {
        $group: {
            _id: {groupByYear: {$year: "$Date"}, groupByVariable: "$Variable"},
            Electricity_Generated: {$sum: "$Value"}
        }
    },
    {
        $sort: {
            "_id.groupByYear": 1
        }
    }
])

db.energy_investment.aggregate([
    {
        $match: {
            Sector: "Power",
            Category: "Generation"
        }
    },
    {
        $group: {
            _id: {groupByYear: "$Year", groupBySubCategory: "$SubCategory", groupByUnits: "$Units"},
            Amount_of_Investment: {$sum: "$Amount"}
        }
    },
    {
        $project: {
            Amount_of_Investment: {$round: ["$Amount_of_Investment", 2]}
        }
    },
    {
        $sort: {
            "_id.groupByYear": 1
        }
    }
])