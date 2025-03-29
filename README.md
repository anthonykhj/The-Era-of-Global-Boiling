# The-Era-of-Global-Boiling
An analysis of EV adoption and emissions data to combat 'global boiling'—using MySQL and NoSQL.

## 📌 Overview
This project analyses electric vehicle (EV) adoption, charging infrastructure, and CO₂ emissions to address the UN’s call for urgent climate action. Using **SQL (MySQL)** and **NoSQL (MongoDB)**, we explore:
- Electric vehicle adoption trends
- Charging infrastructure distribution  
- CO2 emissions comparisons  
- Policy implications for Singapore's 2030 EV goals

## 📂 Datasets
| Dataset | Source | Records | Key Fields |
|---------|--------|---------|------------|
| ev_stations_v1 | [Kaggle](https://www.kaggle.com/datasets/prasertk/electric-vehicle-charging-stations-in-usa) | 40,000+ | location, network, confirmation date |
| electric_vehicle_population | [Kaggle](https://www.kaggle.com/datasets/ssarkar445/electric-vehicle-population) | 150,000+ | make, model, state, electric range |
| co2_emissions_canada | [Kaggle](https://www.kaggle.com/datasets/debajyotipodder/co2-emission-by-vehicles) | 7,000+ | vehicle class, fuel consumption, emissions |
| nei_2017_full_data | [Kaggle](https://www.kaggle.com/datasets/xaviernogueira/us-national-emissions-inventory-nei-2017) | 300,000+ | industry sectors, pollutants, locations |

## 🔎 Queries
| No. | Query | Dataset Considered |
|-----|-------|--------------------|
| 1 | How many vehicle classes are in `co2_emissions_canada`? | `co2_emissions_canada` |
| 2 | What is the average engine size, fuel consumption (city/highway), and CO₂ emission for each vehicle class and transmission? | `co2_emissions_canada` |
| 3 | For each zip code and EV network, display stations last confirmed (2010-2022). | `ev_stations_v1` |
| 4 | For each zip code, display stations within latitudes (33.20-34.70) and longitudes (-118.40, -117.20). | `ev_stations_v1` |
| 5 | For each state and model, display Tesla car counts (descending order). | `electric_vehicle_population` |
| 6 | For each EV type and clean alternative fuel eligibility, display average electric range. | `electric_vehicle_population` |
| 7 | For each make/model/state, display alternative fuel vehicle count and CO₂ emissions (g/km).| `co2_emissions_canada` + `electric_vehicle_population` |
| 8i | For each state, display EV count, station count, and vehicle:station ratio (descending).| `ev_stations_v1` + `electric_vehicle_population` |
| 8ii | For each ZIP code, display EV count, station count, and vehicle:station ratio (descending).| `ev_stations_v1` + `electric_vehicle_population` |
| 9 | For each NAICS description containing "auto" or "motor", display total emissions. | `nei_2017_full_data` |
| 10 | For each state, display emissions from key Tesla suppliers (dana, emerson, etc.). | `nei_2017_full_data` |
