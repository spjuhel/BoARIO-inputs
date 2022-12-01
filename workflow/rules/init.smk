import json
from pathlib import Path

xps_names = []
xps = {}
exps_jsons = pathlib.Path(config['EXPS_JSONS'])

#print("Experiences in config.json:")
for xp in config['EXPS']:
    #print(xp)
    with (exps_jsons/(xp+".json")).open('r') as f:
        dico = json.load(f)
        xps[dico['XP_NAME']] = dico
        xps_names.append(dico['XP_NAME'])

test = {
    "REGIONS" : "FR",
    "MRIOTYPES" : "Full",
    "FLOOD_DMG" : 200000,
    "FLOOD_INT" : 0,
    "PSI" : "0_99",
    "INV_TAU" : 60,
    "INV_TIME": 90
}

record_files = [
    "io_demand_record",
    "final_demand_record",
    "final_demand_unmet_record",
    "iotable_X_max_record",
    "iotable_XVA_record",
    "limiting_stocks_record",
    "overprodvector_record",
    "rebuild_demand_record",
    "rebuild_prod_record"
]

run_json_files = [
    "indexes.json",
    "simulated_events.json",
    "simulated_params.json"
]

run_output_files = [
    "indexes.json",
    "io_demand_record",
    "final_demand_record",
    "final_demand_unmet_record",
    "iotable_X_max_record",
    "iotable_XVA_record",
    "limiting_stocks_record",
    "overprodvector_record",
    "rebuild_demand_record",
    "rebuild_prod_record",
    "simulated_events.json",
    "simulated_params.json"
    ]

to_move_files = [
    "indexes.json",
    "classic_demand_record",
    "final_demand_unmet_record",
    "iotable_X_max_record",
    "iotable_XVA_record",
    "limiting_stocks_record",
    "overprodvector_record",
    "rebuild_demand_record",
    "rebuild_prod_record",
    "simulated_events.json",
    "simulated_params.json",
    "simulation.log",
    "indicators.json",
    "treated_df_limiting.parquet",
    "treated_df_loss.parquet",
    "prod_df.parquet",
    "io_demand_df.parquet",
    "final_demand_df.parquet",
    "prod_chg.json",
    "fd_loss.json"
]

ALL_YEARS_MRIO = glob_wildcards(config['SOURCE_DATA_DIR']+"/IOT_{year}_ixi.zip").year

ALL_FULL_EXIO = expand("{outputdir}/mrios/exiobase3_{year}_full.pkl",outputdir=config["BUILDED_DATA_DIR"], year=ALL_YEARS_MRIO)

ALL_74_EXIO = expand("{outputdir}/mrios/exiobase3_{year}_74_sectors.pkl",outputdir=config["BUILDED_DATA_DIR"], year=ALL_YEARS_MRIO)

MINIMAL_7_EXIO = expand("{outputdir}/mrios/exiobase3_2020_7_sectors.pkl",outputdir=config["BUILDED_DATA_DIR"])
