import json
from pathlib import Path
import pandas as pd
import itertools

xps_names = []
xps = {}
exps_jsons = Path(config['EXPS_JSONS'])

#print("Experiences in config.json:")
for xp in config['EXPS']:
    #print(xp)
    with (exps_jsons/(xp+".json")).open('r') as f:
        dico = json.load(f)
        xps[dico['XP_NAME']] = dico
        xps_names.append(dico['XP_NAME'])

test = {
    "REGIONS" : "FR",
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


def event_params_from_xp_mrio(xp,mrio_used):
    mrio_re = re.compile(r"(?P<mrio>exiobase3|euregio|eora)(?:_(?P<year>\d{4}))?_(?P<sectors>\d+_sectors|full)(?P<custom>.*)")
    match = re.search(mrio_re, mrio_used)
    if match:
        if match.group("custom"):
            raise NotImplementedError("This kind of custom mrio is not yet implemented (or there is a problem in your filename)")
        else:
            tmpl = re.sub(mrio_re,r"\g<mrio>_\g<sectors>",match.string)
    else:
        raise ValueError("There is a problem with the mrio filename : {}".format(mrio_used))

    event_suffix = xp.get("event_template_suffix","")
    if event_suffix != "":
        event_suffix = "_"+event_suffix

    return f"{tmpl}_event_params{event_suffix}.json"

def mrio_params_from_xp_mrio(xp,mrio_used):
    mrio_re = re.compile(r"(?P<mrio>exiobase3|euregio|eora)(?:_(?P<year>\d{4}))?_(?P<sectors>\d+_sectors|full)(?P<custom>.*)")
    match = re.search(mrio_re, mrio_used)
    if match:
        if match.group("custom"):
            raise NotImplementedError("This kind of custom mrio is not yet implemented (or there is a problem in your filename)")
        else:
            tmpl = re.sub(mrio_re,r"\g<mrio>_\g<sectors>",match.string)
    else:
        raise ValueError("There is a problem with the mrio filename : {}".format(mrio_used))

    mrio_suffix = xp.get("mrio_template_suffix","")
    if mrio_suffix != "":
        mrio_suffix = "_"+mrio_suffix

    return f"{tmpl}_params{mrio_suffix}.json"


def sim_df_from_xp(xp):
    output_dir = Path(config["OUTPUT_DIR"]).resolve()
    xp_name = xp["XP_NAME"]
    xp_path = (output_dir/xp_name)
    xp_path.mkdir(exist_ok=True)
    mrios = xp["MRIOS"]
    rep_events_file = xp["REP_EVENTS_FILE"]
    if not Path(xp_path/rep_events_file).exists():
        Path(xp_path/rep_events_file).symlink_to(Path(config["SOURCE_DATA_DIR"]+"/"+rep_events_file))
    rep_events = pd.read_parquet(xp_path/rep_events_file)
    sim_df = pd.DataFrame()
    for mrio in mrios:
        sim_mrio_df = pd.DataFrame()
        mrio_path = xp_path/mrio
        mrio_path.mkdir(exist_ok=True)
        with Path(config["CONFIG_DIR"]+"/"+xp['PARAMS_TEMPLATE']).open("r") as f:
            sim_params = json.load(f)

        psis = xp["PSI"]
        orders = xp["ORDER_TYPE"]
        inv_taus = xp["INV_TAU"]
        reb_taus = xp["REB_TAU"]
        product = itertools.product(psis,orders,inv_taus,reb_taus)

        for psi,order,inv_tau,reb_tau in product:
            param_group_n = f"psi~{psi}_order~{order}_inv~{inv_tau}_reb~{reb_tau}"
            param_group_path = mrio_path/param_group_n
            param_group_path.mkdir(exist_ok=True)
            sim_params["psi_param"] = psi
            sim_params["order_type"] = order
            sim_params["inventory_restoration_tau"] = inv_tau
            sim_params["rebuild_tau"] = reb_tau
            sim_params["mrio_template_file"] = mrio_params_from_xp_mrio(xp,mrio)
            sim_params["event_template_file"] = event_params_from_xp_mrio(xp,mrio)
            with param_group_path/"simulation_params.json".open("w") as f:
                json.dump(sim_params,f,indent=4)
            mrio_params_file = Path(config["BUILDED_DATA"]+"/params/"+sim_params["mrio_template_file"])
            event_params_file = Path(config["BUILDED_DATA"]+"/params/"+sim_params["event_template_file"])
            if not (param_group_path/"mrio_params.json").exists():
                param_group_path/"mrio_params.json".symlink_to(mrio_params_file)
            if not (param_group_path/"event_params.json").exists():
                param_group_path/"event_params.json".symlink_to(event_params_file)

            sim_df = rep_events[["EXIO3_region","share of GVA used as ARIO input","class"]].copy()
            sim_df["psi"] = psi
            sim_df["order_type"] = order
            sim_df["inv_tau"] = inv_tau
            sim_df["rebuild_tau"] = reb_tau
            sim_df["mrio_template_file"] = mrio_params_from_xp_mrio(xp,mrio)
            sim_df["event_template_file"] = event_params_from_xp_mrio(xp,mrio)

            sim_mrio_df = pd.concat([sim_mrio_df,sim_df],axis=0)

        sim_mrio_df["mrio"] = mrio
        sim_mrio_df["xp_name"] = xp_name
        sim_mrio_df["rep_events_file"] = rep_events_file
        sim_df = pd.concat([sim_df,sim_mrio_df],axis=0)
    return sim_df

def all_simulations_df(xps):
    meta_df = pd.DataFrame()
    for xp in xps.values():
        xp_df = sim_df_from_xp(xp)
        meta_df = pd.concat([meta_df, xp_df],axis=0)
    return meta_df

all_simulations_df(xps).to_parquet(config["BUILDED_DATA_DIR"]+"/"+"all_simulations.parquet")
