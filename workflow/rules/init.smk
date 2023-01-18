import json
from pathlib import Path
import pandas as pd
import itertools

include: "./common.smk"

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

def event_params_from_xp_mrio(ev_kind,mrio_used):
    mrio_re = re.compile(r"(?P<mrio>exiobase3|euregio|eora)(?:_(?P<year>\d{4}))?_(?P<sectors>\d+_sectors|full)(?P<custom>.*)")
    match = re.search(mrio_re, mrio_used)
    if match:
        if match.group("custom"):
            raise NotImplementedError("This kind of custom mrio is not yet implemented (or there is a problem in your filename)")
        else:
            tmpl = re.sub(mrio_re,r"\g<mrio>_\g<sectors>",match.string)
    else:
        raise ValueError("There is a problem with the mrio filename : {}".format(mrio_used))
    event_suffix = "_"+ev_kind

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
    rep_events = pd.read_parquet((xp_path/rep_events_file).resolve())
    sim_df = pd.DataFrame()
    for mrio in mrios:
        sim_mrio_df = pd.DataFrame()
        mrio_path = output_dir/mrio
        mrio_path.mkdir(exist_ok=True)
        with Path(config["CONFIG_DIR"]+"/"+xp['PARAMS_TEMPLATE']).open("r") as f:
            sim_params = json.load(f)

        psis = xp["PSI"]
        if not isinstance(psis,list):
            psis = [psis]
        orders = xp["ORDER_TYPE"]
        if not isinstance(orders,list):
            orders = [orders]
        inv_taus = xp["INV_TAU"]
        if not isinstance(inv_taus,list):
            inv_taus = [inv_taus]
        reb_taus = xp["REB_TAU"]
        if not isinstance(reb_taus,list):
            reb_taus = [reb_taus]
        event_kind = xp["EVENT_KIND"]
        if not isinstance(event_kind,list):
            event_kind = [event_kind]
        product = itertools.product(psis,orders,inv_taus,reb_taus,event_kind)
        for psi,order,inv_tau,reb_tau,ev_kind in product:
            params_group_n = f"psi_{psi}_order_{order}_inv_{inv_tau}_reb_{reb_tau}_evtype_{ev_kind}"
            params_group_path = mrio_path/params_group_n
            params_group_path.mkdir(exist_ok=True)
            sim_params["psi_param"] = psi
            sim_params["order_type"] = order
            sim_params["inventory_restoration_tau"] = inv_tau
            sim_params["rebuild_tau"] = reb_tau
            sim_params["mrio_template_file"] = mrio_params_from_xp_mrio(xp,mrio)
            sim_params["event_template_file"] = event_params_from_xp_mrio(ev_kind,mrio,)
            with (params_group_path/"simulation_params.json").open("w") as f:
                json.dump(sim_params,f,indent=4)
            mrio_params_file = Path(config["BUILDED_DATA_DIR"]+"/params/"+sim_params["mrio_template_file"])
            event_params_file = Path(config["BUILDED_DATA_DIR"]+"/params/"+sim_params["event_template_file"])
            if not (params_group_path/"mrio_params.json").exists():
                (params_group_path/"mrio_params.json").symlink_to(mrio_params_file)
            if not (params_group_path/"event_params.json").exists():
                (params_group_path/"event_params.json").symlink_to(event_params_file)

            sim_group_df = rep_events[["mrio_region","share of GVA used as ARIO input","duration","class"]].copy()
            sim_group_df["mrio_region"] = sim_group_df["mrio_region"]
            sim_group_df["ario_dmg_input"] = sim_group_df["share of GVA used as ARIO input"].astype(str)
            sim_group_df["params_group"] = params_group_n
            sim_group_df["event_type"] = ev_kind
            sim_group_df["mrio"] = mrio
            sim_group_df["psi"] = psi
            sim_group_df["order_type"] = order
            sim_group_df["inv_tau"] = inv_tau
            sim_group_df["rebuild_tau"] = reb_tau
            sim_group_df["mrio_template_file"] = mrio_params_from_xp_mrio(xp,mrio)
            sim_group_df["event_template_file"] = event_params_from_xp_mrio(ev_kind,mrio)
            sim_group_df['run'] = sim_group_df.mrio+"/"+sim_group_df.params_group+"/"+sim_group_df.mrio_region+"/"+sim_group_df['ario_dmg_input']+"_"+sim_group_df["duration"].astype(str)+"/"
            sim_mrio_df = pd.concat([sim_mrio_df,sim_group_df],axis=0)

        sim_df = pd.concat([sim_df,sim_mrio_df],axis=0)
    return sim_df

def all_simulations_df(xps):
    meta_df = pd.DataFrame()
    for xp in xps.values():
        xp_df = sim_df_from_xp(xp)
        meta_df = pd.concat([meta_df, xp_df],axis=0)
    return meta_df

def runs_from_all_simulation_parquet(xps):
    path = pathlib.Path(config["BUILDED_DATA_DIR"]+"/"+"all_simulations.parquet").resolve()
    print(path)
    all_simulations_df(xps).to_parquet(path)
    df = pd.read_parquet(path)
    l = list(df["run"])
    return [config["OUTPUT_DIR"] +"/"+ e + "indicators.json" for e in l]

RUNS_PARQUET = [runs_from_parquet(xp) for folder, xp in xps.items()]
RUNS_ALL_SIMS = runs_from_all_simulation_parquet(xps)

include: "./mrio_generation.smk"
