import json
from pathlib import Path
import pandas as pd
import itertools

exps_jsons = Path(config['EXPS_JSONS'])

#print("Experiences in config.json:")
#for xp in config['EXPS']:
#    #print(xp)
#    with (exps_jsons/(xp+".json")).open('r') as f:
#        dico = json.load(f)
#        xps[dico['XP_NAME']] = dico
#        xps_names.append(dico['XP_NAME'])

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
    "iotable_XVA_record",
    "final_demand_record",
    "rebuild_prod_record",
    "iotable_X_max_record",
    "overprodvector_record",
    "rebuild_demand_record",
    "limiting_stocks_record",
    "final_demand_unmet_record",
    "iotable_kapital_destroyed_record"
]

run_json_files = [
    "indexes.json",
    "simulated_events.json",
    "simulated_params.json"
]

run_output_files = [
    "io_demand_record",
    "iotable_XVA_record",
    "final_demand_record",
    "rebuild_prod_record",
    "iotable_X_max_record",
    "overprodvector_record",
    "rebuild_demand_record",
    "limiting_stocks_record",
    "final_demand_unmet_record",
    "iotable_kapital_destroyed_record",
    "indexes.json",
    "simulated_events.json",
    "simulated_params.json"
    ]

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
    with open(xp,'r') as f:
        xp_dic = json.load(f)
    output_dir = Path(config["OUTPUT_DIR"]).resolve()
    xp_name = xp_dic["XP_NAME"]
    xp_path = (output_dir/xp_name)
    print(xp_name)
    period_re = re.compile(r"\d{4}-\d{4}")
    match = re.search(period_re, xp_name)
    if not match:
        period = xp_dic.get("PERIOD")
    else:
        period = match.group(0)
    if not xp_path.exists():
        xp_path.mkdir()
    mrios = xp_dic["MRIOS"]
    mrio_basename = xp_dic["MRIO_NAME"]
    if period is None:
        rep_events_file = xp_dic["REP_EVENTS_FILE"]
    else:
        period.replace("-","_")
        rep_events_file = "representative_events_{}_{}.parquet".format(mrio_basename,period)
    rep_events_path = pathlib.Path(config["FLOOD_DATA_DIR"])/"builded-data"/mrio_basename/rep_events_file
    if not Path(xp_path/rep_events_file).exists():
        Path(xp_path/rep_events_file).symlink_to(rep_events_path)
    rep_events = pd.read_parquet((xp_path/rep_events_file).resolve())
    sim_df = pd.DataFrame()
    for mrio in mrios:
        sim_mrio_df = pd.DataFrame()
        mrio_path = output_dir/mrio
        if not mrio_path.exists():
            mrio_path.mkdir()
        with Path(config["CONFIG_DIR"]+"/"+xp_dic['PARAMS_TEMPLATE']).open("r") as f:
            sim_params = json.load(f)

        psis = xp_dic["PSI"]
        if not isinstance(psis,list):
            psis = [psis]
        orders = xp_dic["ORDER_TYPE"]
        if not isinstance(orders,list):
            orders = [orders]
        inv_taus = xp_dic["INV_TAU"]
        if not isinstance(inv_taus,list):
            inv_taus = [inv_taus]
        reb_taus = xp_dic["REB_TAU"]
        if not isinstance(reb_taus,list):
            reb_taus = [reb_taus]
        event_kind = xp_dic["EVENT_KIND"]
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
            sim_params["mrio_template_file"] = mrio_params_from_xp_mrio(xp_dic,mrio)
            sim_params["event_template_file"] = event_params_from_xp_mrio(ev_kind,mrio,)
            with (params_group_path/"simulation_params.json").open("w") as f:
                json.dump(sim_params,f,indent=4)
            mrio_params_file = Path(config["MRIO_DATA_DIR"]+"/"+str(mrio).split("_",1)[0]+"/builded-files/params/"+sim_params["mrio_template_file"])
            event_params_file = Path(config["MRIO_DATA_DIR"]+"/"+str(mrio).split("_",1)[0]+"/builded-files/params/"+sim_params["event_template_file"])
            if not (params_group_path/"mrio_params.json").exists():
                (params_group_path/"mrio_params.json").symlink_to(target=mrio_params_file)
            if not (params_group_path/"event_params.json").exists():
                (params_group_path/"event_params.json").symlink_to(target=event_params_file)

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
            sim_group_df["mrio_template_file"] = mrio_params_from_xp_mrio(xp_dic,mrio)
            sim_group_df["event_template_file"] = event_params_from_xp_mrio(ev_kind,mrio)
            sim_group_df['run'] = sim_group_df.mrio+"/"+sim_group_df.params_group+"/"+sim_group_df.mrio_region+"/"+sim_group_df['ario_dmg_input']+"_"+sim_group_df["duration"].astype(str)+"/"
            sim_mrio_df = pd.concat([sim_mrio_df,sim_group_df],axis=0)

        sim_df = pd.concat([sim_df,sim_mrio_df],axis=0)
    return sim_df
