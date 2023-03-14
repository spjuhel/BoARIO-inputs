import json
import pandas
import re
from pathlib import Path

required_scripts = ['aggreg_exio3.py', 'flood_country_aggreg.py']

def runs_from_expdir(wildcards):
    xp = xpjson_from_name(wildcards.expdir)
    with open(xp,'r') as f:
        xp_dic = json.load(f)
    sim_df = sim_df_from_xp(xp)
    l = sim_df[["mrio","params_group","mrio_region","class"]].values
    xp_folder = xp_dic["XP_NAME"]
    out = config["OUTPUT_DIR"]
    runs = [f"{out}/{xp_folder}/{mrio}/{params}/{region}/{ev_class}/{xp_folder}~{mrio}~{params}~{region}~{ev_class}.name" for mrio,params,region,ev_class in l]
    return runs

def xpjson_from_name(expdir):
    """
    Get experience dict from its name/folder
    """
    return config["EXPS_JSONS"]+"/"+expdir+".json"

def xp_from_name(expdir):
    """
    Get experience dict from its name/folder
    """
    with (exps_jsons/(expdir+".json")).open('r') as f:
        xp = json.load(f)
        return xp

def csv_from_all_xp(xps):
    """
    List all csv files corresponding to the dictionary of experiences in argument
    """
    all_csv = []
    for xp in xps:
        with open(xp,"r") as f:
            xp_dic = json.load(f)

        xp_type = xp_dic["DMG_TYPE"]
        tmp = expand("{outputdir}/{expdir}/{files}.csv", outputdir=config["OUTPUT_DIR"], expdir=xp_dic["XP_NAME"], files=["general", "prodloss", "finalloss"])
        all_csv.append(tmp)
    return all_csv

def run_inputs(wildcards):
    """
    Get run general inputs (mrio, params_template, rep_event_flood_file) from experience
    """
    mrio_re = re.compile("exiobase3|euregio|icio")
    mrio_type = mrio_re.search(wildcards.mrio_used)
    if not mrio_type:
        raise ValueError("MRIO {} not recognised",wildcards.mrio_used)
    else:
        mrio_type = mrio_type.group()
    return {
        "mrio" : expand("{inputdir}/{mrio_type}/builded-files/pkls/{{mrio_used}}.pkl",inputdir=config["MRIO_DATA_DIR"],mrio_type=mrio_type)
    }


def input_for_indicators_symlinks(wildcards):
    # xp_folder ; mrio ; region ; params_group ; ev_class
    xp = xpjson_from_name(wildcards.xp_folder)
    sim_df = sim_df_from_xp(xp)
    sim_df = sim_df.drop_duplicates()
    sim_df.set_index(["mrio","params_group","mrio_region","class"],inplace=True)
    mrio = wildcards.mrio_used
    params_group = wildcards.params_group
    region  = wildcards.region
    ev_class = wildcards.ev_class
    run = sim_df.loc[(mrio,params_group,region,ev_class)]
    out=config["OUTPUT_DIR"]
    pct_dur=str(run.ario_dmg_input)+"_"+str(run.duration)
    inds = f"{out}/{{mrio_used}}/{{params_group}}/{{region}}/{pct_dur}/indicators"
    parquets = f"{out}/{{mrio_used}}/{{params_group}}/{{region}}/{pct_dur}/parquets"
    return [inds,parquets]

def get_event_template(mrio_used,shock_type):
    mrio_re = re.compile(r"(?P<mrio>exiobase3|euregio)(?:_(?P<year>\d{4}))?_(?P<sectors>\d+_sectors|full)(?P<custom>.*)")
    match = re.search(mrio_re, mrio_used)
    if match:
        if match.group("custom"):
            raise NotImplementedError("This kind of custom mrio is not yet implemented (or there is a problem in your filename)")
        else:
            event_tmpl = re.sub(mrio_re,r"\g<mrio>_\g<sectors>",match.string)
    else:
        raise ValueError("There is a problem with the mrio filename : {}".format(mrio_used))
    return expand("{maindir}/{mrio}/builded-files/params/{tmpl}_event_params_{kind}.json",maindir=config["MRIO_DATA_DIR"], kind=shock_type, tmpl=event_tmpl, mrio=match["mrio"])

def get_mrio_params(mrio_used,xp_folder):
    mrio_re = re.compile(r"(?P<mrio>exiobase3|euregio)(?:_(?P<year>\d{4}))?_(?P<sectors>\d+_sectors|full)(?P<custom>.*)")
    match = re.search(mrio_re, mrio_used)
    if match:
        if match.group("custom"):
            raise NotImplementedError("This kind of custom mrio is not yet implemented (or there is a problem in your filename)")
        else:
            params_tmpl = re.sub(mrio_re,r"\g<mrio>_\g<sectors>",match.string)
    else:
        raise ValueError("There is a problem with the mrio filename : {}".format(mrio_used))
    return expand("{outputdir}/{mrio}/builded-files/params/{tmpl}_params.json",outputdir=config["MRIO_DATA_DIR"], tmpl=params_tmpl, mrio=match['mrio'])

def find_floodbase(wildcards):
    xp = xp_from_name(wildcards.expdir)
    mrio_basename = xp['MRIO_NAME']
    if xp.get("FLOOD_BASE") is not None:
        return pathlib.Path(config["FLOOD_DATA_DIR"])/"builded-data"/mrio_basename/xp["FLOOD_BASE"]
    period_re = re.compile(r"\d{4}-\d{4}")
    match = re.search(period_re, wildcards.expdir)
    if match:
        period = match.group(0).replace("-","_")
        floodbase_p = pathlib.Path(config["FLOOD_DATA_DIR"])/"builded-data"/mrio_basename/"6_full_floodbase_{}_{}.parquet".format(mrio_basename,period)
    else:
        raise ValueError("No period found in exp name, cannot find corresponding floodbase")

    if floodbase_p.exists():
        return floodbase_p
    else:
        raise FileNotFoundError("Floodbase {} cannot be found".format(floodbase_p))

def find_repevents(wildcards):
    period_re = re.compile(r"\d{4}-\d{4}")
    match = re.search(period_re, wildcards.expdir)
    xp = xp_from_name(wildcards.expdir)
    mrio_basename = xp['MRIO_NAME']
    if xp.get("REP_EVENTS_FILE") is not None:
        return pathlib.Path(config["FLOOD_DATA_DIR"])/"builded-data"/mrio_basename/xp["REP_EVENTS_FILE"]
    if match:
        period = match.group(0).replace("-","_")
        repevents_p = pathlib.Path(config["FLOOD_DATA_DIR"])/"builded-data"/mrio_basename/"representative_events_{}_{}.parquet".format(mrio_basename,period)
    else:
        raise ValueError("No period found in exp name, cannot find corresponding repevents")

    if repevents_p.exists():
        return repevents_p
    else:
        raise FileNotFoundError("Floodbase {} cannot be found".format(repevents_p))

def find_periodname(wildcards):
    period_re = re.compile(r"\d{4}-\d{4}")
    match = re.search(period_re, wildcards.expdir)
    if match:
        period = match.group(0)
        return period
    else:
        xp = xp_from_name(wildcards.expdir)
        if xp.get("PERIOD") is not None:
            match = re.search(period_re, xp["PERIOD"])
            period = match.group(0)
        else:
            raise ValueError("No period found in exp name or xp dict, cannot find corresponding period name")
