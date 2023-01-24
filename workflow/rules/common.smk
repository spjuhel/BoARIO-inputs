import json
import pandas
import re
from pathlib import Path

required_scripts = ['aggreg_exio3.py', 'flood_country_aggreg.py']

def check_config(config):
    #print("########### CHECKING CONFIG FILE ###############")
    ario_dir = Path(config['ARIO_DIR'])
    if not ario_dir.exists() and ario_dir.is_dir():
        raise FileNotFoundError("Given ARIO directory doesn't exist - {}".format(ario_dir))
    else:
        config['ARIO_DIR'] = str(ario_dir.resolve())

    script_dir = Path(config['INPUTS_GENERATION_SCRIPTS_DIR'])
    if not script_dir.exists() and script_dir.is_dir():
        raise FileNotFoundError("Given script directory doesn't exist - {}".format(script_dir))
    else:
        files = [str(x.name) for x in script_dir.glob("*.py") if x.is_file()]
        for script in required_scripts:
            if script not in files:
                raise FileNotFoundError("Script {} is not in the directory {}, yet is required".format(script,script_dir))

        config['INPUTS_GENERATION_SCRIPTS_DIR'] = str(script_dir.resolve())

    source_data_dir = Path(config['SOURCE_DATA_DIR'])
    if not source_data_dir.exists() and source_data_dir.is_dir():
        raise FileNotFoundError("Given source data directory doesn't exist - {}".format(source_data_dir))
    else:
        config['SOURCE_DATA_DIR'] = str(source_data_dir.resolve())

    config_dir = Path(config['CONFIG_DIR'])
    if not config_dir.exists() and config_dir.is_dir():
        raise FileNotFoundError("Given config files directory doesn't exist - {}".format(config_dir))
    else:
        config['CONFIG_DIR'] = str(config_dir.resolve())

    builded_data_dir = Path(config['BUILDED_DATA_DIR'])
    if not builded_data_dir.exists() and builded_data_dir.is_dir():
        raise FileNotFoundError("Given builded data directory doesn't exist - {}".format(builded_data_dir))
    else:
        config['BUILDED_DATA_DIR'] = str(builded_data_dir.resolve())

    config['OUTPUT_DIR'] = str(Path(config['OUTPUT_DIR']).resolve())
    #config['LONG_TERM_DIR'] = str(Path(config['LONG_TERM_DIR']).resolve())
    #print("################# CHECKING DONE ###################")
    #print("Here are the configuration you are using :")
    #print(json.dumps(config, indent=4))

def run_Full_get_mem_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_temporal_units_to_sim"] // 730
    return max(2000*n_2years,4000)

def run_Full_get_vmem_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_temporal_units_to_sim"] // 730
    return max(3000*n_2years,6000)

def run_Full_get_disk_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_temporal_units_to_sim"] // 730
    return 500*n_2years

def indicators_get_mem_mb(wildcards, input):
    #run_config = (Path(input[0]).parent)/"simulated_params.json"
    #with run_config.open("r") as f:
    #    params_template = json.load(f)

    #n_2years = params_template["n_temporal_units_to_sim"] // 730
    return 8000

def indicators_get_vmem_mb(wildcards, input):
    #run_config = (Path(input[0]).parent)/"simulated_params.json"
    #with run_config.open("r") as f:
    #    params_template = json.load(f)

    #n_2years = params_template["n_temporal_units_to_sim"] // 730
    return 7000 #*n_2years

def indicators_get_disk_mb(wildcards, input):
    run_config = (Path(input[0]).parent)/"simulated_params.json"
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_temporal_units_to_sim"] // 730
    return 500*n_2years


def runs(xp):
    dmg_type = xp['DMG_TYPE']
    xp_folder = xp['XP_NAME']
    inv_params = list(zip(xp['INV_TAU'],xp['INV_TIME']))
    ################################ SUBREGIONS RUNS ########################################
    if xp["MRIOTYPES"] == "Subregions":
        if "SUBREGIONS" not in xp.keys():
            raise ValueError("Run is a subregions run but SUBREGIONS key is not present in the experience dictionary")
        if "MAINMRIO" not in xp.keys():
            raise ValueError(
                """Run is a subregions run but MAINMRIO key is not present in the experience dictionary.
                This key define the mrio without subregions to compare to (and also to build the subregions mrios from)
            """)
        if "SUBREGIONS_MRIOS" not in xp.keys():
            raise ValueError(
                """Run is a subregions run but SUBREGION_MRIOS key is not present in the experience dictionary.
                This key define the mrios with subregions to run the simulations with.
                """)
        tmp = [xp['MAINMRIO'] in sub for sub in xp['SUBREGIONS_MRIOS']]
        if False in tmp:
            raise ValueError(
                """Run is a subregions run but SUBREGION_MRIOS key contains values that do not correspond to the MAINMRIO key.
                This key define the mrios with subregions to run the simulations with.
                """)

        unsegmented_runs = expand(xp_folder+"/{mrio_used}/{region}_type_Full_qdmg_"+dmg_type+"_{flood}_Psi_{psi}",
                                          mrio_used=xp['MAINMRIO'],
                                          region=xp["REGIONS"],
                                          flood=xp["FLOOD_INT"],
                                          psi=xp["PSI"])

        segmented_runs_all = []
        segmented_runs_one = []
        if "all" in xp['SUBREGIONS']:
            segmented_runs_all = expand(xp_folder+"/{mrio_used}/{region}-all_type_Subregions_qdmg_"+dmg_type+"_{flood}_Psi_{psi}",
                                        mrio_used=xp['SUBREGIONS_MRIOS'],
                                        region=xp["REGIONS"],
                                        flood=xp["FLOOD_INT"],
                                        psi=xp["PSI"])
        if "one" in xp['SUBREGIONS']:
            segmented_runs_one = expand(xp_folder+"/{mrio_used}/{region}-{region}1_type_Subregions_qdmg_"+dmg_type+"_{flood}_Psi_{psi}",
                                        mrio_used=xp['SUBREGIONS_MRIOS'],
                                        region=xp["REGIONS"],
                                        flood=xp["FLOOD_INT"],
                                        psi=xp["PSI"])

        tmp = unsegmented_runs + segmented_runs_all + segmented_runs_one
        inv_tmp = expand("_inv_tau_{inv}_inv_time_{inv_t}/indicators.json", zip, inv=xp["INV_TAU"], inv_t=xp["INV_TIME"])
        runs = expand("{part1}{part2}",part1=tmp,part2=inv_tmp)
    ################################ `CLASSIC` RUNS ########################################
    else:
        tmp = expand(xp_folder+"/{mrio_used}/{region}_type_{stype}_qdmg_"+dmg_type+"_{flood}_Psi_{psi}",
                         mrio_used=xp['MRIOS'],
                         region=xp["REGIONS"],
                         stype=xp["EVENT_KIND"],
                         flood=xp["FLOOD_INT"],
                         psi=xp["PSI"])
        inv_tmp = expand("_inv_tau_{inv}_inv_time_{inv_t}/indicators.json", zip, inv=xp["INV_TAU"], inv_t=xp["INV_TIME"])
        runs = expand("{part1}{part2}",part1=tmp,part2=inv_tmp)
    return expand("{out}/{runs}", out=config['OUTPUT_DIR'], runs=runs)

def runs_from_expdir(wildcards):
    xp = xpjson_from_name(wildcards.expdir)
    with open(xp,'r') as f:
        xp_dic = json.load(f)
    sim_df = sim_df_from_xp(xp)
    l = sim_df[["mrio","params_group","mrio_region","class"]].values
    runs = [f"{mrio}/{params}/{region}/{ev_class}/indicators" for mrio,params,region,ev_class in l]
    xp_folder = xp_dic["XP_NAME"]
    out = config["OUTPUT_DIR"]
    runs = [f"{out}/{xp_folder}/{run}" for run in runs]
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
        tmp = expand("{outputdir}/{expdir}/{files}.csv", outputdir=config["OUTPUT_DIR"], expdir=xp_dic["XP_NAME"], files=[xp_type+"_general", xp_type+"_prodloss", xp_type+"_fdloss"])
        all_csv.append(tmp)
    return all_csv


def run_RoW_inputs(wildcards):
    xp_config = xps[wildcards.xp_folder]
    return {
        "mrio" : expand("{inputdir}/mrios/{{mrio_used}}_{wildcards.region}.pkl",inputdir=config["BUILDED_DATA_DIR"]),
        "event_template" : expand("{maindir}/../exps/{expfolder}/{{mrio_used}}_event_template.json",maindir=config["CONFIG_DIR"],expfolder=config["FOLDER"]),
        "params_template" : expand("{inputdir}/{params_template}",inputdir=config["CONFIG_DIR"], params_template=xp_config["PARAMS_TEMPLATE"]),
        "mrio_params" : expand("{inputdir}/mrios/{{mrio_used}}_params.json",inputdir=config["BUILDED_DATA_DIR"]),
        "flood_gdp" : expand("{datadir}/{flood_gdp_file}",datadir=config["SOURCE_DATA_DIR"],flood_gdp_file=xp_config["REP_EVENTS_FILE"])
    }


def run_inputs(wildcards):
    """
    Get run general inputs (mrio, params_template, rep_event_flood_file) from experience
    """
    xp_config = xps[wildcards.xp_folder]
    return {
        "mrio" : expand("{inputdir}/mrios/{{mrio_used}}.pkl",inputdir=config["BUILDED_DATA_DIR"]),
        "params_template" : expand("{inputdir}/{params_template}",inputdir=config["CONFIG_DIR"], params_template=xp_config["PARAMS_TEMPLATE"]),
        "flood_gdp" : expand("{datadir}/{flood_gdp_file}",datadir=config["SOURCE_DATA_DIR"],flood_gdp_file=xp_config["REP_EVENTS_FILE"])
    }

def run_inputs2(wildcards):
    """
    Get run general inputs (mrio, params_template, rep_event_flood_file) from experience
    """
    return {
        "mrio" : expand("{inputdir}/mrios/{{mrio_used}}.pkl",inputdir=config["BUILDED_DATA_DIR"])
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
    mrio_re = re.compile(r"(?P<mrio>exiobase3)(?:_(?P<year>\d{4}))?_(?P<sectors>\d+_sectors|full)(?P<custom>.*)")
    match = re.search(mrio_re, mrio_used)
    if match:
        if match.group("custom"):
            raise NotImplementedError("This kind of custom mrio is not yet implemented (or there is a problem in your filename)")
        else:
            event_tmpl = re.sub(mrio_re,r"\g<mrio>_\g<sectors>",match.string)
    else:
        raise ValueError("There is a problem with the mrio filename : {}".format(mrio_used))
    return expand("{maindir}/params/{tmpl}_event_params_{kind}.json",maindir=config["BUILDED_DATA_DIR"], kind=shock_type, tmpl=event_tmpl)

def get_mrio_params(mrio_used,xp_folder):
    mrio_re = re.compile(r"(?P<mrio>exiobase3)(?:_(?P<year>\d{4}))?_(?P<sectors>\d+_sectors|full)(?P<custom>.*)")
    match = re.search(mrio_re, mrio_used)
    if match:
        if match.group("custom"):
            raise NotImplementedError("This kind of custom mrio is not yet implemented (or there is a problem in your filename)")
        else:
            params_tmpl = re.sub(mrio_re,r"\g<mrio>_\g<sectors>",match.string)
    else:
        raise ValueError("There is a problem with the mrio filename : {}".format(mrio_used))
    return expand("{outputdir}/params/{tmpl}_params.json",outputdir=config["BUILDED_DATA_DIR"], tmpl=params_tmpl)
