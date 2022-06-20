import json
from pathlib import Path

required_scripts = ['aggreg_exio3.py', 'flood_country_aggreg.py']

def check_config(config):
    print("########### CHECKING CONFIG FILE ###############")
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
    config['LONG_TERM_DIR'] = str(Path(config['LONG_TERM_DIR']).resolve())
    print("################# CHECKING DONE ###################")
    print("Here are the configuration you are using :")
    print(json.dumps(config, indent=4))

def run_Full_get_mem_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_timesteps"] // 730
    return 2000*n_2years

def run_Full_get_vmem_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_timesteps"] // 730
    return 3000*n_2years

def run_Full_get_disk_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_timesteps"] // 730
    return 500*n_2years

def indicators_get_mem_mb(wildcards, input):
    run_config = (Path(input[0]).parent)/"simulated_params.json"
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_timesteps"] // 730
    return 12000*n_2years

def indicators_get_vmem_mb(wildcards, input):
    run_config = (Path(input[0]).parent)/"simulated_params.json"
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_timesteps"] // 730
    return 15000*n_2years

def indicators_get_disk_mb(wildcards, input):
    run_config = (Path(input[0]).parent)/"simulated_params.json"
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_timesteps"] // 730
    return 500*n_2years


def runs(xp):
    dmg_type = xp['DMG_TYPE']
    xp_folder = xp['FOLDER']
    inv_params = list(zip(xp['INV_TAU'],xp['INV_TIME']))
    ################################ SUBREGIONS RUNS ########################################
    if xp["MRIOTYPES"] == "Subregions":
        if "SUBREGIONS" not in xp.keys():
            raise ValueError("Run is a subregions run but SUBREGIONS key is not present in the experience dictionnary")
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
                         stype=xp["MRIOTYPES"],
                         flood=xp["FLOOD_INT"],
                         psi=xp["PSI"])
        inv_tmp = expand("_inv_tau_{inv}_inv_time_{inv_t}/indicators.json", zip, inv=xp["INV_TAU"], inv_t=xp["INV_TIME"])
        runs = expand("{part1}{part2}",part1=tmp,part2=inv_tmp)
    return expand("{out}/{runs}", out=config['LONG_TERM_DIR'], runs=runs)
