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
