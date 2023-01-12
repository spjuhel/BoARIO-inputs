import os
import pathlib
import pandas as pd
import logging
import argparse
import json
import pandas as pd

parser = argparse.ArgumentParser(description='Build the mrio parameters json file from a spreadsheet')
parser.add_argument('spreadsheet_path', type=str, help='The str path to the spreadsheet')
parser.add_argument('monetary_unit', type=int, help='The unit prefix factor as an integer (Thousands, millions, etc), e.g. 1000000 for Exiobase3')
parser.add_argument('main_inventory_duration', type=int, help='The principal inventory duration (for runs where inventory duration is changed)')
parser.add_argument('-po', "--mrio_params_output", type=str, help='The path to save the mrio params json file to')
parser.add_argument('-eo', "--events_params_output", type=str, help='The path to save the events params json file to')

args = parser.parse_args()
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
scriptLogger = logging.getLogger("EXIOBASE3_JSON_PARAMS_BUILDER")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)

scriptLogger.addHandler(consoleHandler)
scriptLogger.setLevel(logging.INFO)
scriptLogger.propagate = False

def params_from_ods(ods_file,monetary,main_inv_dur):
    mrio_params = {}
    mrio_params["monetary_unit"] = monetary
    mrio_params["main_inv_dur"] = main_inv_dur
    df = pd.read_excel(ods_file)
    mrio_params["capital_ratio_dict"] = df[["Aggregated version sector", "Capital to VA ratio"]].set_index("Aggregated version sector").to_dict()['Capital to VA ratio']
    mrio_params["inventories_dict"] = df[["Aggregated version sector", "Inventory size (days)"]].set_index("Aggregated version sector").to_dict()['Inventory size (days)']
    return mrio_params

def event_tmpl_from_ods(ods_file):
    event_params = {}
    event_params["aff_regions"] = ["Undefined"]
    event_params["dmg_regional_distrib"] = [1]
    event_params["dmg_sectoral_distrib_type"] = "gdp"
    event_params["duration"] = -1
    event_params["name"] = "Undefined"
    event_params["occur"] = 7
    event_params["kapital_damage"] = -1
    event_params["shock_type"] = "kapital_destroyed_rebuild"
    df = pd.read_excel(ods_file)
    event_params["aff_sectors"] = df.loc[(df.Affected=="Yes"),"Aggregated version sector"].to_list()
    event_params["rebuilding_sectors"] = df.loc[(df["Rebuilding factor"] > 0),["Aggregated version sector", "Rebuilding factor"]].set_index("Aggregated version sector").to_dict()['Rebuilding factor']
    return event_params

if __name__ == '__main__':
    args = parser.parse_args()
    mrio_params = params_from_ods(args.spreadsheet_path, args.monetary_unit, args.main_inventory_duration)
    event_params = event_tmpl_from_ods(args.spreadsheet_path)
    with pathlib.Path(args.mrio_params_output).open("w") as f:
        json.dump(mrio_params, f, indent=4)
    savepath = pathlib.Path(args.events_params_output)
    with savepath.with_stem("{}{}".format(savepath.stem,"_rebuilding")).open("w") as f:
        json.dump(event_params, f, indent=4)

    event_params_recover = event_params.copy()
    event_params_recover["shock_type"] = "kapital_destroyed_recover"
    event_params_recover["recover_function"] = "convexe"
    del event_params_recover["rebuilding_sectors"]
    with savepath.with_stem("{}{}".format(savepath.stem,"_recover")) .open("w") as f:
        json.dump(event_params_recover, f, indent=4)
