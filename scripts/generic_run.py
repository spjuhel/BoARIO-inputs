# BoARIO : The Adaptative Regional Input Output model in python.
# Copyright (C) 2022  Samuel Juhel
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

#import os
import re
#import sys
import subprocess

# should not be required anymore
# module_path = os.path.abspath(os.path.join("../"))
# if module_path not in sys.path:
#     sys.path.append(module_path)

# module_path = os.path.abspath(os.path.join("./"))
# if module_path not in sys.path:
#     sys.path.append(module_path)

import boario
from boario.event import EventKapitalRebuild, EventKapitalRecover
from boario.simulation import Simulation
from boario.model_base import ARIOBaseModel
from boario.extended_models import ARIOPsiModel
import json
import pathlib
import logging
import pickle
import argparse

def dist_is_editable():
   """Is distribution an editable install?"""
   for pth in boario.__path__:
       if "site-packages" in pth:
           return False
   return True

def get_git_describe() -> str:
    return subprocess.check_output(['git', 'describe', '--tags']).decode('ascii').strip()

def run(region, mrio_name, mrio_path, params_group, dmg_as_pct, duration, runs_dir, logger):

    runs_dir = pathlib.Path(runs_dir).resolve()
    if not runs_dir.exists():
        raise ValueError("{} doesn't exist".format(runs_dir))

    mrio_run_dir = (runs_dir/mrio_name)
    mrio_run_dir.mkdir(exist_ok = True)

    # psi_0_90_order_alt_inv_60_reb_60_evtype_recover

    params_re = re.compile(r"psi_(?P<psi>1_0|0_\d+)_order_(?P<order>[a-z]+)_inv_(?P<inv>\d+)_reb_(?P<reb>\d+)_evtype_(?P<evtype>recover|rebuilding)")
    match = re.search(params_re, params_group)
    if not match:
        raise ValueError("There is a problem with the parameter group : {}".format(params_group))

    params_group_dir = (mrio_run_dir/params_group)
    params_group_dir.mkdir(exist_ok = True)

    region_run_dir = (params_group_dir/region)
    region_run_dir.mkdir(exist_ok = True)

    logger.info(params_group_dir)
    logger.info(region_run_dir)

    with (params_group_dir/"simulation_params.json").resolve().open("r") as f:
        logger.info("Loading simulation params template from {}".format((params_group_dir/"simulation_params.json").resolve()))
        params_template = json.load(f)

    with (params_group_dir/"mrio_params.json").resolve().open("r")  as f:
        logger.info("Loading mrio params template from {}".format((params_group_dir/"mrio_params.json").resolve()))
        mrio_template = json.load(f)

    with (params_group_dir/"event_params.json").resolve().open("r")  as f:
        logger.info("Loading event params template from {}".format((params_group_dir/"event_params.json").resolve()))
        event_template = json.load(f)

    params_template["results_storage"] = (str(dmg_as_pct)+"_"+str(duration))
    params_template["output_dir"] = str(region_run_dir)
    params_template["mrio_params_file"] = str((params_group_dir/"mrio_params.json").resolve())

    mrio_path = pathlib.Path(mrio_path)
    with mrio_path.open("rb") as f:
        mrio = pickle.load(f)

    event = event_template.copy()

    if match["evtype"] == "recover":
        logger.info("Setting flood recovery duration to {}".format(float(match["reb"])))
        event["recovery_time"] = float(match["reb"])
    elif match["evtype"] == "rebuilding":
        logger.info("Setting flood rebuilding duration to {}".format(float(match["reb"])))
        event["rebuild_tau"] = float(match["reb"])

    value_added = (mrio.x.T - mrio.Z.sum(axis=0))
    value_added = value_added.reindex(sorted(value_added.index), axis=0) #type: ignore
    value_added = value_added.reindex(sorted(value_added.columns), axis=1)
    value_added[value_added < 0] = 0.0
    gdp_df = value_added.groupby("region",axis=1).sum().T["indout"]
    if mrio.unit.unit.unique()[0] != "M.EUR" :
        scriptLogger.warning("MRIO unit appears to not be 'M.EUR'; but {} instead, which is not yet implemented. Contact the dev !".format(mrio.unit.unit.unique()[0]))
    else:
        gdp_df = gdp_df*(10**6)

    logger.info("Main storage dir is : {}".format(pathlib.Path(params_template["output_dir"]).resolve()))
    dmg_as_pct = float(dmg_as_pct)
    total_direct_dmg = dmg_as_pct * gdp_df[region] #float(event_row['total_dmg'])
    logger.info("Setting flood duration to {}".format(duration))
    event["duration"] = duration
    logger.info("Setting flood damage (as share of GVA) to {}".format(dmg_as_pct))
    event["r_dmg"] = dmg_as_pct
    event["kapital_damage"] = total_direct_dmg
    event["aff_regions"] = [region]
    logger.info("Setting aff_regions to {}".format([region]))
    if params_template['model_type'] == "ARIOPsi":
        model = ARIOPsiModel(mrio,
                             order_type=params_template["order_type"],
                             alpha_base=params_template["alpha_base"],
                             alpha_max=params_template["alpha_max"],
                             alpha_tau=params_template["alpha_tau"],
                             rebuild_tau=event["rebuild_tau"],
                             main_inv_dur = mrio_template["main_inv_dur"],
                             monetary_factor= mrio_template["monetary_unit"],
                             psi_param=params_template["psi_param"],
                             inventory_restoration_tau=params_template["inventory_restoration_tau"],
                             inventory_dict = mrio_template["inventories_dict"],
                             capital_ratio_dict = mrio_template["capital_ratio_dict"],
                             temporal_units_by_step = params_template["temporal_units_by_step"]
        )
    elif params_template['model_type'] == "ARIOBase":
        model = ARIOBaseModel(mrio,
                             order_type=params_template["order_type"],
                             alpha_base=params_template["alpha_base"],
                             alpha_max=params_template["alpha_max"],
                             alpha_tau=params_template["alpha_tau"],
                             rebuild_tau=event["rebuild_tau"],
                             main_inv_dur = mrio_template["main_inv_dur"],
                             monetary_factor= mrio_template["monetary_unit"],
                             inventory_dict = mrio_template["inventories_dict"],
                              capital_ratio_dict = mrio_template["capital_ratio_dict"],
                             temporal_units_by_step = params_template["temporal_units_by_step"]
        )
    else:
        raise ValueError(f"Unexpected model type {params_template['model_type']}")

    sim = Simulation(model,
                     register_stocks = params_template["register_stocks"],
                     n_temporal_units_to_sim= params_template["n_temporal_units_to_sim"],
                     boario_output_dir=params_template["output_dir"],
                     results_dir_name=params_template["results_storage"]
                     )

    scriptLogger.info(f"Building event from:\n {event}")
    if event['shock_type'] == "kapital_destroyed_recover":
        ev = EventKapitalRecover(impact=event["kapital_damage"],
                                 recovery_time=event["recovery_time"],
                                 recovery_function=event["recovery_function"],
                                 aff_regions=event["aff_regions"],
                                 aff_sectors=event["aff_sectors"],
                                 impact_regional_distrib=event["dmg_regional_distrib"],
                                 impact_sectoral_distrib_type=event["dmg_sectoral_distrib_type"],
                                 name = f"{region}_{params_group}_{dmg_as_pct}_{duration}"
                                 )
    elif event['shock_type'] == "kapital_destroyed_rebuild":
        ev = EventKapitalRebuild(impact=event["kapital_damage"],
                                 rebuild_tau=event["rebuild_tau"],
                                 rebuilding_sectors=event["rebuilding_sectors"],
                                 aff_regions=event["aff_regions"],
                                 aff_sectors=event["aff_sectors"],
                                 impact_regional_distrib=event["dmg_regional_distrib"],
                                 impact_sectoral_distrib_type=event["dmg_sectoral_distrib_type"],
                                 duration = duration,
                                 name = f"{region}_{params_group}_{dmg_as_pct}_{duration}"
                                 )
    else:
        raise NotImplementedError(f"event type {event['shock_type']} is not implemented")

    sim.add_event(ev)
    try:
        logger.info("Model ready, looping")
        sim.loop(progress=False)
    except Exception:
        logger.exception("There was a problem:")

parser = argparse.ArgumentParser(description="Produce indicators from one run folder")
parser.add_argument("mrio_name", type=str, help="The name of the MRIO to run the simulation with")
parser.add_argument("region", type=str, help="The region to run")
parser.add_argument("dmg", type=str, help="The damages expressed as a fraction of gdp")
parser.add_argument("duration", type=str, help="The duration of the flood")
parser.add_argument("params_group", type=str, help="The parameters group to simulate with")
parser.add_argument("mrio", type=str, help="The mrio path")
parser.add_argument("out_dir", type=str, help="The general output directory")

if __name__ == "__main__":
    args = parser.parse_args()
    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
    scriptLogger = logging.getLogger("generic_run")
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    scriptLogger.addHandler(consoleHandler)
    scriptLogger.setLevel(logging.INFO)
    scriptLogger.propagate = False
    scriptLogger.info("You are running the following version of BoARIO : {}".format(boario.__version__))
    scriptLogger.info("You are using BoARIO in editable install mode : {}".format(dist_is_editable()))
    scriptLogger.info("BoARIO's location is : {}".format(boario.__path__))
    scriptLogger.info("=============== STARTING RUN ================")
    scriptLogger.info("Runs is on this set: {}_{}_{}_{}_{}".format(args.mrio_name, args.region, args.dmg, args.duration, args.params_group))
    run(args.region, args.mrio_name, args.mrio, args.params_group, args.dmg, int(args.duration), args.out_dir, scriptLogger)
