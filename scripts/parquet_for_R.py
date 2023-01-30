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

import argparse
import logging
from pathlib import Path
import re
import pandas as pd
import numpy as np

POSSIBLE_DATA_TYPE = ["prodloss","finalloss"]
POSSIBLE_PERIOD = ["1970-2015","2016-2035","2036-2050"]
COL_SELECT = ["period","model","mrio_region","MRIO","sector type","semester","Total direct damage to capital (2010€PPP)","Population aff (2015 est.)","direct_prodloss_as_2010gva_share","share of GVA used as ARIO input"]

def build_drias(datafile:str, inputdir:Path, mean:bool=False):
    loss_re = re.compile("|".join(POSSIBLE_DATA_TYPE))
    if loss_type := loss_re.search(datafile):
        loss_type = loss_type.group()
    else:
        raise ValueError("Loss type cannot be deduced from datafile {}".format(datafile))
    df = pd.read_parquet(inputdir/datafile)
    df.reset_index(inplace=True)
    col_regions = df.filter(regex=r"^[A-Z]{2}$").columns
    col_sel = pd.Index(COL_SELECT)
    cols = col_sel.union(col_regions,sort=False)

    if not mean:
        df_carre = df[cols].copy()
        df_carre = df_carre.groupby(["period","MRIO","model","mrio_region","sector type","semester"]).sum()
        nomean = "_nomean"
    else:
        df_carre = df[cols].copy()
        df_carre = df_carre.groupby(["period","MRIO","model","mrio_region","sector type","semester"]).sum().reset_index()
        df_carre.drop("model",axis=1,inplace=True)
        df_carre = df_carre.groupby(["period","MRIO","mrio_region","sector type","semester"]).mean()
        nomean = ""
        col_sel = col_sel.drop('model')

    df_carre.reset_index(inplace=True)
    sector_types = df_carre["sector type"].unique()
    df_carre_essai = df_carre.melt(id_vars=col_sel,value_vars=col_regions,var_name="region_output",value_name=loss_type)
    if not mean:
        df_carre_essai.set_index(["period","model","mrio_region","MRIO","semester","region_output"],inplace=True)
    else:
        df_carre_essai.set_index(["period","mrio_region","MRIO","semester","region_output"],inplace=True)

    df_carre_essai[sector_types] = df_carre_essai.pivot(columns="sector type",values=loss_type)
    df_carre_essai.drop(loss_type,inplace=True,axis=1)
    df_carre_essai.drop("sector type",inplace=True,axis=1)
    df_carre_essai.reset_index(inplace=True)
    df_carre_essai["SELF_damage"] = np.where(df_carre_essai["mrio_region"] == df_carre_essai["region_output"], "SELF", "OTHER")
    df_carre_essai["total"] = df_carre_essai[sector_types].sum(axis=1)
    if not mean:
        df_carre_essai = df_carre_essai.melt(id_vars=['period', 'model', 'mrio_region', 'MRIO', 'semester', 'region_output',
       'Total direct damage to capital (2010€PPP)', 'Population aff (2015 est.)', 'direct_prodloss_as_2010gva_share',
                                                      'share of GVA used as ARIO input', 'SELF_damage'],value_vars=list(sector_types).append("total"), var_name="name")
    else:
        df_carre_essai = df_carre_essai.melt(id_vars=['period', 'mrio_region', 'MRIO', 'semester', 'region_output',
       'Total direct damage to capital (2010€PPP)', 'Population aff (2015 est.)', 'direct_prodloss_as_2010gva_share',
                                                      'share of GVA used as ARIO input', 'SELF_damage'],value_vars=list(sector_types).append("total"), var_name="name")
    df_carre_essai.to_parquet(inputdir/(loss_type+f"_drias_carre_essai{nomean}.parquet"))

def parse_arguments():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-i', '--input-dir', type=str, help="The str path to the aggreg files directory", required=True)
    return parser.parse_args()


if __name__ == '__main__':
    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
    logger = logging.getLogger("Parquets for R")
    logger.setLevel(logging.INFO)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    args = parse_arguments()
    inputdir = Path(args.input_dir)
    files = list(inputdir.glob("*_full_flood_base_results.parquet"))
    logger.info('Found {} files to process'.format(len(files)))
    for f in files:
        f = str(f)
        build_drias(f,inputdir,False)
        build_drias(f,inputdir,True)
