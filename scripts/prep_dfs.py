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

from typing import Optional
import pandas as pd
import pathlib
import argparse
import logging
import pathlib
import numpy as np
import pandas as pd
from tqdm.notebook import tqdm
import pathlib
import warnings
from scipy.interpolate import interp1d
import numpy as np
import pandas as pd
from tqdm.notebook import tqdm
import pathlib
import logging
import argparse
tqdm.pandas()

def prepare_general_df(general_csv:pathlib.Path, period:str, representative_path) -> pd.DataFrame:
    df = pd.read_csv(general_csv)
    df['period'] = period
    df.drop(["region", "rebuild_durations", "inv_tau", "shortage_ind_mean"],axis=1, inplace=True)
    df.drop(["top_5_sector_chg", "10_first_shortages_(step,region,sector,stock_of)"],axis=1, inplace=True)
    df["prod_lost_aff"] = df["prod_lost_tot"] - df["prod_lost_unaff"]
    df["unaff_fd_unmet"] = df["tot_fd_unmet"] - df["aff_fd_unmet"]
    repevents_df = pd.read_parquet(representative_path)
    repevents_df = repevents_df.reset_index().set_index(["mrio_region","class"])
    repevents_df = repevents_df.rename_axis(["mrio_region","Impacting flood percentile"])
    df = df.set_index(["mrio_region","Impacting flood percentile"])
    df = df.join(repevents_df)
    df = df.reset_index()

    if (df["gdp_dmg_share"]==-1).any():
            df["gdp_dmg_share"] = df['share of GVA used as ARIO input']
    df = df[["period","MRIO","run_id","gdp_dmg_share","Total direct damage to capital (2010€PPP)", "year", "psi", "mrio_region", "final_cluster"]]
    df = df.set_index(["period","run_id"])
    del repevents_df
    return df

def get_final_clusters(general_df:pd.DataFrame, rep_events:pd.DataFrame) -> pd.DataFrame:
    res = pd.merge(general_df, rep_events[["final_cluster","mrio_region","class"]], how='left', left_on=["mrio_region","Impacting flood percentile"], right_on=["mrio_region","class"], validate="m:1")
    return res.drop(["mrio_region","class"],axis=1)

def prepare_loss_df(df_csv:pathlib.Path, period:str) -> pd.DataFrame:
    df = pd.read_csv(df_csv,index_col=[0], header=[0,1,2])
    df['period'] = period
    return df.reset_index().set_index(["period","run_id"])

def index_a_df(general_df:pd.DataFrame, df_to_index:pd.DataFrame, semester:bool) -> pd.DataFrame:
    if not semester:
        df_to_index = df_to_index.groupby(["sector type","region"],axis=1).sum()
        res_df = general_df.join(df_to_index.stack(level=list(range(df_to_index.columns.nlevels-1))))
    else:
        res_df = general_df.join(df_to_index.stack(level=list(range(df_to_index.columns.nlevels-1))))
    res_df.reset_index(inplace=True)
    res_df.set_index("run_id", inplace=True)
    res_df['semester'] = res_df['semester'].str.extract('(\d+)').astype(int)
    if res_df.isna().any().any():
        raise ValueError("NA found during treatment")
    if res_df is not None:
        return res_df
    else:
        raise ValueError("Dataframe is empty after treatment")

def remove_too_few_flood(df:pd.DataFrame, semester:bool) -> pd.DataFrame:
    df= df.copy()
    if semester:
        mask = df.groupby(["MRIO","mrio_region", "sector type","period","semester"]).count()
        df.set_index(["MRIO","mrio_region", "sector type","period","semester"],inplace=True) # this could break ? (I added semester)
    else:
        mask = df.groupby(["MRIO","mrio_region", "sector type","period"]).count()
        df.set_index(["MRIO","mrio_region", "sector type","period"],inplace=True)
    df.drop((mask[mask["year"]==1].index), inplace=True)
    df.reset_index(inplace=True)
    return df

def remove_not_sim_region(flood_base:pd.DataFrame, general_df:pd.DataFrame) -> pd.DataFrame:
    reg_sim = [reg for reg in flood_base["mrio_region"].unique() if reg in general_df["mrio_region"].unique()]
    return flood_base[flood_base["mrio_region"].isin(reg_sim)]

def prepare_flood_base(df_base:pd.DataFrame, period:str) -> pd.DataFrame:
    df_base['period'] = period
    df_base['year'] = df_base.date_start.dt.year
    #df_base.reset_index(inplace=True)
    #mask = df_base.groupby(["mrio_region","period"]).count()
    #df_base.set_index(["mrio_region","period"],inplace=True)
    #df_base.drop((mask[mask["final_cluster"]<=5].index),inplace=True)
    #df_base.reset_index(inplace=True)
    df_base.sort_values(by=["mrio_region","share of GVA used as ARIO input"],inplace=True)
    return df_base

def select_semesters(df:pd.DataFrame, sem:int):
    return df.loc[df["semester"]<=sem]

def extend_df(df_base:pd.DataFrame, df_loss:pd.DataFrame, semester:bool) -> pd.DataFrame:
    # expand for all mrio simulated
    df_mrio = pd.DataFrame({'MRIO':[mrio for mrio in df_loss.MRIO.unique()]})
    flood_base_loss = df_base.copy().merge(df_mrio,how="cross")
    df_sectors = pd.DataFrame({'sector type':[sector for sector in df_loss["sector type"].unique()]})
    flood_base_loss = flood_base_loss.merge(df_sectors,how="cross")
    if semester:
        df_semester = pd.DataFrame({'semester':[semester for semester in df_loss.semester.unique()]})
        flood_base_loss = flood_base_loss.merge(df_semester, how="cross")
    return flood_base_loss

def interpolations_coefs(reg_df,gdp_dmgs,regions, grouper):
    dico = {}
    reg_coefs = None
    for reg in regions:
        with warnings.catch_warnings():
            warnings.filterwarnings("error")
            np.seterr(invalid="raise")
            try:
                reg_coefs = reg_df.groupby(grouper).apply(lambda row_group: interp1d(row_group[gdp_dmgs], row_group[reg],fill_value="extrapolate"))
            except FloatingPointError:
                print(reg)
        if reg_coefs is None:
            raise ValueError("Error in regression coefficient computation")
        dico[reg] = reg_coefs
    return pd.concat(dico.values(), axis=1, keys=dico.keys(), names="region")


def reg_coef_dict_to_df_to_dict(df:pd.DataFrame, regions:list, grouper, values:str = "gdp_dmg_share") -> dict:
    df_res = interpolations_coefs(df, values, regions, grouper)
    return df_res.to_dict()

def run_interpolation_semester(mrios, semesters, flood_base:pd.DataFrame, region_list:list, loss_dict:dict, loss_type_str:str, sector_types:list) -> pd.DataFrame:
    # TODO See how to do this with a pd.concat
    flood_base = flood_base.reset_index()
    flood_base = flood_base.sort_values(by=["mrio_region", "share of GVA used as ARIO input"])
    flood_base_gr = flood_base.groupby("mrio_region")
    res_l = []
    # list of ALL regions
    for region in region_list:
        sect_l = []
        #print("Running indirect impacted region {}".format(region))
        for sector_type in sector_types:
            mr_l = []
            for mrio in mrios:
                #print("Running interpolation for {}".format(mrio))
                sem_l=[]
                for semester in semesters:
                    try:
                        serie = flood_base_gr.apply(lambda group : projected_loss_region(loss_dict=loss_dict,group=group, region=region, sector_type=sector_type, mrio=mrio, semester=semester))
                    except FloatingPointError as e:
                        raise RuntimeError(f"A floating point error happened when computing for region: {region}, for {sector_type}, for {mrio}, for semester {semester} :\n\n{e}")
                    serie.index = flood_base['final_cluster']
                    sem_l.append(serie)
                df_sem = pd.concat(sem_l, axis=0, keys=semesters, names=["semester"])
                mr_l.append(df_sem)
            df_mr = pd.concat(mr_l, axis=0, keys=mrios, names=["MRIO"])
            sect_l.append(df_mr)
        df_sect = pd.concat(sect_l, axis=1, keys=sector_types, names=["sector type"])
        res_l.append(df_sect)
    #print("Finished looping, doing last concatenation")
    df_res = pd.concat(res_l, axis=1, keys=region_list)
    return df_res

def projected_loss(loss_dict:dict, impacted_region:str, dmg:float, sector_type:str, mrio:str, semester:Optional[str]):
    """Return the projected loss for a given region, impacted region, sector, and damage."""
    if semester is None:
        try:
            return lambda region: loss_dict[region][(mrio, impacted_region, sector_type)](dmg)
        except KeyError:
            return lambda region: 0.0
        except FloatingPointError as e:
            raise RuntimeError(f"A floating point error happened when computing for impacted_region: {impacted_region}, for {sector_type}, for {mrio}, for {dmg} :\n\n{e}")
    else:
        try:
            return lambda region: loss_dict[region][(mrio, impacted_region, sector_type, semester)](dmg)
        except KeyError:
            return lambda region: 0.0
        except FloatingPointError as e:
            raise RuntimeError(f"A floating point error happened when computing for impacted_region: {impacted_region}, for {sector_type}, for {mrio}, for {dmg} :\n\n{e}")

def projected_loss_region(group, loss_dict:dict, region:str, sector_type:str, mrio:str, semester:Optional[str]):

    return pd.Series(projected_loss(loss_dict=loss_dict,impacted_region=group.head(1)["mrio_region"].values[0],dmg=group["share of GVA used as ARIO input"], sector_type=sector_type, mrio=mrio, semester=semester)(region), name=region)

parser = argparse.ArgumentParser(description="Interpolate results and produce aggregated results from all simulations")
parser.add_argument('-ig', "--input-general", type=str, help='The str path to the general.csv file', required=True)
parser.add_argument('-il', "--input-loss", type=str, help='The str path to the prod|final loss.csv file', required=True)
parser.add_argument('-T', "--loss-type", type=str, help='The type of loss to prepare prod|final', required=True)
parser.add_argument('-B', "--flood-base", type=str, help='Path where the flood database is.', required=True)
parser.add_argument('-R', "--representative", type=str, help='Path where the representative events database is.', required=True)
parser.add_argument('-N', "--period-name", type=str, help='Name of the period',required=True)
parser.add_argument('-o', "--output", type=str, help='Path where to save parquets files')

if __name__ == '__main__':
    args = parser.parse_args()
    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
    scriptLogger = logging.getLogger("Aggregate results from experience")
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    scriptLogger.addHandler(consoleHandler)
    scriptLogger.setLevel(logging.INFO)
    scriptLogger.propagate = False
    scriptLogger.info('Starting Script')

    semester_run = True
    semesters = 4
    loss_type = args.loss_type
    general_csv = pathlib.Path(args.input_general).resolve()
    loss_csv = pathlib.Path(args.input_loss).resolve()
    representative = args.representative
    period_name = args.period_name
    output = pathlib.Path(args.output)
    floodbase_path = pathlib.Path(args.flood_base)
    output.mkdir(parents=True, exist_ok=True)
    scriptLogger.info("#### PHASE 1 : {}loss : Read CSVs, properly index them, removing floods impossible to interpolate ####".format(loss_type))

    general_df = prepare_general_df(general_csv=general_csv, period=period_name, representative_path=representative)
    loss_df = prepare_loss_df(df_csv=loss_csv, period=period_name)
    if not floodbase_path.exists():
        raise ValueError("File {}, doesn't exist".format(floodbase_path))
    else:
        flood_base_df = pd.read_parquet(floodbase_path)

    flood_base_df = prepare_flood_base(flood_base_df,period=period_name)

    scriptLogger.info('Found all parquet files')
    # This is intriguing that we have to do that
    #flood_base_df = remove_not_sim_region(flood_base_df,general_df)

    regions_list = list(loss_df.columns.get_level_values(2).unique())
    flooded_regions = list(flood_base_df["mrio_region"].unique())
    sectors_list = list(loss_df.columns.get_level_values(1).unique())

    scriptLogger.info("#### Doing {}loss result ####".format(loss_type))
    scriptLogger.info("Indexing properly, removing too rare to extrapolate floods")
    loss_df = index_a_df(general_df, loss_df, semester_run)
    loss_df = select_semesters(loss_df,semesters)
    res_df = extend_df(flood_base_df, loss_df, semester_run)
    res_df.set_index(["final_cluster", "MRIO", "period", "mrio_region", "sector type", "semester"],inplace=True)
    loss_df.set_index(["final_cluster", "MRIO", "period", "mrio_region", "sector type", "semester"],inplace=True)

    res_df = res_df.merge(loss_df, how="outer", left_index=True, right_index=True, indicator=True, copy=False)
    sim_df = res_df.loc[res_df._merge=="both"].copy()
    sim_df.rename(columns={"Total direct damage to capital (2010€PPP)_x":"Total direct damage to capital (2010€PPP)", "year_x":"year"}, inplace=True)
    sim_df.reset_index(inplace=True)
    sim_df.drop(["Total direct damage to capital (2010€PPP)_y","year_y"],axis=1,inplace=True)
    res_df.rename(columns={"Total direct damage to capital (2010€PPP)_x":"Total direct damage to capital (2010€PPP)", "year_x":"year"}, inplace=True)
    res_df.reset_index(inplace=True)
    res_df.drop(["Total direct damage to capital (2010€PPP)_y","year_y"],axis=1,inplace=True)
    res_df.drop(res_df.loc[res_df._merge=="both"].index, inplace=True)

    # Remove simulated floods from flood base (we will merge those later)
    flood_base_df = flood_base_df.loc[~flood_base_df.final_cluster.isin(sim_df.final_cluster.unique())].copy()
    scriptLogger.info("Computing regression coefficients")

    # Now we can do this
    loss_df.reset_index(inplace=True)
    loss_df = remove_too_few_flood(loss_df,semester_run)
    res_df.set_index(["final_cluster", "MRIO", "sector type", "semester"],inplace=True)
    res_df.to_parquet(output/"{}loss_full_index.parquet".format(loss_type))
    sim_df.set_index(["final_cluster", "MRIO", "sector type", "semester"],inplace=True)
    sim_df.to_parquet(output/"{}loss_sim_df.parquet".format(loss_type))
    grouper = ["MRIO", "mrio_region", "sector type", "semester"]
    loss_dict = reg_coef_dict_to_df_to_dict(loss_df, regions=regions_list, grouper=grouper, values="gdp_dmg_share")
    res_df.reset_index(inplace=True)
    mrios = res_df.MRIO.unique()
    res_df = res_df.set_index("MRIO")
    scriptLogger.info("Writing temp result index to {}".format(output))
    res_df.to_parquet(output/"{}loss_full_index.parquet".format(loss_type))
    scriptLogger.info("Running interpolation")
    indexer = ["MRIO","semester","final_cluster"]
    semesters = loss_df.semester.unique()
    res_loss_df = run_interpolation_semester(mrios, semesters, flood_base_df, regions_list, loss_dict, loss_type_str="loss", sector_types=sectors_list)
    res_loss_df = res_loss_df.stack(level=1).reset_index()
    res_loss_df.set_index(["final_cluster", "MRIO", "sector type", "semester"],inplace=True)
    res_df.reset_index(inplace=True)
    res_df.set_index(["final_cluster", "MRIO", "sector type", "semester"],inplace=True)
    res_loss_df.to_parquet(output/"{}loss_interp_df.parquet".format(loss_type))
    res_df.update(res_loss_df,errors="raise")
    res_df = pd.concat([res_df, sim_df],axis=0)
    col1 = res_df.filter(regex=r"([A-Z]{2,3}\d{0,2}[A-Z]{0,2}\d{0,2})").columns
    col2 = pd.Index(["final_cluster", "period","model","mrio_region","MRIO", "sector type", "semester", "Total direct damage to capital (2010€PPP)", "Population aff (2015 est.)", "dmg_as_direct_prodloss (M€)", "direct_prodloss_as_2010gva_share", "share of GVA used as ARIO input", "return_period", "long", "lat"])
    cols = col2.union(col1,sort=False)
    res_df.reset_index(inplace=True)
    res_df = res_df[cols]
    res_df.set_index(["final_cluster", "MRIO", "sector type"],inplace=True)
    res_df.to_parquet(output/"{}loss_full_flood_base_results.parquet".format(loss_type))
