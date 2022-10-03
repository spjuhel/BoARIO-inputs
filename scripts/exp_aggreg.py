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
from datetime import date
tqdm.pandas()


def prepare_general_df(general_csv:pathlib.Path, period:str) -> pd.DataFrame:
    df = pd.read_csv(general_csv)
    df['period'] = period
    df_name = (df["run_name"].str.split("_",expand=True).rename(columns={0:"Impacted EXIO3 region",
                                                          1:"n0",
                                                          2:"MRIO type",
                                                          3:"n1",
                                                          4:"n2",
                                                          5:"Impacting flood percentile",
                                                          6:"n3",
                                                          7:"n4",
                                                          8:"n5",
                                                          9:"n6",
                                                          10:"n7",
                                                          11:"Inventory restoration Tau",
                                                          12:"n8",
                                                          13:"n9",
                                                          14:"Inventory initial duration"}))
    df_name = df_name.drop(list(df_name.filter(regex = r"n\d")), axis = 1)
    df = pd.concat([df_name, df],axis=1)
    df = df.drop(["region", "rebuild_durations", "inv_tau", "shortage_ind_mean"],axis=1)
    df = df.drop(["top_5_sector_chg", "10_first_shortages_(step,region,sector,stock_of)"],axis=1)
    df["prod_lost_aff"] = df["prod_lost_tot"] - df["prod_lost_unaff"]
    df["unaff_fd_unmet"] = df["tot_fd_unmet"] - df["aff_fd_unmet"]
    df = df.set_index(["period","mrio","run_name"])
    return df

def prepare_loss_df(df_csv:pathlib.Path, period:str) -> pd.DataFrame:
    df = pd.read_csv(df_csv,index_col=[0,1], header=[0,1])
    df['period'] = period
    return df.reset_index().set_index(["mrio","run_name","period"])

def index_a_df(general_df:pd.DataFrame, df_to_index:pd.DataFrame) -> pd.DataFrame:
    res_df = general_df.join(df_to_index.stack(level=0))
    res_df = res_df.reset_index().set_index("run_name")
    res_df = res_df.drop_duplicates(subset=["mrio","Impacted EXIO3 region", "gdp_dmg_share", "sector type","psi","period"])
    if res_df is not None:
        res_df = res_df.dropna(how="any",axis=0)
        return res_df
    else:
        raise ValueError("Dataframe is empty after treatment")

def remove_too_few_flood(df:pd.DataFrame) -> pd.DataFrame:
    mask = df.groupby(["mrio","Impacted EXIO3 region", "sector type","period"]).count()
    df = df.set_index(["mrio","Impacted EXIO3 region", "sector type","period"]).drop((mask[mask["MRIO type"]==1].index))
    df = df.reset_index()
    return df

def filter_by_psi(df:pd.DataFrame, psi:float) -> pd.DataFrame:
    return df[df['psi']==psi]

def filter_period(df:pd.DataFrame, period:list[int])-> pd.DataFrame:
    start = period[0]
    end = period[1]
    if (end - start) < 0:
        raise ValueError("Given period is void [start : {}, end : {}]".format(start,end))
    df = df[df.date_start.dt.year <= end]
    df = df[df.date_start.dt.year >= start]
    return df

# linear interpolation per group
def interpolations_coefs(reg_df,x_values,ys):
    dico = {}
    reg_coefs = None
    for y in ys:
        with warnings.catch_warnings():
            warnings.filterwarnings("error")
            np.seterr(invalid="raise")
            try:
                reg_coefs = reg_df.groupby(["mrio", "Impacted EXIO3 region", "sector type"]).apply(lambda row_group: interp1d(row_group[x_values], row_group[y],fill_value="extrapolate"))
            except FloatingPointError:
                print(y)
        if reg_coefs is None:
            raise ValueError("Error in regression coefficient computation")
        dico[y] = reg_coefs
    return pd.concat(dico.values(), axis=1, keys=dico.keys())

# This dict has shitty structure... : ###############################################################
# First key level is the region for wich we want to know the indirect impact from the flood
# Second key level is a tuple (region, sector_type); region is the region impacted by the flood,
# sector_type is the type (rebuilding/non rebuilding) of sectors we want to know the indirect impact from the flood
def reg_coef_dict_to_df_to_dict(df:pd.DataFrame, regions:list, values:str = "gdp_dmg_share") -> dict:
    df_res = interpolations_coefs(df, values, regions)
    return df_res.to_dict()

def projected_loss(loss_dict:dict, impacted_region:str, dmg:float, sector_type:str, mrio:str):
    """Return the projected loss for a given region, impacted region, sector, and damage."""
    return lambda region: loss_dict[region][(mrio, impacted_region, sector_type)](dmg)

def projected_loss_region(group, loss_dict:dict, region:str, sector_type:str, mrio:str):
    return pd.Series(projected_loss(loss_dict=loss_dict,impacted_region=group.head(1)["EXIO3_region"].values[0],dmg=group["dmg_as_gdp_share"], sector_type=sector_type, mrio=mrio)(region), name=region)

def prepare_flood_base(df_base:pd.DataFrame, period:str) -> pd.DataFrame:
    df_base['period'] = period
    df_base['year'] = df_base.date_start.dt.year
    df_base = df_base.reset_index()
    mask = df_base.groupby(["EXIO3_region","period"]).count()
    df_base = df_base.set_index(["EXIO3_region","period"]).drop((mask[mask["final_cluster"]<=5].index))
    df_base = df_base.reset_index()
    df_base = df_base.sort_values(by=["EXIO3_region","dmg_as_gdp_share"])
    return df_base

def extend_df_mrio(df_base:pd.DataFrame, df_loss:pd.DataFrame) -> pd.DataFrame:
    # expand for all mrio simulated
    df_mrio = pd.DataFrame({'mrio':[mrio for mrio in df_loss.mrio.unique()]})
    flood_base_loss = df_base.copy().merge(df_mrio,how="cross")
    return flood_base_loss

def run_interpolation(df_prodloss:pd.DataFrame, df_fdloss:pd.DataFrame, mrios, flood_base:pd.DataFrame, region_list:list, prodloss_dict:dict, fdloss_dict:dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    # TODO See how to do this with a pd.concat
    flood_base = flood_base.reset_index()
    flood_base = flood_base.sort_values(by=["EXIO3_region", "dmg_as_gdp_share"])
    flood_base_gr = flood_base.groupby("EXIO3_region")
    for mrio in mrios:
        print("Running interpolation for {}".format(mrio))
        for region in region_list:
            print("Running indirect impacted region {}".format(region))
            df_prodloss.loc[mrio,region+"_non-rebuild_prodloss (M€)"] = flood_base_gr.apply(lambda group : projected_loss_region(loss_dict=prodloss_dict,group=group, region=region, sector_type="non-rebuilding", mrio=mrio)).values
            df_prodloss.loc[mrio,region+"_rebuild_prodloss (M€)"] = flood_base_gr.apply(lambda group : projected_loss_region(loss_dict=prodloss_dict,group=group, region=region, sector_type="rebuilding", mrio=mrio)).values
            df_fdloss.loc[mrio,region+"_non-rebuild_fdloss (M€)"] = flood_base_gr.apply(lambda group : projected_loss_region(loss_dict=fdloss_dict,group=group, region=region, sector_type="non-rebuilding", mrio=mrio)).values
            df_fdloss.loc[mrio,region+"_rebuild_fdloss (M€)"] = flood_base_gr.apply(lambda group : projected_loss_region(loss_dict=fdloss_dict,group=group, region=region, sector_type="rebuilding", mrio=mrio)).values
    return df_prodloss, df_fdloss

def avoid_fragments(df:pd.DataFrame) -> pd.DataFrame:
    res = df.copy()
    return res

def prepare_for_maps(df_prod:pd.DataFrame, df_final_demand:pd.DataFrame) -> pd.DataFrame:
    def get_impacted_prodloss(row):
        return (row.loc[:,[(row.name[2],"rebuild_prodloss (M€)"),(row.name[2],"non-rebuild_prodloss (M€)")]]).sum()

    def get_impacted_fdloss(row):
        return (row.loc[:,[(row.name[2],"rebuild_fdloss (M€)"),(row.name[2],"non-rebuild_fdloss (M€)")]]).sum()

    df_prod = df_prod.rename(columns={
    "dmg_as_gdp_share":"Direct damage to capital (GDP share)",
    "dmg_as_direct_prodloss (M€)":"Direct production loss (M€)",
    "dmg_as_direct_prodloss (€)":"Direct production loss (€)",
    "direct_prodloss_as_gdp_share":"Direct production loss (GDP share)",
    "total_dmg": "Direct damage to capital (€)"
    })

    df_prod["year"] = pd.DatetimeIndex(df_prod["date_start"]).year
    df_prod["date_start"] = pd.DatetimeIndex(df_prod["date_start"])
    bins = [np.datetime64(date(1970,12,31)), np.datetime64(date(2005,12,31)), np.datetime64(date(2015,12,31)), np.datetime64(date(2034,12,31)), np.datetime64(date(2049,12,31)), np.datetime64(date(2069,12,31)), np.datetime64(date(2099,12,31)), np.datetime64(date(2149,12,31))] #type: ignore
    group_names = ["historic","[2006-2016[", "[2016-2035[", "[2035-2050[", "[2051-2070[", "[2071-2100[", "2100+"]
    df_prod["date_cat"] = pd.cut(df_prod.date_start, bins, labels=group_names)
    str_rebuild = "rebuild"
    str_non_rebuild = "non-rebuild"
    str_fdloss = "fdloss"
    str_prodloss = "prodloss"
    str_unit = r"\(M€\)"
    re_all = "^[A-Z]{2}_("+str_rebuild+"|"+str_non_rebuild+")_("+str_prodloss+"|"+str_fdloss+") "+str_unit+"$"
    df_prod = df_prod.reset_index()
    df_prod_all_events = df_prod.groupby(["mrio","model", "date_cat"])[list(df_prod.filter(regex = re_all))].agg("sum")
    assert df_prod_all_events is not None
    df_prod_all_events.columns = df_prod_all_events.columns.str.split("_" ,n=1,expand=True)
    df_prod_by_region_impacted = df_prod.groupby(["mrio", "model", "EXIO3_region", "date_cat"])[list(df_prod.filter(regex = re_all))].agg("sum")
    assert df_prod_by_region_impacted is not None
    df_prod_by_region_impacted.columns = df_prod_by_region_impacted.columns.str.split("_" ,n=1,expand=True)
    df_prod_by_region_impacted = df_prod_by_region_impacted.reset_index()
    prodloss_from_local_events = df_prod_by_region_impacted.groupby(["mrio", "model", "EXIO3_region","date_cat"]).apply(get_impacted_prodloss)
    prodloss_from_local_events.index.names = ["mrio", "model", "EXIO3_region", "date_cat", "affected region", "sector_type"]
    prodloss_from_local_events = prodloss_from_local_events.droplevel(4)
    prodloss_from_local_events.name = "Production change due to local events (M€)"

    df_final_demand = df_final_demand.rename(columns={
    "dmg_as_gdp_share":"Direct damage to capital (GDP share)",
    "dmg_as_direct_prodloss (M€)":"Direct production loss (M€)",
    "dmg_as_direct_prodloss (€)":"Direct production loss (€)",
    "direct_prodloss_as_gdp_share":"Direct production loss (GDP share)",
    "total_dmg": "Direct damage to capital (€)"
    })

    df_final_demand["year"] = pd.DatetimeIndex(df_final_demand["date_start"]).year
    df_final_demand["date_start"] = pd.DatetimeIndex(df_final_demand["date_start"])
    bins = [np.datetime64(date(1970,12,31)), np.datetime64(date(2005,12,31)), np.datetime64(date(2015,12,31)), np.datetime64(date(2034,12,31)), np.datetime64(date(2049,12,31)), np.datetime64(date(2069,12,31)), np.datetime64(date(2099,12,31)), np.datetime64(date(2149,12,31))] #type: ignore
    group_names = ["historic","[2006-2016[", "[2016-2035[", "[2035-2050[", "[2051-2070[", "[2071-2100[", "2100+"]
    df_final_demand["date_cat"] = pd.cut(df_final_demand.date_start, bins, labels=group_names)
    str_rebuild = "rebuild"
    str_non_rebuild = "non-rebuild"
    str_fdloss = "fdloss"
    str_prodloss = "prodloss"
    str_unit = r"\(M€\)"
    re_all = "^[A-Z]{2}_("+str_rebuild+"|"+str_non_rebuild+")_("+str_prodloss+"|"+str_fdloss+") "+str_unit+"$"
    df_final_demand = df_final_demand.reset_index()
    df_final_demand_all_events = df_final_demand.groupby(["mrio","model", "date_cat"])[list(df_final_demand.filter(regex = re_all))].agg("sum")
    assert df_final_demand_all_events is not None
    df_final_demand_all_events.columns = df_final_demand_all_events.columns.str.split("_" ,n=1,expand=True)
    df_final_demand_by_region_impacted = df_final_demand.groupby(["mrio", "model", "EXIO3_region", "date_cat"])[list(df_final_demand.filter(regex = re_all))].agg("sum")
    assert df_final_demand_by_region_impacted is not None
    df_final_demand_by_region_impacted.columns = df_final_demand_by_region_impacted.columns.str.split("_" ,n=1,expand=True)
    df_final_demand_by_region_impacted = df_final_demand_by_region_impacted.reset_index()
    #print(df_final_demand_by_region_impacted)
    finalloss_from_local_events = df_final_demand_by_region_impacted.groupby(["mrio", "model", "EXIO3_region","date_cat"]).apply(get_impacted_fdloss)
    finalloss_from_local_events.index.names = ["mrio", "model", "EXIO3_region", "date_cat", "affected region", "sector_type"]
    finalloss_from_local_events = finalloss_from_local_events.droplevel(4)
    finalloss_from_local_events.name = "Final consumption not met due to local events (M€)"

    total_direct_loss_df = df_prod.groupby(["mrio", "model", "EXIO3_region", "date_cat"])[["Direct damage to capital (€)","Direct production loss (GDP share)", "Direct production loss (M€)"]].sum()

    df_prod_all_events = df_prod_all_events.melt(value_name="Projected total production change (M€)",var_name=["region","sector_type"], ignore_index=False)
    df_prod_all_events = df_prod_all_events.rename(columns={"region":"EXIO3_region"}).set_index(["EXIO3_region","sector_type"], append=True)
    df_prod_all_events = df_prod_all_events.join(total_direct_loss_df)
    df_final_demand_all_events = df_final_demand_all_events.melt(value_name="Projected total final consumption not met (M€)",var_name=["region","sector_type"], ignore_index=False)
    df_final_demand_all_events = df_final_demand_all_events.rename(columns={"region":"EXIO3_region"}).set_index(["EXIO3_region","sector_type"], append=True)

    df_for_map = df_prod_all_events.join(pd.DataFrame(prodloss_from_local_events)).reset_index()
    df_for_map["Production change due to local events (M€)"] = df_for_map["Production change due to local events (M€)"].fillna(0)
    df_for_map["Direct damage to capital (€)"] = df_for_map["Direct damage to capital (€)"].fillna(0)
    df_for_map["Direct production loss (GDP share)"] = df_for_map["Direct production loss (GDP share)"].fillna(0)
    df_for_map["Direct production loss (M€)"] = df_for_map["Direct production loss (M€)"].fillna(0)
    df_for_map["Production change due to foreign events (M€)"] = df_for_map["Projected total production change (M€)"] - df_for_map["Production change due to local events (M€)"]
    df_for_map = df_for_map.set_index(["mrio", "model", "EXIO3_region","sector_type", "date_cat"])
    df_for_map = df_for_map.rename(index={"rebuild_prodloss (M€)":"Rebuilding",
                            "non-rebuild_prodloss (M€)":"Non-rebuilding"
                            })
    tmp = df_final_demand_all_events.join(pd.DataFrame(finalloss_from_local_events))
    tmp = tmp.rename(index={"rebuild_fdloss (M€)":"Rebuilding",
                            "non-rebuild_fdloss (M€)":"Non-rebuilding"
                            })
    df_for_map = df_for_map.join(tmp).reset_index()
    df_for_map["Final consumption not met due to local events (M€)"] = df_for_map["Final consumption not met due to local events (M€)"].fillna(0)
    df_for_map["Final consumption not met due to foreign events (M€)"] = df_for_map["Projected total final consumption not met (M€)"] - df_for_map["Final consumption not met due to local events (M€)"]

    return df_for_map


parser = argparse.ArgumentParser(description="Interpolate results and produce aggregated results from all simulations")
parser.add_argument('-i', "--input", type=str, help='The str path to the input experiment folder', required=True)
#parser.add_argument('run_type', type=str, help='The type of runs to produce csv from ("raw", "int" or "all")')
parser.add_argument('-B', "--flood-base", type=str, help='Path where the flood database is.', required=True)
parser.add_argument('-N', "--period-name", type=str, help='Name of the period',required=True)
parser.add_argument('-P', "--period", type=int, help='Starting and ending year for a specific period to study', nargs=2)
parser.add_argument('-o', "--output", type=str, help='Path where to save parquets files')
parser.add_argument("--psi", type=float, help='Psi value to check (when multiple)', default=0.85)

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

    folder = pathlib.Path(args.input).resolve()
    if not folder.exists():
        raise ValueError("Directory {}, doesn't exist".format(folder))
    else:
        general_df = prepare_general_df(general_csv=folder/"int_general.csv", period=args.period_name)
        prodloss_df = prepare_loss_df(df_csv=folder/"int_prodloss.csv", period=args.period_name)
        finaldemand_df = prepare_loss_df(df_csv=folder/"int_fdloss.csv", period=args.period_name)

    floodbase_path = pathlib.Path(args.flood_base)
    if not floodbase_path.exists():
        raise ValueError("File {}, doesn't exist".format(floodbase_path))
    else:
        flood_base_df = pd.read_parquet(floodbase_path)

    flood_base_df = prepare_flood_base(flood_base_df,period=args.period_name)
    if args.period is not None:
        flood_base_df = filter_period(flood_base_df, args.period)

    scriptLogger.info('Found all parquet files')
    regions_list = list(prodloss_df.columns.get_level_values(1).unique())
    flooded_regions = list(flood_base_df["EXIO3_region"].unique())

    scriptLogger.info("Indexing properly, removing too rare to extrapolate floods")
    prodloss_df = index_a_df(general_df, prodloss_df)
    prodloss_df = remove_too_few_flood(prodloss_df)
    finaldemand_df = index_a_df(general_df, finaldemand_df)
    finaldemand_df = remove_too_few_flood(finaldemand_df)

    scriptLogger.info("Computing regression coefficients")
    prodloss_dict = reg_coef_dict_to_df_to_dict(prodloss_df, regions=regions_list, values="gdp_dmg_share")
    finalloss_dict = reg_coef_dict_to_df_to_dict(finaldemand_df, regions=regions_list, values="gdp_dmg_share")

    if args.psi :
        prodloss_df = prodloss_df[prodloss_df['psi']==args.psi]
        finaldemand_df = finaldemand_df[finaldemand_df['psi']==args.psi]

    prodloss_df = extend_df_mrio(flood_base_df, prodloss_df)
    finaldemand_df = extend_df_mrio(flood_base_df, finaldemand_df)
    mrios = prodloss_df.mrio.unique()
    prodloss_df = prodloss_df.set_index("mrio")
    finaldemand_df = finaldemand_df.set_index("mrio")
    scriptLogger.info("Running interpolation")
    prodloss_df, finaldemand_df = run_interpolation(prodloss_df, finaldemand_df, mrios, flood_base_df, regions_list, prodloss_dict, finalloss_dict)
    # prodloss_df =  run_interpolation(prodloss_df, mrios, flood_base_df, regions_list, prodloss_dict, "non-rebuilding","_non-rebuild_prodloss (M€)")
    prodloss_df = avoid_fragments(prodloss_df)
    # finaldemand_df = run_interpolation(finaldemand_df, mrios, flood_base_df, regions_list, finalloss_dict, "rebuilding","_rebuild_fdloss (M€)")
    # finaldemand_df = run_interpolation(finaldemand_df, mrios, flood_base_df, regions_list, finalloss_dict, "non-rebuilding","_non-rebuild_fdloss (M€)")
    finaldemand_df = avoid_fragments(finaldemand_df)

    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    scriptLogger.info("Writing result to {}".format(output))
    prodloss_df.to_parquet(output/"prodloss_full_flood_base_results.parquet")
    finaldemand_df.to_parquet(output/"fdloss_full_flood_base_results.parquet")

    #prodloss_df = pd.read_parquet(output/"prodloss_full_flood_base_results.parquet")
    #finaldemand_df = pd.read_parquet(output/"fdloss_full_flood_base_results.parquet")
    scriptLogger.info("Building df for maps")
    df_for_maps = prepare_for_maps(prodloss_df, finaldemand_df)
    df_for_maps.to_parquet(output/"df_for_maps.parquet",index=False)
    scriptLogger.info("Everything finished !")
