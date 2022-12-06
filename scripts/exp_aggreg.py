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
import geopandas as geopd
import logging
import argparse
tqdm.pandas()

def check_df(df:pd.DataFrame):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Dataframe is not of type DataFrame")
    if "return_period" not in df.columns:
        raise ValueError("Dataframe has no 'return_period' column")
    if ("lat" not in df.columns) or ("long" not in df.columns):
        raise ValueError("Dataframe lacks either 'lat', 'long' or both column(s)")

def check_flopros(flopros:geopd.GeoDataFrame):
    if not isinstance(flopros, geopd.GeoDataFrame):
        raise ValueError("Flopros dataframe is not of type GeoDataFrame")
    if "MerL_Riv" not in flopros.columns:
        raise ValueError("Dataframe has no 'MerL_Riv' column (ie merged river flood protection layer)")

def gdfy_floods(df:pd.DataFrame, crs="epsg:4326"):
    gdf = geopd.GeoDataFrame(df, geometry=geopd.points_from_xy(df.long, df.lat), crs = crs)
    return gdf

def join_flopros(gdf:geopd.GeoDataFrame,flopros:geopd.GeoDataFrame):
    res = geopd.sjoin(gdf,flopros[["MerL_Riv","geometry"]], how="left",predicate="within")
    res.drop(["index_right","geometry"],axis=1,inplace=True)
    res["protected"] = res["return_period"] < res["MerL_Riv"]
    return pd.DataFrame(res)

def prepare_general_df(general_csv:pathlib.Path, period:str, representative_path) -> pd.DataFrame:
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
    df_name.drop(list(df_name.filter(regex = r"n\d")), axis = 1, inplace=True)
    df = pd.concat([df_name, df],axis=1)
    df.drop(["region", "rebuild_durations", "inv_tau", "shortage_ind_mean"],axis=1, inplace=True)
    df.drop(["top_5_sector_chg", "10_first_shortages_(step,region,sector,stock_of)"],axis=1, inplace=True)
    df["prod_lost_aff"] = df["prod_lost_tot"] - df["prod_lost_unaff"]
    df["unaff_fd_unmet"] = df["tot_fd_unmet"] - df["aff_fd_unmet"]
    df.set_index(["period","mrio","run_name"], inplace=True)
    repevents_df = pd.read_parquet(representative_path)
    repevents_df = repevents_df.reset_index().set_index(["EXIO3_region","class"])
    repevents_df = repevents_df.rename_axis(["Impacted EXIO3 region","Impacting flood percentile"])
    df = df.reset_index().set_index(["Impacted EXIO3 region","Impacting flood percentile"])
    df = df.join(repevents_df)
    df = df.reset_index()

    if (df["gdp_dmg_share"]==-1).any():
            df["gdp_dmg_share"] = df['share of GVA used as ARIO input']
    df = df[["period","mrio","run_name","gdp_dmg_share","Total direct damage (2010€PPP)", "year", "psi", "Impacted EXIO3 region", "MRIO type", "final_cluster"]]
    df = df.set_index(["period","mrio","run_name"])
    del repevents_df
    return df

def get_final_clusters(general_df:pd.DataFrame, rep_events:pd.DataFrame) -> pd.DataFrame:
    res = pd.merge(general_df, rep_events[["final_cluster","EXIO3_region","class"]], how='left', left_on=["Impacted EXIO3 region","Impacting flood percentile"], right_on=["EXIO3_region","class"], validate="m:1")
    return res.drop(["EXIO3_region","class"],axis=1)

def prepare_loss_df(df_csv:pathlib.Path, period:str) -> pd.DataFrame:
    df = pd.read_csv(df_csv,index_col=[0,1], header=[0,1,2])
    df['period'] = period
    return df.reset_index().set_index(["mrio","run_name","period"])

def index_a_df(general_df:pd.DataFrame, df_to_index:pd.DataFrame, semester:bool) -> pd.DataFrame:
    if not semester:
        df_to_index = df_to_index.groupby(["sector type","region"],axis=1).sum()
        res_df = general_df.join(df_to_index.stack(level=list(range(df_to_index.columns.nlevels-1))))
    else:
        res_df = general_df.join(df_to_index.stack(level=list(range(df_to_index.columns.nlevels-1))))
    res_df.reset_index(inplace=True)
    res_df.set_index("run_name", inplace=True)
    res_df['semester'] = res_df['semester'].str.extract('(\d+)').astype(int)
    # Why did I do this ?
    # if semester:
    #     res_df.drop_duplicates(subset=["mrio","Impacted EXIO3 region", "gdp_dmg_share", "sector type","psi","period","semester"], inplace=True)
    # else:
    #     res_df.drop_duplicates(subset=["mrio","Impacted EXIO3 region", "gdp_dmg_share", "sector type","psi","period"], inplace=True)
    if res_df.isna().any().any():
        raise ValueError("NA found during treatment")
    if res_df is not None:
        return res_df
    else:
        raise ValueError("Dataframe is empty after treatment")

def remove_too_few_flood(df:pd.DataFrame, semester:bool) -> pd.DataFrame:
    df= df.copy()
    if semester:
        mask = df.groupby(["mrio","Impacted EXIO3 region", "sector type","period","semester"]).count()
        df.set_index(["mrio","Impacted EXIO3 region", "sector type","period","semester"],inplace=True) # this could break ? (I added semester)
    else:
        mask = df.groupby(["mrio","Impacted EXIO3 region", "sector type","period"]).count()
        df.set_index(["mrio","Impacted EXIO3 region", "sector type","period"],inplace=True)
    df.drop((mask[mask["MRIO type"]==1].index), inplace=True)
    df.reset_index(inplace=True)
    return df

def remove_not_sim_region(flood_base:pd.DataFrame, general_df:pd.DataFrame) -> pd.DataFrame:
    reg_sim = [reg for reg in flood_base["EXIO3_region"].unique() if reg in general_df["Impacted EXIO3 region"].unique()]
    return flood_base[flood_base["EXIO3_region"].isin(reg_sim)]

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

def select_semesters(df:pd.DataFrame, sem:int):
    return df.loc[df["semester"]<=sem]

# linear interpolation per group
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

# This dict has shitty structure... : ###############################################################
# First key level is the region for wich we want to know the indirect impact from the flood
# Second key level is a tuple (region, sector_type); region is the region impacted by the flood,
# sector_type is the type (rebuilding/non rebuilding) of sectors we want to know the indirect impact from the flood
def reg_coef_dict_to_df_to_dict(df:pd.DataFrame, regions:list, grouper, values:str = "gdp_dmg_share") -> dict:
    df_res = interpolations_coefs(df, values, regions, grouper)
    return df_res.to_dict()

def projected_loss(loss_dict:dict, impacted_region:str, dmg:float, sector_type:str, mrio:str, semester:Optional[str]):
    """Return the projected loss for a given region, impacted region, sector, and damage."""
    if semester is None:
        try:
            return lambda region: loss_dict[region][(mrio, impacted_region, sector_type)](dmg)
        except KeyError:
            return lambda region: 0.0
    else:
        try:
            return lambda region: loss_dict[region][(mrio, impacted_region, sector_type, semester)](dmg)
        except KeyError:
            return lambda region: 0.0


def projected_loss_region(group, loss_dict:dict, region:str, sector_type:str, mrio:str, semester:Optional[str]):

    return pd.Series(projected_loss(loss_dict=loss_dict,impacted_region=group.head(1)["EXIO3_region"].values[0],dmg=group["dmg_as_2015_gva_share"], sector_type=sector_type, mrio=mrio, semester=semester)(region), name=region)

def prepare_flood_base(df_base:pd.DataFrame, period:str) -> pd.DataFrame:
    df_base['period'] = period
    df_base['year'] = df_base.date_start.dt.year
    df_base.reset_index(inplace=True)
    #mask = df_base.groupby(["EXIO3_region","period"]).count()
    #df_base.set_index(["EXIO3_region","period"],inplace=True)
    #df_base.drop((mask[mask["final_cluster"]<=5].index),inplace=True)
    #df_base.reset_index(inplace=True)
    df_base.sort_values(by=["EXIO3_region","dmg_as_2015_gva_share"],inplace=True)
    return df_base

def extend_df(df_base:pd.DataFrame, df_loss:pd.DataFrame, semester:bool) -> pd.DataFrame:
    # expand for all mrio simulated
    df_mrio = pd.DataFrame({'mrio':[mrio for mrio in df_loss.mrio.unique()]})
    flood_base_loss = df_base.copy().merge(df_mrio,how="cross")
    df_sectors = pd.DataFrame({'sector type':[sector for sector in df_loss["sector type"].unique()]})
    flood_base_loss = flood_base_loss.merge(df_sectors,how="cross")
    if semester:
        df_semester = pd.DataFrame({'semester':[semester for semester in df_loss.semester.unique()]})
        flood_base_loss = flood_base_loss.merge(df_semester, how="cross")
    return flood_base_loss

def run_interpolation_semester(mrios, semesters, flood_base:pd.DataFrame, region_list:list, loss_dict:dict, loss_type_str:str) -> pd.DataFrame:
    # TODO See how to do this with a pd.concat
    flood_base = flood_base.reset_index()
    flood_base = flood_base.sort_values(by=["EXIO3_region", "dmg_as_2015_gva_share"])
    flood_base_gr = flood_base.groupby("EXIO3_region")
    res_l = []
    # list of ALL regions
    for region in region_list:
        sect_l = []
        #print("Running indirect impacted region {}".format(region))
        for sector_type in ["rebuilding","non-rebuilding"]:
            mr_l = []
            for mrio in mrios:
                #print("Running interpolation for {}".format(mrio))
                sem_l=[]
                for semester in semesters:
                    serie = flood_base_gr.apply(lambda group : projected_loss_region(loss_dict=loss_dict,group=group, region=region, sector_type=sector_type, mrio=mrio, semester=semester))
                    serie.index = flood_base['final_cluster']
                    sem_l.append(serie)
                df_sem = pd.concat(sem_l, axis=0, keys=semesters, names=["semester"])
                mr_l.append(df_sem)
            df_mr = pd.concat(mr_l, axis=0, keys=mrios, names=["mrio"])
            sect_l.append(df_mr)
        df_sect = pd.concat(sect_l, axis=1, keys=["rebuilding","non-rebuilding"], names=["sector type"])
        res_l.append(df_sect)
    #print("Finished looping, doing last concatenation")
    df_res = pd.concat(res_l, axis=1, keys=region_list)
    return df_res

def run_interpolation_simple(mrios, flood_base:pd.DataFrame, region_list:list, loss_dict:dict, loss_type_str:str) -> pd.DataFrame:
    # TODO See how to do this with a pd.concat
    flood_base.reset_index(inplace=True)
    flood_base.sort_values(by=["EXIO3_region", "dmg_as_2015_gva_share"], inplace=True)
    flood_base_gr = flood_base.groupby("EXIO3_region")
    res_l = []
    for region in region_list:
        sect_l = []
        #print("Running indirect impacted region {}".format(region))
        for sector_type in ["rebuilding","non-rebuilding"]:
            mr_l = []
            for mrio in mrios:
                #print("Running interpolation for {}".format(mrio))
                serie = flood_base_gr.apply(lambda group : projected_loss_region(loss_dict=loss_dict,group=group, region=region, sector_type=sector_type, mrio=mrio, semester=None))
                serie.index = flood_base['final_cluster']
                mr_l.append(serie)
            df_mr = pd.concat(mr_l, axis=0, keys=mrios, names=["mrio"])
            sect_l.append(df_mr)
        df_sect = pd.concat(sect_l, axis=1, keys=["rebuilding", "non-rebuilding"], names=["sector type"])
        res_l.append(df_sect)
    #print("Finished looping, doing last concatenation")
    df_res = pd.concat(res_l, axis=1, keys=region_list)
    return df_res

def avoid_fragments(df:pd.DataFrame) -> pd.DataFrame:
    res = df.copy()
    return res

def preprepare_for_maps(df_loss:pd.DataFrame, loss_type:str, save_path, regions_list, semester:bool):
    def get_impacted_prodloss(row):
        return (row[row.name[2]]).sum()
        #return (row.loc[:,[(row.name[2],"rebuild_prodloss (M€)"),(row.name[2],"non-rebuild_prodloss (M€)")]]).sum()

    def get_impacted_fdloss(row):
        return (row[row.name[2]]).sum()
        #return (row.loc[:,[(row.name[2],"rebuild_fdloss (M€)"),(row.name[2],"non-rebuild_fdloss (M€)")]]).sum()


    if semester:
        grouper1=["mrio", "model", "EXIO3_region", "period","semester","sector type"]
        grouper2=["mrio", "model", "period","semester","sector type"]
        index_name = ["mrio", "model", "EXIO3_region", "period", "semester", "affected region", "sector_type"]
        droplevel=5
    else:
        grouper1=["mrio", "model", "EXIO3_region", "period","sector type"]
        grouper2=["mrio", "model", "period","sector type"]
        index_name = ["mrio", "model", "EXIO3_region", "period", "affected region", "sector_type"]
        droplevel=4
    if loss_type == "prod":
        df_loss = df_loss.rename(columns={
            "dmg_as_2015_gva_share":"Direct damage to capital (2015GVA share)",
            "dmg_as_direct_prodloss (M€)":"Direct production loss (M€)",
            "dmg_as_direct_prodloss (€)":"Direct production loss (€)",
            "direct_prodloss_as_2015gva_share":"Direct production loss (2015GVA share)",
            "Total direct damage (2010€PPP)": "Total direct damage to capital (2010€PPP)"
        })
        # str_rebuild = "rebuild"
        # str_non_rebuild = "non-rebuild"
        # str_fdloss = "fdloss"
        # str_prodloss = "prodloss"
        # str_unit = r"\(M€\)"
        # re_all = "^[A-Z]{2}_("+str_rebuild+"|"+str_non_rebuild+")_("+str_prodloss+"|"+str_fdloss+") "+str_unit+"$"
        # df_loss.reset_index(inplace=True)
        # df_loss_all_events = df_loss.groupby(["mrio","model", "period"])[list(df_loss.filter(regex = re_all))].agg("sum")
        df_loss_all_events = df_loss.groupby(grouper2)[regions_list].sum()
        assert df_loss_all_events is not None
        #df_loss_all_events.columns = df_loss_all_events.columns.str.split("_" ,n=1,expand=True)
        #df_prod_by_region_impacted = df_loss.groupby(grouper)[list(df_loss.filter(regex = re_all))].agg("sum")
        df_prod_by_region_impacted = df_loss.groupby(grouper1)[regions_list].sum()
        assert df_prod_by_region_impacted is not None
        #df_prod_by_region_impacted.columns = df_prod_by_region_impacted.columns.str.split("_" ,n=1,expand=True)
        #df_prod_by_region_impacted.reset_index(inplace=True)
        prodloss_from_local_events = df_prod_by_region_impacted.groupby(grouper1).apply(get_impacted_prodloss)
        #prodloss_from_local_events.index.names = index_name
        #prodloss_from_local_events = prodloss_from_local_events.droplevel(droplevel)
        #prodloss_from_local_events.name = "Production change due to local events (M€)"
        total_direct_loss_df = df_loss.groupby(["mrio", "model", "EXIO3_region", "period"])[["Total direct damage to capital (2010€PPP)","Direct production loss (2015GVA share)", "Direct production loss (M€)"]].sum()
        df_loss_all_events = df_loss_all_events.melt(value_name="Projected total production change (M€)",var_name=["region"], ignore_index=False)
        df_loss_all_events.rename(columns={"region":"EXIO3_region"},inplace=True)
        df_loss_all_events.reset_index(inplace=True)
        df_loss_all_events.set_index(["mrio", "model", "EXIO3_region", "period"],inplace=True)
        #df_loss_all_events.set_index(["EXIO3_region","sector_type"], append=True, inplace=True)
        df_loss_all_events = df_loss_all_events.join(total_direct_loss_df)

        total_direct_loss_df.to_parquet(save_path/"4_direct_loss.parquet")
        prodloss_from_local_events.to_pickle(save_path/"4_prodloss_local.pkl")
        df_loss_all_events.to_parquet(save_path/"4_prodloss_all.parquet")
        return prodloss_from_local_events

    elif loss_type == "final":

        df_loss = df_loss.rename(columns={
        "dmg_as_2015_gva_share":"Direct damage to capital (2015GVA share)",
        "dmg_as_direct_prodloss (M€)":"Direct production loss (M€)",
        "dmg_as_direct_prodloss (€)":"Direct production loss (€)",
        "direct_prodloss_as_gva_share":"Direct production loss (2015GVA share)",
        "Total direct damage (2010€PPP)": "Total direct damage to capital (2010€PPP)"
        })

        # str_rebuild = "rebuild"
        # str_non_rebuild = "non-rebuild"
        # str_fdloss = "fdloss"
        # str_prodloss = "prodloss"
        # str_unit = r"\(M€\)"
        # re_all = "^[A-Z]{2}_("+str_rebuild+"|"+str_non_rebuild+")_("+str_prodloss+"|"+str_fdloss+") "+str_unit+"$"
        #df_final_demand.reset_index(inplace=True)
        df_final_demand_all_events = df_loss.groupby(grouper2)[regions_list].sum()
        assert df_final_demand_all_events is not None
        #df_final_demand_all_events.columns = df_final_demand_all_events.columns.str.split("_" ,n=1,expand=True)
        df_final_demand_by_region_impacted = df_loss.groupby(grouper1)[regions_list].sum()
        assert df_final_demand_by_region_impacted is not None
        #df_final_demand_by_region_impacted.columns = df_final_demand_by_region_impacted.columns.str.split("_" ,n=1,expand=True)
        #df_final_demand_by_region_impacted.reset_index(inplace=True)
        #print(df_final_demand_by_region_impacted)
        finalloss_from_local_events = df_final_demand_by_region_impacted.groupby(grouper1).apply(get_impacted_fdloss)
        #finalloss_from_local_events.index.names = index_name
        #finalloss_from_local_events = finalloss_from_local_events.droplevel(droplevel)
        #finalloss_from_local_events.name = "Final consumption not met due to local events (M€)"

        df_final_demand_all_events = df_final_demand_all_events.melt(value_name="Projected total final consumption not met (M€)",var_name=["region"], ignore_index=False)
        #df_final_demand_all_events.rename(columns={"region":"EXIO3_region"},inplace=True)
        #df_final_demand_all_events.set_index(["EXIO3_region","sector_type"], append=True, inplace=True)

        finalloss_from_local_events.to_pickle(save_path/"5_fdloss_local.pkl")
        df_final_demand_all_events.to_parquet(save_path/"5_fdloss_all.parquet")
        return finalloss_from_local_events


def prepare_for_maps(df_prod_all_events:pd.DataFrame, prodloss_from_local_events:pd.DataFrame, df_final_demand_all_events, finalloss_from_local_events, semester:bool) -> pd.DataFrame:
    if semester:
        indexer = ["mrio", "model", "EXIO3_region","sector type", "period", "semester"]
    else:
        indexer = ["mrio", "model", "EXIO3_region","sector type", "period"]
    prodloss_from_local_events.name = "Production change due to local events (M€)"
    df_prod_all_events.reset_index(inplace=True)
    #df_prod_all_events.set_index(indexer,inplace=True)
    prodloss_from_local_events.set_index(indexer,inplace=True)
    df_for_map = df_prod_all_events.join(pd.DataFrame(prodloss_from_local_events)).reset_index()
    df_for_map["Production change due to local events (M€)"] = df_for_map["Production change due to local events (M€)"].fillna(0)
    df_for_map["Total direct damage to capital (2010€PPP)"] = df_for_map["Total direct damage to capital (2010€PPP)"].fillna(0)
    df_for_map["Direct production loss (2015GVA share)"] = df_for_map["Direct production loss (2015GVA share)"].fillna(0)
    df_for_map["Direct production loss (M€)"] = df_for_map["Direct production loss (M€)"].fillna(0)
    df_for_map["Production change due to foreign events (M€)"] = df_for_map["Projected total production change (M€)"] - df_for_map["Production change due to local events (M€)"]
    df_for_map = df_for_map.set_index(indexer)
    #df_for_map = df_for_map.rename(index={"rebuild_prodloss (M€)":"Rebuilding",
    #                        "non-rebuild_prodloss (M€)":"Non-rebuilding"
    #                        })
    finalloss_from_local_events.name = "Final consumption not met due to local events (M€)"
    df_final_demand_all_events.rename(columns={"region":"EXIO3_region"},inplace=True)
    df_final_demand_all_events.set_index("EXIO3_region",append=True,inplace=True)
    tmp = df_final_demand_all_events.join(pd.DataFrame(finalloss_from_local_events))
    #tmp = tmp.rename(index={"rebuild_fdloss (M€)":"Rebuilding",
    #                        "non-rebuild_fdloss (M€)":"Non-rebuilding"
    #                        })
    #df_for_map = df_for_map.set_index("region",append=True)
    #tmp = tmp.set_index("region",append=True)
    df_for_map = df_for_map.join(tmp).reset_index()
    df_for_map["Final consumption not met due to local events (M€)"] = df_for_map["Final consumption not met due to local events (M€)"].fillna(0)
    df_for_map["Final consumption not met due to foreign events (M€)"] = df_for_map["Projected total final consumption not met (M€)"] - df_for_map["Final consumption not met due to local events (M€)"]
    #df_for_map.rename(columns={"EXIO3_region":"Flooded region"},inplace=True)
    return df_for_map

def prepare_for_maps2(df_prod_all_events:pd.DataFrame, prodloss_from_local_events:pd.DataFrame, semester:bool) -> pd.DataFrame:
    if semester:
        indexer = ["mrio", "model", "EXIO3_region","sector_type", "period", "semester"]
    else:
        indexer = ["mrio", "model", "EXIO3_region","sector_type", "period"]
    df_for_map = df_prod_all_events.join(pd.DataFrame(prodloss_from_local_events)).reset_index()
    df_for_map["Production change due to local events (M€)"] = df_for_map["Production change due to local events (M€)"].fillna(0)
    df_for_map["Total direct damage to capital (2010€PPP)"] = df_for_map["Total direct damage to capital (2010€PPP)"].fillna(0)
    df_for_map["Direct production loss (2015GVA share)"] = df_for_map["Direct production loss (2015GVA share)"].fillna(0)
    df_for_map["Direct production loss (M€)"] = df_for_map["Direct production loss (M€)"].fillna(0)
    df_for_map["Production change due to foreign events (M€)"] = df_for_map["Projected total production change (M€)"] - df_for_map["Production change due to local events (M€)"]
    df_for_map = df_for_map.set_index(indexer)
    df_for_map = df_for_map.rename(index={"rebuild_prodloss (M€)":"Rebuilding",
                            "non-rebuild_prodloss (M€)":"Non-rebuilding"
                            })
    return df_for_map


parser = argparse.ArgumentParser(description="Interpolate results and produce aggregated results from all simulations")
parser.add_argument('-i', "--input", type=str, help='The str path to the input experiment folder', required=True)
#parser.add_argument('run_type', type=str, help='The type of runs to produce csv from ("raw", "int" or "all")')
parser.add_argument('-B', "--flood-base", type=str, help='Path where the flood database is.', required=True)
parser.add_argument('-R', "--representative", type=str, help='Path where the representative events database is.', required=True)
parser.add_argument('-N', "--period-name", type=str, help='Name of the period',required=True)
parser.add_argument("--phase", type=int, help='Call for the second phase', default=None)
parser.add_argument("--semester", type=int, help='Separate by semester', default=None)
parser.add_argument('-P', "--period", type=int, help='Starting and ending year for a specific period to study', nargs=2)
parser.add_argument('-o', "--output", type=str, help='Path where to save parquets files')
parser.add_argument("--psi", type=float, help='Psi value to check (when multiple)')
parser.add_argument("--protection-dataframe", type=str, help='Path where the protection database is.', required=True)

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
    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    if args.semester is not None:
        semester_run=True
        scriptLogger.info("Results by semester")
    else:
        semester_run=False

    if args.phase is None or args.phase == 1 :
        scriptLogger.info("#### PHASE 1 : Prodloss : Read CSVs, properly index them, removing floods impossible to interpolate ####")
        if not folder.exists():
            raise ValueError("Directory {}, doesn't exist".format(folder))
        else:
            general_df = prepare_general_df(general_csv=folder/"int_general.csv", period=args.period_name, representative_path=args.representative)
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
        # This is intriguing that we have to do that
        flood_base_df = remove_not_sim_region(flood_base_df,general_df)
        regions_list = list(prodloss_df.columns.get_level_values(2).unique())
        flooded_regions = list(flood_base_df["EXIO3_region"].unique())

        scriptLogger.info("#### Doing prodloss result ####")
        scriptLogger.info("Indexing properly, removing too rare to extrapolate floods")
        prodloss_df = index_a_df(general_df, prodloss_df, semester_run)
        prodloss_df = select_semesters(prodloss_df,args.semester)
        if args.psi :
            prodloss_df = prodloss_df[prodloss_df['psi']==args.psi]
        res_df = extend_df(flood_base_df, prodloss_df, semester_run)
        if semester_run:
            res_df.set_index(["final_cluster", "mrio", "period", "EXIO3_region", "sector type", "semester"],inplace=True)
            prodloss_df.set_index(["final_cluster", "mrio", "period", "Impacted EXIO3 region", "sector type", "semester"],inplace=True)
        else:
            res_df.set_index(["final_cluster", "mrio", "period", "Impacted EXIO3 region", "sector type"],inplace=True)
            prodloss_df.set_index(["final_cluster", "mrio", "period", "Impacted EXIO3 region", "sector type"],inplace=True)
        res_df = res_df.merge(prodloss_df, how="outer", left_index=True, right_index=True, indicator=True, copy=False)
        sim_df = res_df.loc[res_df._merge=="both"].copy()
        sim_df.rename(columns={"Total direct damage (2010€PPP)_x":"Total direct damage (2010€PPP)", "year_x":"year"}, inplace=True)
        sim_df.reset_index(inplace=True)
        sim_df.drop(["Impacted EXIO3 region","Total direct damage (2010€PPP)_y","year_y"],axis=1,inplace=True)
        res_df.rename(columns={"Total direct damage (2010€PPP)_x":"Total direct damage (2010€PPP)", "year_x":"year"}, inplace=True)
        res_df.reset_index(inplace=True)
        res_df.drop(["Impacted EXIO3 region","Total direct damage (2010€PPP)_y","year_y"],axis=1,inplace=True)
        res_df.drop(res_df.loc[res_df._merge=="both"].index, inplace=True)

        # Remove simulated floods from flood base (we will merge those later)
        flood_base_df = flood_base_df.loc[~flood_base_df.final_cluster.isin(sim_df.final_cluster.unique())].copy()
        scriptLogger.info("Computing regression coefficients")
        if semester_run:
            grouper = ["mrio", "Impacted EXIO3 region", "sector type", "semester"]
        else:
            grouper = ["mrio", "Impacted EXIO3 region", "sector type"]

        # Now we can do this
        prodloss_df.reset_index(inplace=True)
        prodloss_df = remove_too_few_flood(prodloss_df,semester_run)
        prodloss_dict = reg_coef_dict_to_df_to_dict(prodloss_df, regions=regions_list, grouper=grouper, values="gdp_dmg_share")
        mrios = res_df.mrio.unique()
        res_df = res_df.set_index("mrio")
        scriptLogger.info("Writing temp result index to {}".format(output))
        res_df.to_parquet(output/"1_prodloss_full_index.parquet")
        scriptLogger.info("Running interpolation")
        if semester_run:
            indexer = ["mrio","semester","final_cluster"]
            semesters = prodloss_df.semester.unique()
            res_prodloss_df = run_interpolation_semester(mrios, semesters, flood_base_df, regions_list, prodloss_dict, loss_type_str="prodloss")
        else:
            indexer = ["mrio", "final_cluster"]
            res_prodloss_df = run_interpolation_simple(mrios, flood_base_df, regions_list, prodloss_dict, loss_type_str="prodloss")
        res_prodloss_df = res_prodloss_df.stack(level=1).reset_index()#rename_axis(index=indexer)
        #res_prodloss_df.columns = ["_".join(a) for a in res_prodloss_df.columns.to_flat_index()]
        if semester_run:
            res_prodloss_df.set_index(["final_cluster", "mrio", "sector type", "semester"],inplace=True)
            res_df.reset_index(inplace=True)
            #sim_df.reset_index(inplace=True)
            res_df.set_index(["final_cluster", "mrio", "sector type", "semester"],inplace=True)
            sim_df.set_index(["final_cluster", "mrio", "sector type", "semester"],inplace=True)
        else:
            res_prodloss_df.set_index(["final_cluster", "mrio", "sector type"],inplace=True)
            res_df.reset_index(inplace=True)
            #sim_df.reset_index(inplace=True)
            res_df.set_index(["final_cluster", "mrio", "sector type"],inplace=True)
            sim_df.set_index(["final_cluster", "mrio", "sector type"],inplace=True)
        res_df.update(res_prodloss_df,errors="raise")
        res_df = pd.concat([res_df, sim_df],axis=0)
        scriptLogger.info("Writing temp result to {}".format(output))
        col1 = res_df.filter(regex="^[A-Z]{2}$").columns
        col2 = pd.Index(["final_cluster", "period","model","EXIO3_region","mrio", "sector type", "semester", "Total direct damage (2010€PPP)", "Population aff (2015 est.)", "dmg_as_direct_prodloss (M€)", "direct_prodloss_as_2015gva_share", "share of GVA used as ARIO input", "return_period", "long", "lat"])
        cols = col2.union(col1,sort=False)
        res_df.reset_index(inplace=True)
        res_df = res_df[cols]
        res_df.set_index(["final_cluster", "mrio", "sector type"],inplace=True)
        res_df.to_parquet(output/"1_prodloss_full_flood_base_results.parquet")
        scriptLogger.info("#### DONE ####")
    elif args.phase == 2 :
        scriptLogger.info("#### PHASE 2: Finaldemandloss : Read CSVs, properly index them, removing floods impossible to interpolate  ####")
        if not folder.exists():
            raise ValueError("Directory {}, doesn't exist".format(folder))
        else:
            general_df = prepare_general_df(general_csv=folder/"int_general.csv", period=args.period_name, representative_path=args.representative)
            prodloss_df = prepare_loss_df(df_csv=folder/"int_prodloss.csv", period=args.period_name)
            finaldemand_df = prepare_loss_df(df_csv=folder/"int_fdloss.csv", period=args.period_name)

        floodbase_path = pathlib.Path(args.flood_base)
        if not floodbase_path.exists():
            raise ValueError("File {}, doesqn't exist".format(floodbase_path))
        else:
            flood_base_df = pd.read_parquet(floodbase_path)

        flood_base_df = prepare_flood_base(flood_base_df,period=args.period_name)
        if args.period is not None:
            flood_base_df = filter_period(flood_base_df, args.period)

        scriptLogger.info('Found all parquet files')
        # Should not be the case
        flood_base_df = remove_not_sim_region(flood_base_df,general_df)
        regions_list = list(prodloss_df.columns.get_level_values(2).unique())
        flooded_regions = list(flood_base_df["EXIO3_region"].unique())

        scriptLogger.info("#### Doing finalloss result ####")
        scriptLogger.info("Indexing properly, removing too rare to extrapolate floods")
        finaldemand_df = index_a_df(general_df, finaldemand_df, semester_run)
        finaldemand_df = select_semesters(finaldemand_df, args.semester)
        if args.psi:
            finaldemand_df = finaldemand_df[finaldemand_df['psi']==args.psi]
        res_df = extend_df(flood_base_df, finaldemand_df, semester_run)

        if semester_run:
            res_df.set_index(["final_cluster", "mrio", "period", "EXIO3_region", "sector type", "semester"],inplace=True)
            finaldemand_df.set_index(["final_cluster", "mrio", "period", "Impacted EXIO3 region", "sector type", "semester"],inplace=True)
        else:
            res_df.set_index(["final_cluster", "mrio", "period", "Impacted EXIO3 region", "sector type"],inplace=True)
            finaldemand_df.set_index(["final_cluster", "mrio", "period", "Impacted EXIO3 region", "sector type"],inplace=True)
        res_df = res_df.merge(finaldemand_df, how="outer", left_index=True, right_index=True, indicator=True, copy=False)
        sim_df = res_df.loc[res_df._merge=="both"].copy()
        sim_df.rename(columns={"Total direct damage (2010€PPP)_x":"Total direct damage (2010€PPP)", "year_x":"year"}, inplace=True)
        sim_df.reset_index(inplace=True)
        sim_df.drop(["Impacted EXIO3 region","Total direct damage (2010€PPP)_y","year_y"],axis=1,inplace=True)
        res_df.rename(columns={"Total direct damage (2010€PPP)_x":"Total direct damage (2010€PPP)", "year_x":"year"}, inplace=True)
        res_df.reset_index(inplace=True)
        res_df.drop(["Impacted EXIO3 region","Total direct damage (2010€PPP)_y","year_y"],axis=1,inplace=True)
        res_df.drop(res_df.loc[res_df._merge=="both"].index, inplace=True)

        # Remove simulated floods from flood base (we will merge those later)
        flood_base_df = flood_base_df.loc[~flood_base_df.final_cluster.isin(sim_df.final_cluster.unique())].copy()
        scriptLogger.info("Computing regression coefficients")
        if semester_run:
            grouper = ["mrio", "Impacted EXIO3 region", "sector type", "semester"]
        else:
            grouper = ["mrio", "Impacted EXIO3 region", "sector type"]

         # Now we can do this
        finaldemand_df.reset_index(inplace=True)
        finaldemand_df = remove_too_few_flood(finaldemand_df,semester_run)
        finalloss_dict = reg_coef_dict_to_df_to_dict(finaldemand_df, regions=regions_list, grouper=grouper, values="gdp_dmg_share")
        mrios = finaldemand_df.mrio.unique()
        res_df = res_df.set_index("mrio")
        scriptLogger.info("Writing temp result index to {}".format(output))
        res_df.to_parquet(output/"2_fdloss_full_index.parquet")
        scriptLogger.info("Running interpolation")
        if semester_run:
            indexer = ["mrio","semester","final_cluster"]
            semesters = finaldemand_df.semester.unique()
            res_finaldemand_df = run_interpolation_semester(mrios, semesters, flood_base_df, regions_list, finalloss_dict, loss_type_str="fdloss")
        else:
            indexer = ["mrio","final_cluster"]
            res_finaldemand_df = run_interpolation_simple(mrios, flood_base_df, regions_list, finalloss_dict, loss_type_str="fdloss")
        res_finaldemand_df = res_finaldemand_df.stack(level=1).reset_index()
        #res_finaldemand_df.columns = ["_".join(a) for a in res_finaldemand_df.columns.to_flat_index()]
        if semester_run:
            res_finaldemand_df.set_index(["final_cluster", "mrio", "sector type", "semester"],inplace=True)
            res_df.reset_index(inplace=True)
            #sim_df.reset_index(inplace=True)
            res_df.set_index(["final_cluster", "mrio", "sector type", "semester"],inplace=True)
            sim_df.set_index(["final_cluster", "mrio", "sector type", "semester"],inplace=True)
        else:
            res_finaldemand_df.set_index(["final_cluster", "mrio", "sector type"],inplace=True)
            res_df.reset_index(inplace=True)
            #sim_df.reset_index(inplace=True)
            res_df.set_index(["final_cluster", "mrio", "sector type"],inplace=True)
            sim_df.set_index(["final_cluster", "mrio", "sector type"],inplace=True)
        res_df.update(res_finaldemand_df,errors="raise")
        res_df = pd.concat([res_df, sim_df],axis=0)
        col1 = res_df.filter(regex="^[A-Z]{2}$").columns
        col2 = pd.Index(["final_cluster","period","model","EXIO3_region","mrio", "sector type", "semester", "Total direct damage (2010€PPP)", "Population aff (2015 est.)", "dmg_as_direct_prodloss (M€)", "direct_prodloss_as_2015gva_share", "share of GVA used as ARIO input", "return_period", "long", "lat"])
        cols = col2.union(col1,sort=False)
        res_df.reset_index(inplace=True)
        res_df = res_df[cols]
        res_df.set_index(["final_cluster", "mrio", "sector type"],inplace=True)
        scriptLogger.info("Writing temp result to {}".format(output))
        res_df.to_parquet(output/"2_fdloss_full_flood_base_results.parquet")
        scriptLogger.info("#### DONE ####")
    elif args.phase == 3:
        scriptLogger.info("#### PHASE 3: adding flood protection ####")
        scriptLogger.info("prodloss")
        df_path = output/"1_prodloss_full_flood_base_results.parquet"
        flopros_path = pathlib.Path(args.protection_dataframe).resolve()
        outdf = output/"3_prodloss_full_flood_base_results_with_prot.parquet"
        scriptLogger.info('Reading flood df from {}'.format(df_path))
        df = pd.read_parquet(df_path)
        check_df(df)
        scriptLogger.info('Reading flopros df from {}'.format(flopros_path))
        flopros = geopd.read_file(flopros_path)
        check_flopros(flopros)
        scriptLogger.info('geodf from df')
        gdf = gdfy_floods(df)
        scriptLogger.info('Joining with flopros and computing protected floods')
        gdf = geopd.sjoin(gdf,flopros[["MerL_Riv","geometry"]], how="left",predicate="within")
        gdf.drop(["index_right","geometry"],axis=1,inplace=True)
        gdf["protected"] = gdf["return_period"] < gdf["MerL_Riv"]
        #res = join_flopros(gdf,flopros)
        scriptLogger.info('Writing to {}'.format(outdf))
        gdf.to_parquet(outdf)
        scriptLogger.info("fdloss")
        df_path = output/"2_fdloss_full_flood_base_results.parquet"
        outdf = output/"3_fdloss_full_flood_base_results_with_prot.parquet"
        scriptLogger.info('Reading flood df from {}'.format(df_path))
        df = pd.read_parquet(df_path)
        check_df(df)
        scriptLogger.info('geodf from df')
        gdf = gdfy_floods(df)
        scriptLogger.info('Joining with flopros and computing protected floods')
        gdf = geopd.sjoin(gdf,flopros[["MerL_Riv","geometry"]], how="left",predicate="within")
        gdf.drop(["index_right","geometry"],axis=1,inplace=True)
        gdf["protected"] = gdf["return_period"] < gdf["MerL_Riv"]
        #res = join_flopros(gdf,flopros)
        scriptLogger.info('Writing to {}'.format(outdf))
        gdf.to_parquet(outdf)
        scriptLogger.info('Writing to {}'.format(outdf))
        gdf.to_parquet(outdf)
    elif args.phase == 4:
        scriptLogger.info("#### PHASE 4 ####")
        prodloss_df = pd.read_parquet(output/"3_prodloss_full_flood_base_results_with_prot.parquet")
        # n_prot = len(prodloss_df.loc[prodloss_df["protected"]])
        prodloss_df = prodloss_df[~prodloss_df["protected"]]
        regions_list = prodloss_df.filter(regex="^[A-Z]{2}$").columns
        # a regions_list = ['AT', 'AU', 'BE', 'BG', 'BR', 'CA', 'CH', 'CN', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', 'GB', 'GR', 'HR', 'HU', 'ID', 'IE', 'IN', 'IT', 'JP', 'KR', 'LT', 'LU', 'LV', 'MT', 'MX', 'NL', 'NO', 'PL', 'PT', 'RO', 'RU', 'SE', 'SI', 'SK', 'TR', 'TW', 'US', 'WA', 'WE', 'WF', 'WL', 'WM', 'ZA']
        preprepare_for_maps(prodloss_df,"prod",output,regions_list, semester_run)
    elif args.phase == 5:
        scriptLogger.info("#### PHASE 5 ####")
        finaldemand_df = pd.read_parquet(output/"3_fdloss_full_flood_base_results_with_prot.parquet")
        regions_list = finaldemand_df.filter(regex="^[A-Z]{2}$").columns
        finaldemand_df = finaldemand_df[~finaldemand_df["protected"]]
        preprepare_for_maps(finaldemand_df,"final",output, regions_list, semester_run)
    elif args.phase == 6:
        scriptLogger.info("#### PHASE 6 ####")
        scriptLogger.info("Building df for maps")

        df_prod_all_events = pd.read_parquet(output/"4_prodloss_all.parquet")
        prodloss_from_local_events = pd.read_pickle(output/"4_prodloss_local.pkl")
        df_final_demand_all_events = pd.read_parquet(output/"5_fdloss_all.parquet")
        finalloss_from_local_events = pd.read_pickle(output/"5_fdloss_local.pkl")
        df_for_maps = prepare_for_maps(df_prod_all_events, prodloss_from_local_events,df_final_demand_all_events,finalloss_from_local_events,semester_run)
        df_for_maps.to_parquet(output/"6_df_for_maps.parquet")
        scriptLogger.info("Everything finished !")
