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

import os
from pathlib import Path
import numpy as np
import pandas as pd
import pickle
from datetime import datetime
import re
import country_converter as coco
import matplotlib.pyplot as plt
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
from tqdm.notebook import tqdm
import json
import pathlib
import geopandas as geopd
import logging
import argparse

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

parser = argparse.ArgumentParser(description="Add protection column to flood dataframe")
parser.add_argument('-B', "--flood-dataframe", type=str, help='Path where the flood database is.', required=True)
parser.add_argument('-P', "--protection-dataframe", type=str, help='Path where the protection database is.', required=True)
parser.add_argument('-o', "--output", type=str, help='Where to save the output.', required=True)

if __name__ == '__main__':
    args = parser.parse_args()
    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
    scriptLogger = logging.getLogger("Add protection column")
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    scriptLogger.addHandler(consoleHandler)
    scriptLogger.setLevel(logging.INFO)
    scriptLogger.propagate = False
    scriptLogger.info('Starting Script')
    df_path = pathlib.Path(args.flood_dataframe).resolve()
    flopros_path = pathlib.Path(args.protection_dataframe).resolve()
    output = pathlib.Path(args.output)
    scriptLogger.info('Reading flood df from {}'.format(df_path))
    df = pd.read_parquet(df_path)
    check_df(df)
    scriptLogger.info('Reading flopros df from {}'.format(flopros_path))
    flopros = geopd.read_file(flopros_path)
    check_flopros(flopros)
    scriptLogger.info('geodf from df')
    gdf = gdfy_floods(df)
    scriptLogger.info('Joining with flopros and computing protected floods')
    res = join_flopros(gdf,flopros)
    scriptLogger.info('Writing to {}'.format(output))
    res.to_parquet(output)
