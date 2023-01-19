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

import json
import re
from typing import Iterable
import pandas as pd
import pathlib
import argparse
import logging

NAME_RE = r"(?P<xp_name>[a-zA-Z\-\d]+)~(?P<mrio_name>[a-zA-Z\-_\d]+)~(?P<params>psi_(?P<psi>(?:0|1)_\d+)_order_(?P<order>[a-z]+)_inv_(?P<inv>\d+)_reb_(?P<reb>\d+)_evtype_(?P<evtype>[a-zA-Z]+))~(?P<region>[A-Z]+)~(?P<class>(?:\d+%)|max|min).name"

parser = argparse.ArgumentParser(description="Produce csv from json indicators")
parser.add_argument('folder', type=str, help='The str path to the main folder')
parser.add_argument('run_type', type=str, help='The type of runs to produce csv from ("raw", "int" or "all")')
parser.add_argument('-o', "--output", type=str, help='Path where to save csv')

def deserialize_multiindex_dataframe(dataframe_json: dict) -> pd.DataFrame:
    """Deserialize the dataframe json into a dataframe object.
    The dataframe json must be generated with DataFrame.to_json(orient="split")
    This function is to address the fact that `pd.read_json()` isn't behaving correctly (yet)
    https://github.com/pandas-dev/pandas/issues/4889
    """
    def convert_index(json_obj):
        to_tuples = [tuple(i) if isinstance(i, list) else i for i in json_obj]
        if all(isinstance(i, list) for i in json_obj):
            return pd.MultiIndex.from_tuples(to_tuples)
        else:
            return pd.Index(to_tuples)
    json_dict = dataframe_json
    columns = convert_index(json_dict['columns'])
    index = convert_index(json_dict['index'])
    dataframe = pd.DataFrame(json_dict["data"], index, columns)
    return dataframe

def produce_general_csv(folder,save_path):
    future_df = []
    files = list(folder.glob('**/*/indicators.json'))
    names =list(folder.glob('**/*/*.name'))
    if len(files) == 0:
        raise ValueError(
            """No indicators file found. Perhaps folder is mistyped ? :
            Looked in {}
            """.format(folder))
    scriptLogger.info('Found {} indicators files to regroup'.format(len(files)))
    for ind,name in zip(files,names):
        # flood-dottori-test_exiobase3_2011_74_sectors_psi_0_90_order_alt_inv_60_reb_60_evtype_recover_AU_1%.name
        name_re = re.compile(NAME_RE)
        match = name_re.match(name.name)
        if match is None:
            raise ValueError("Simulation name error : {}".format(name.name))
        with ind.open('r') as f:
            dico = json.load(f)

        dico["Experience"] = match["xp_name"]
        dico["MRIO"] = match["mrio_name"]
        dico['Impacting flood percentile'] = match["class"]
        dico['mrio_region'] = match["region"]
        dico['Run Parameters'] = match["params"]
        dico['psi'] = match['psi']
        dico['order type'] = match['order']
        dico['inv tau'] = match['inv']
        dico['reb tau'] = match['reb']
        dico["run_id"] = match["mrio_name"] + "_" + match["params"] + "_" + match["region"] + "_" + match["class"]
        if isinstance(dico['region'],list) and len(dico['region'])==1:
            dico['region'] = dico['region'][0]
        future_df.append(dico)
    future_df = pd.DataFrame(future_df)
    future_df=future_df.set_index("run_id")
    future_df.to_csv(save_path)

def produce_region_loss_csv(folder,save_path, jsontype):
    future_df = None
    files = list(folder.glob('**/*/'+jsontype+'.json'))
    names =list(folder.glob('**/*/*.name'))
    for ind,name in zip(files,names) :
        name_re = re.compile(NAME_RE)
        match = name_re.match(name.name)
        if match is None:
            raise ValueError("Simulation name error : {}".format(name.name))

        with ind.open('r') as f:
            js = json.load(f)

        #js["index"] = [[ind.parent.parent.name,js["index"][0]]]
        df = deserialize_multiindex_dataframe(js)
        df["run_id"] = match["mrio_name"] + "_" + match["params"] + "_" + match["region"] + "_" + match["class"]
        df.rename_axis(["affected region"],axis=0, inplace=True)
        if df.columns.nlevels == 2:
            df.rename_axis(["sector type","impacted region"],axis=1, inplace=True)
        elif df.columns.nlevels ==3:
            df.rename_axis(["semester", "sector type","impacted region"],axis=1, inplace=True)
        else:
            raise ValueError("Dataframe columns have {} levels (2 or 3 expected)".format(df.columns.nlevels))
        if future_df is None:
            future_df = df.copy()
        else:
            future_df = pd.concat([future_df,df])

    #print(future_df.reset_index())
    future_df=future_df.set_index("run_id")
    future_df.to_csv(save_path)

if __name__ == '__main__':
    args = parser.parse_args()
    folder = pathlib.Path(args.folder).resolve()
    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
    scriptLogger = logging.getLogger("indicators_batch")
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)

    if args.run_type == "raw":
        runtype = "raw_"
    elif args.run_type == "int":
        runtype = "int_"
    elif args.run_type == "all":
        runtype="all_"
    else:
        scriptLogger.warning('Unrecognized run type.')
        parser.print_usage()
        exit()

    scriptLogger.addHandler(consoleHandler)
    scriptLogger.setLevel(logging.INFO)
    scriptLogger.propagate = False
    scriptLogger.info('Starting Script')
    scriptLogger.info('Will produce regrouped indicators for folder {}'.format(folder))
    produce_general_csv(folder,save_path=args.output+"/"+runtype+"general.csv")
    produce_region_loss_csv(folder,save_path=args.output+"/"+runtype+"prodloss.csv",jsontype="prod_chg")
    produce_region_loss_csv(folder,save_path=args.output+"/"+runtype+"fdloss.csv", jsontype="fd_loss")
