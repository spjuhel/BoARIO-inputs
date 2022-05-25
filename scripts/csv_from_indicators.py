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
from typing import Iterable
import pandas as pd
import pathlib
import argparse
import logging

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

def produce_general_csv(folder,run_type,save_path):
    future_df = []
    files = list(folder.glob('**/*'+run_type+'*/indicators.json'))
    scriptLogger.info('Found {} indicators files to regroup'.format(len(files)))
    for ind in files:
        with ind.open('r') as f:
            dico = json.load(f)

        dico['run_name'] = ind.parent.name
        if isinstance(dico['region'],list) and len(dico['region'])==1:
            dico['region'] = dico['region'][0]
        ##################################
        # for k,v in dico.items():       #
        #     if isinstance(v,Iterable): #
        #         dico[k]=str(v)         #
        # print(dico)                    #
        # df = pd.DataFrame(dico)        #
        # print(df)                      #
        # if future_df is None:          #
        #     future_df= df.copy()       #
        # else:                          #
        ##################################
        future_df.append(dico)
    future_df = pd.DataFrame(future_df)
    future_df=future_df.set_index("run_name")
    future_df.to_csv(save_path)

def produce_region_prod_loss_csv(folder,run_type,save_path):
    future_df = None
    for ind in folder.glob('**/*'+run_type+'*/prod_chg.json'):
        if "RoW" in ind.parent.name:
            pass
        else:
            with ind.open('r') as f:
                js = json.load(f)

            df = deserialize_multiindex_dataframe(js)
            df.rename_axis("run_name",axis=0, inplace=True)
            df.rename_axis(["sector type","region"],axis=1, inplace=True)
            if future_df is None:
                future_df = df.copy()
            else:
                future_df = pd.concat([future_df,df])

    future_df=future_df.set_index("run_name")
    future_df.to_csv(save_path)

def produce_region_fd_loss_csv(folder,run_type,save_path):
    future_df = None
    for ind in folder.glob('**/*'+run_type+'*/fd_loss.json'):
        if "RoW" in ind.parent.name:
            pass
        else:
            with ind.open('r') as f:
                js = json.load(f)

            df = deserialize_multiindex_dataframe(js)
            df.rename_axis("run_name",axis=0, inplace=True)
            df.rename_axis(["sector type","region"],axis=1, inplace=True)
            if future_df is None:
                future_df = df.copy()
            else:
                future_df = pd.concat([future_df,df])
    future_df=future_df.set_index("run_name")
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
    produce_general_csv(folder,args.run_type,save_path=args.output+"/"+runtype+"general.csv")
    produce_region_prod_loss_csv(folder,args.run_type,save_path=args.output+"/"+runtype+"prodloss.csv")
    produce_region_fd_loss_csv(folder,args.run_type,save_path=args.output+"/"+runtype+"fdloss.csv")
