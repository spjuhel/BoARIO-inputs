import os
import pathlib
import re
import pymrio as pym
import pandas as pd
import pickle as pkl
import logging
import argparse
import json
import country_converter as coco
from pymrio.core.mriosystem import IOSystem

cc = coco.CountryConverter()

parser = argparse.ArgumentParser(description='Aggregate an EXIOBASE3 MRIO table in less regions')
parser.add_argument('exio_path', type=str, help='The str path to the exio3 (zip file or already pre-treated pkl file)')
parser.add_argument('subregions_target', type=str,
                    help="""Subregions to generate. Format is REGION_sliced_in_N""")
parser.add_argument('original_mrio_params', type=str, help='A path to the json file of the original mrio parameters file', nargs='?', default=None)
parser.add_argument('-o', "--output", type=str, help='The str path to save the pickled mrio to', nargs='?', default='./mrio_dump')
parser.add_argument('-po', "--params_output", type=str, help='The path to save the new params to')

args = parser.parse_args()
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
scriptLogger = logging.getLogger("EXIO3_Subregion_generator")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)

scriptLogger.addHandler(consoleHandler)
scriptLogger.setLevel(logging.INFO)
scriptLogger.propagate = False

def _split_region_df_Z(df_name:str, mrio_in: pym.IOSystem, region: str, split_number: int = 2, internal_exchange: bool = False) -> pd.DataFrame:
    """Split a region in Z dataframe in `split_numer` sub-regions

    """
    if not hasattr(mrio_in, df_name):
        raise ValueError("DataFrame '{}' was not found in the MRIO".format(df_name))
    if region not in mrio_in.get_regions():
        raise ValueError("region '{}' was not found in the MRIO".format(region))
    idx = pd.IndexSlice
    mrio = mrio_in.copy()
    df = getattr(mrio,df_name)
    df.loc[:,idx[region,:]] = df.loc[:,idx[region,:]] / split_number
    df = df.rename(columns={region:region+'_1'})
    copies = [(df.loc[:,idx[region+"_1",:]]).rename(columns={region+"_1":region+'_'+str(i+2)}) for i in range(split_number-1)]
    df = df.join(copies)
    df = df.reindex(sorted(df.columns),axis=1)

    df = df.T
    df.loc[:,idx[region,:]] = df.loc[:,idx[region,:]] / split_number
    df = df.rename(columns={region:region+'_1'})
    copies = [(df.loc[:,idx[region+"_1",:]]).rename(columns={region+"_1":region+'_'+str(i+2)}) for i in range(split_number-1)]
    df = df.join(copies)
    df = df.reindex(sorted(df.columns),axis=1)
    df = df.T

    if not internal_exchange:
        for i in range(split_number):
            df.loc[idx[region+"_"+str(i+1),:],idx[region+"_"+str(i+1),:]] = df.loc[idx[region+"_"+str(i+1),:],idx[region+"_"+str(i+1),:]] * split_number
            for j in range(split_number):
                if i!=j:
                    df.loc[idx[region+"_"+str(i+1),:],idx[region+"_"+str(j+1),:]] = 0
    return df

def _split_region_df_A(df_name:str, mrio_in: pym.IOSystem, region: str, split_number: int = 2, internal_exchange: bool = False) -> pd.DataFrame:
    """Split a region dataframe in two sub-region

    """
    if not hasattr(mrio_in, df_name):
        raise ValueError("DataFrame '{}' was not found in the MRIO".format(df_name))
    if region not in mrio_in.get_regions():
        raise ValueError("region '{}' was not found in the MRIO".format(region))
    idx = pd.IndexSlice
    mrio = mrio_in.copy()
    df = getattr(mrio,df_name)
    df = df.T
    df.loc[:,idx[region,:]] = df.loc[:,idx[region,:]] / split_number
    df = df.rename(columns={region:region+'_1'})
    copies = [(df.loc[:,idx[region+"_1",:]]).rename(columns={region+"_1":region+'_'+str(i+2)}) for i in range(split_number-1)]
    df = df.join(copies)
    df = df.reindex(sorted(df.columns),axis=1)
    df = df.T

    df = df.rename(columns={region:region+'_1'})
    copies = [(df.loc[:,idx[region+"_1",:]]).rename(columns={region+"_1":region+'_'+str(i+2)}) for i in range(split_number-1)]
    df = df.join(copies)
    df = df.reindex(sorted(df.columns),axis=1)

    if not internal_exchange:
        for i in range(split_number):
            df.loc[idx[region+"_"+str(i+1),:],idx[region+"_"+str(i+1),:]] = df.loc[idx[region+"_"+str(i+1),:],idx[region+"_"+str(i+1),:]] * split_number
            for j in range(split_number):
                if i!=j:
                    df.loc[idx[region+"_"+str(i+1),:],idx[region+"_"+str(j+1),:]] = 0
    return df

def _split_region_df_xY_shape(df_name:str, mrio_in: pym.IOSystem, region: str, split_number: int = 2) -> pd.DataFrame:
    if not hasattr(mrio_in, df_name):
        raise ValueError("DataFrame '{}' was not found in the MRIO".format(df_name))
    if region not in mrio_in.get_regions():
        raise ValueError("Region '{}' was not found in the MRIO".format(region))
    idx = pd.IndexSlice
    mrio = mrio_in.copy()
    df = getattr(mrio,df_name)
    df = df.T
    df.loc[:,idx[region,:]] = df.loc[:,idx[region,:]] / split_number
    df = df.rename(columns={region:region+'_1'})
    copies = [(df.loc[:,idx[region+"_1",:]]).rename(columns={region+"_1":region+'_'+str(i+2)}) for i in range(split_number-1)]
    df = df.join(copies)
    df = df.reindex(sorted(df.columns),axis=1)
    df = df.T
    return df

def split_region(mrio_in: pym.IOSystem, region: str, split_number: int = 2, internal_exchange: bool = False):
    mrio = mrio_in.copy()
    mrio.Z = _split_region_df_Z("Z", mrio_in, region, split_number, internal_exchange)
    mrio.A = _split_region_df_A("A", mrio_in, region, split_number, internal_exchange)
    mrio.x = _split_region_df_xY_shape("x", mrio_in, region, split_number)
    mrio.Y = _split_region_df_xY_shape("Y", mrio_in, region, split_number)
    return mrio

if __name__ == '__main__':
    args = parser.parse_args()
    name = pathlib.Path(args.exio_path).stem
    subregion = re.compile("(?P<region>[A-Z]{2})_sliced_in_(?P<split_number>[0-9]+)")
    match = subregion.match(args.subregions_target)
    if match is None:
        scriptLogger.warning("Subregions target '{}' didn't match regex {}".format(args.subregions_target,subregion))
    region=match['region']
    split_number=int(match['split_number'])
    with pathlib.Path(args.exio_path).open('rb') as f:
        mrio_in = pkl.load(f)
    mrio_out = split_region(mrio_in, region, split_number)
    name = args.output
    name = pathlib.Path(name).absolute()
    scriptLogger.info("Saving to {}".format(name))
    with open(name, 'wb') as f:
        pkl.dump(mrio_out, f)
    if args.original_mrio_params is not None:
        original_mrio_params_path = pathlib.Path(args.original_mrio_params)
        if not original_mrio_params_path.exists():
            raise FileNotFoundError("Given mrio params file not found - {}".format(original_mrio_params_path))
        else:
            with pathlib.Path(original_mrio_params_path).open('r') as f:
                old_mrio_params = json.load(f)
        with pathlib.Path(args.params_output).open('w') as f:
            json.dump(old_mrio_params, f, indent=4)
