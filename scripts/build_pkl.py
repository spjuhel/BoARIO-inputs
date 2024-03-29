import os
import pathlib
from typing import Optional
import pymrio as pym
import pandas as pd
import pickle as pkl
import logging
import argparse
import json
import country_converter as coco
from pymrio.core.mriosystem import IOSystem
import numpy as np
from pymrio.tools.ioparser import ParserError
from pathlib import Path

EXIO3_TYPES = ["exio3", "EXIO3", "exiobase", "exiobase3", "EXIOBASE", "EXIOBASE3"]
OECD_TYPES = ["oecd", "OECD", "icio", "ICIO", "oecd-icio", "OECD-ICIO"]
EUREGIO_TYPES = ["euregio", "EUREGIO"]
ALL_MRIO_TYPES = EXIO3_TYPES + OECD_TYPES + EUREGIO_TYPES
REGIONS_RENAMING = {"DEE1":"DEE0","DEE2":"DEE0","DEE3":"DEE0"}

def load_euregio(path:Path,year,inv_treatment=True):
    ioz_path = path/f"Z_{year}.csv"
    iova_path = path/f"VA_{year}.csv"
    ioy_path = path/f"Y_{year}.csv"
    regions_path = path/"regions_index.csv"
    sectors_path = path/"sectors.csv"
    va_path = path/f"va_index.csv"
    fd_path = path/f"fd_index.csv"

    regions_csv = pd.read_csv(regions_path)
    regions_names = regions_csv.loc[0,:].unique()
    sectors_csv = pd.read_csv(sectors_path)
    sectors_names = sectors_csv.loc[0,:].unique()

    fd_csv = pd.read_csv(fd_path, header=None)
    fd_names = fd_csv.loc[0,:].unique()

    va_csv = pd.read_csv(va_path, header=None)
    va_names = va_csv.iloc[:,0].unique()

    IO_index = pd.MultiIndex.from_product([regions_names,sectors_names])
    IOZ = pd.read_csv(ioz_path, names=IO_index, decimal=",")
    IOZ.index = IO_index

    IOVA = pd.read_csv(iova_path, names=IO_index, decimal=",")
    IOVA.index = va_names

    Y_columns = pd.MultiIndex.from_product([regions_names,fd_names])
    IOY = pd.read_csv(ioy_path, names=Y_columns, decimal=",")
    IOY.index = IO_index

    IOZ = IOZ.fillna(0)
    IOVA = IOVA.fillna(0)
    IOY = IOY.fillna(0)

    IOZ = IOZ.rename_axis(["region","sector"])
    IOZ = IOZ.rename_axis(["region","sector"],axis=1)
    IOY = IOY.rename_axis(["region","sector"])
    IOY = IOY.rename_axis(["region","category"],axis=1)

    if inv_treatment:
        invs = IOY.loc[:,(slice(None),"Inventory_adjustment")].sum(axis=1)
        invs.name = "Inventory_use"
        invs_neg = pd.DataFrame(-invs).T
        invs_neg[invs_neg<0] = 0
        IOVA = pd.concat([IOVA,invs_neg],axis=0)
        IOY = IOY.clip(lower=0)

    return IOZ, IOY, IOVA

def makeIOSys(IOZ,IOY,IOVA,year):
    euregio = pym.IOSystem(Z=IOZ, Y=IOY,year=year, unit=pd.DataFrame(data = ['2010_€_MILLIONS']*5, index = IOVA.index, columns = ['unit']))
    factor_input = pym.Extension(name = 'VA', F=IOVA)
    euregio.factor_input = factor_input
    euregio.calc_all()
    return euregio

def correct_regions(euregio:pym.IOSystem):
    euregio.rename_regions(REGIONS_RENAMING).aggregate_duplicates()
    return euregio

def lexico_reindex(mrio: pym.IOSystem) -> pym.IOSystem:
    """Re-index IOSystem lexicographicaly

    Sort indexes and columns of the dataframe of a :ref:`pymrio.IOSystem` by
    lexical order.

    Parameters
    ----------
    mrio : pym.IOSystem
        The IOSystem to sort

    Returns
    -------
    pym.IOSystem
        The sorted IOSystem

    """
    for attr in ["Z", "Y", "x", "A"]:
        if getattr(mrio,attr) is None:
            raise ValueError("Attribute {} is None, did you forget to calc_all() the MRIO ?".format(attr))
    mrio.Z = mrio.Z.reindex(sorted(mrio.Z.index), axis=0)#type: ignore
    mrio.Z = mrio.Z.reindex(sorted(mrio.Z.columns), axis=1)#type: ignore
    mrio.Y = mrio.Y.reindex(sorted(mrio.Y.index), axis=0)#type: ignore
    mrio.Y = mrio.Y.reindex(sorted(mrio.Y.columns), axis=1)#type: ignore
    mrio.x = mrio.x.reindex(sorted(mrio.x.index), axis=0) #type: ignore
    mrio.A = mrio.A.reindex(sorted(mrio.A.index), axis=0)#type: ignore
    mrio.A = mrio.A.reindex(sorted(mrio.A.columns), axis=1)#type: ignore

    return mrio

def aggreg(mrio_path, mrio_type, year:Optional[int], save_path:os.PathLike):
    scriptLogger.info("Make sure you use the same python environment as the one loading the pickle file (especial pymrio and pandas version !)")
    scriptLogger.info("Your current environment is: {}".format(os.environ['CONDA_PREFIX']))
    mrio_path = pathlib.Path(mrio_path)
    if not mrio_path.exists():
        raise FileNotFoundError("MRIO file not found - {}".format(mrio_path))

    scriptLogger.info("Parsing MRIO from {}".format(mrio_path.resolve()))
    if mrio_type in EXIO3_TYPES:
        mrio_pym = pym.parse_exiobase3(path=mrio_path)
    elif mrio_type in OECD_TYPES:
        if mrio_path.is_dir():
            if not year:
                raise ValueError("Trying to parse OECD MRIO with a folder, but no year given. Please specify a year with -y or --year option.")
            else:
                mrio_pym = pym.parse_oecd(path=mrio_path, year=year)
        elif mrio_path.suffix == ".csv":
            mrio_pym = pym.parse_oecd(path=mrio_path)
        elif mrio_path.suffix == ".zip":
            mrio_pym = pym.parse_oecd(path=mrio_path)
        else:
            raise ValueError("MRIO file ({}) not recognized as valid (should be a directory, a .csv or .zip)".format(mrio_path))
    else:
        raise ValueError("MRIO type ({}) not recognized. Possible types currently : {}".format(mrio_type,ALL_MRIO_TYPES))

    scriptLogger.info("Removing unnecessary IOSystem attributes")
    attr = ['Z', 'Y', 'x', 'A', 'L', 'unit', 'population', 'meta', '__non_agg_attributes__', '__coefficients__', '__basic__']
    tmp = list(mrio_pym.__dict__.keys())
    for at in tmp:
        if at not in attr:
            delattr(mrio_pym,at)
    assert isinstance(mrio_pym, IOSystem)
    scriptLogger.info("Done")
    scriptLogger.info("Computing the missing IO components")
    mrio_pym.calc_all()
    scriptLogger.info("Done")
    scriptLogger.info("Re-indexing lexicographicaly")
    mrio_pym = lexico_reindex(mrio_pym)
    scriptLogger.info("Done")
    save_path = pathlib.Path(save_path)
    scriptLogger.info("Saving to {}".format(save_path.absolute()))
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with open(save_path, 'wb') as f:
        pkl.dump(mrio_pym, f)

logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
scriptLogger = logging.getLogger("MRIO PKL File builder")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)

scriptLogger.addHandler(consoleHandler)
scriptLogger.setLevel(logging.INFO)
scriptLogger.propagate = False

parser = argparse.ArgumentParser(description='Build a pkl MRIO table from a zip file. Considers the zip an EXIOBASE3 by default.')
parser.add_argument("-i", '--mrio-path', type=str, help='The str path to the mrio (zip file or already pre-treated pkl file)', required=True)
parser.add_argument("-t", '--mrio-type', type=str, help='The type of the mrio', default="EXIO3")
parser.add_argument("-y", '--year', type=int, help='The year to use (when input is a folder with multiple years)', default=None)
parser.add_argument('-o', "--output", type=str, help='The path to save the pickled mrio to', nargs='?', default='./mrio_dump')
args = parser.parse_args()

if __name__ == '__main__':
    args = parser.parse_args()
    aggreg(args.mrio_path, mrio_type=args.mrio_type, year=args.year, save_path=args.output)
