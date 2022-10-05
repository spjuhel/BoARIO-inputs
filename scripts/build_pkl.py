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

EXIO3_TYPES = ["exio3", "EXIO3", "exiobase", "exiobase3", "EXIOBASE", "EXIOBASE3"]
OECD_TYPES = ["oecd", "OECD", "icio", "ICIO", "oecd-icio", "OECD-ICIO"]

ALL_MRIO_TYPES = EXIO3_TYPES + OECD_TYPES

logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
scriptLogger = logging.getLogger("MRIO PKL File builder")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)

scriptLogger.addHandler(consoleHandler)
scriptLogger.setLevel(logging.INFO)
scriptLogger.propagate = False

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

parser = argparse.ArgumentParser(description='Build a pkl MRIO table from a zip file. Consider the zip an EXIOBASE3 by default.')
parser.add_argument("-i", '--mrio-path', type=str, help='The str path to the mrio (zip file or already pre-treated pkl file)', required=True)
parser.add_argument("-t", '--mrio-type', type=str, help='The type of the mrio', default="EXIO3")
parser.add_argument("-y", '--year', type=int, help='The year to use (when input is a folder with multiple years)', default=None)
parser.add_argument('-o', "--output", type=str, help='The path to save the pickled mrio to', nargs='?', default='./mrio_dump')
args = parser.parse_args()

if __name__ == '__main__':
    args = parser.parse_args()
    aggreg(args.mrio_path, mrio_type=args.mrio_type, year=args.year, save_path=args.output)
