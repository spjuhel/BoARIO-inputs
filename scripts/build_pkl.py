import os
import pathlib
import pymrio as pym
import pandas as pd
import pickle as pkl
import logging
import argparse
import json
import country_converter as coco
from pymrio.core.mriosystem import IOSystem
import numpy as np

parser = argparse.ArgumentParser(description='Build a pkl EXIOBASE3 MRIO table from a zip file')
parser.add_argument('exio_path', type=str, help='The str path to the exio3 (zip file or already pre-treated pkl file)')
parser.add_argument('-o', "--output", type=str, help='The path to save the pickled mrio to', nargs='?', default='./mrio_dump')

args = parser.parse_args()
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
scriptLogger = logging.getLogger("EXIOBASE3_Sector_Aggregator")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)

scriptLogger.addHandler(consoleHandler)
scriptLogger.setLevel(logging.INFO)
scriptLogger.propagate = False

def lexico_reindex(mrio: pym.IOSystem) -> pym.IOSystem:
    """Reindex IOSystem lexicographicaly

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

    mrio.Z = mrio.Z.reindex(sorted(mrio.Z.index), axis=0)
    mrio.Z = mrio.Z.reindex(sorted(mrio.Z.columns), axis=1)
    mrio.Y = mrio.Y.reindex(sorted(mrio.Y.index), axis=0)
    mrio.Y = mrio.Y.reindex(sorted(mrio.Y.columns), axis=1)
    mrio.x = mrio.x.reindex(sorted(mrio.x.index), axis=0) #type: ignore
    mrio.A = mrio.A.reindex(sorted(mrio.A.index), axis=0)
    mrio.A = mrio.A.reindex(sorted(mrio.A.columns), axis=1)

    return mrio

def aggreg(exio_path, save_path=None):
    scriptLogger.info("Make sure you use the same python environment as the one loading the pickle file (especial pymrio and pandas version !)")
    scriptLogger.info("Your current environment is: {}".format(os.environ['CONDA_PREFIX']))
    params=False
    exio_path = pathlib.Path(exio_path)
    if not exio_path.exists():
        raise FileNotFoundError("Exiobase file not found - {}".format(exio_path))

    if exio_path.suffix == ".zip":
        scriptLogger.info("Parsing EXIOBASE3 from {}".format(exio_path.resolve()))
        exio3 = pym.parse_exiobase3(path=exio_path)
    else:
        raise TypeError("File type ({}) not recognize for the script (must be zip or pkl) : {}".format(exio_path.suffix,exio_path.resolve()))

    scriptLogger.info("Removing unecessary IOSystem attributes")
    attr = ['Z', 'Y', 'x', 'A', 'L', 'unit', 'population', 'meta', '__non_agg_attributes__', '__coefficients__', '__basic__']
    tmp = list(exio3.__dict__.keys())
    for at in tmp:
        if at not in attr:
            delattr(exio3,at)
    assert isinstance(exio3, IOSystem)
    scriptLogger.info("Done")
    scriptLogger.info("Computing the missing IO components")
    exio3.calc_all()
    scriptLogger.info("Done")
    scriptLogger.info("Reindexing lexicographicaly")
    exio3 = lexico_reindex(exio3)
    scriptLogger.info("Done")
    name = save_path
    scriptLogger.info("Saving to {}".format(pathlib.Path(name).absolute()))
    exio3 = lexico_reindex(exio3)
    with open(name, 'wb') as f:
        pkl.dump(exio3, f)

if __name__ == '__main__':
    args = parser.parse_args()
    name = pathlib.Path(args.exio_path).stem
    aggreg(args.exio_path, save_path=args.output)
