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

parser = argparse.ArgumentParser(description='Aggregate an EXIOBASE3 MRIO table in less sectors')
parser.add_argument('exio_path', type=str, help='The str path to the exio3 (zip file or already pre-treated pkl file)')
parser.add_argument('aggreg_path', type=str, help='The str path to the ods aggregation matrix file')
parser.add_argument('sector_names_json_path', type=str, help='The str path to the json file with the new names of the sectors')
parser.add_argument('original_mrio_params', type=str, help='A path to the json file of the original mrio parameters file', nargs='?', default=None)
parser.add_argument('-o', "--output", type=str, help='The path to save the pickled mrio to', nargs='?', default='./mrio_dump')
parser.add_argument('-po', "--params_output", type=str, help='The path to save the new params to')

args = parser.parse_args()
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
scriptLogger = logging.getLogger("EXIOBASE3_Sector_Aggregator")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)

scriptLogger.addHandler(consoleHandler)
scriptLogger.setLevel(logging.INFO)
scriptLogger.propagate = False

def new_params_from_old(new_sectors,old_mrio_params,sec_agg_matrix):
    sec_agg_matrix.index = new_sectors
    sector_mapping = {k : [kk for kk in v.keys() if v[kk] == 1] for k,v in sec_agg_matrix.T.to_dict().items()}
    new_mrio_params = old_mrio_params.copy()
    new_mrio_params['capital_ratio_dict'] = {k:0 for k in sector_mapping.keys()}
    for k in new_mrio_params['capital_ratio_dict'].keys():
        values = [old_mrio_params['capital_ratio_dict'][k_old] for k_old in sector_mapping[k]]
        new_mrio_params['capital_ratio_dict'][k] = np.mean(values).round(3)

    new_mrio_params['inventories_dict'] = {k:0 for k in sector_mapping.keys()}

    for k in new_mrio_params['inventories_dict'].keys():
        values = [np.inf if old_mrio_params['inventories_dict'][k_old] == "inf" else old_mrio_params['inventories_dict'][k_old] for k_old in sector_mapping[k]]
        new_mrio_params['inventories_dict'][k] = np.mean(values).round(3)

    return new_mrio_params

def aggreg(exio_path, sector_aggregator_path, new_sectors_name_path, old_mrio_params_path, save_path=None, mrio_params_save_path=None):
    scriptLogger.info("Loading region aggregator")
    scriptLogger.info("Make sure you use the same python environment as the one loading the pickle file (especial pymrio and pandas version !)")
    scriptLogger.info("Your current environment is: {}".format(os.environ['CONDA_PREFIX']))
    params=False
    with pathlib.Path(new_sectors_name_path).open('r') as f:
        a = json.load(f)

    if old_mrio_params_path is not None:
        old_mrio_params_path = pathlib.Path(old_mrio_params_path)
        if not old_mrio_params_path.exists():
            raise FileNotFoundError("Given mrio params file not found - {}".format(old_mrio_params_path))
        else:
            with pathlib.Path(old_mrio_params_path).open('r') as f:
                old_mrio_params = json.load(f)
            params=True

    exio_path = pathlib.Path(exio_path)
    if not exio_path.exists():
        raise FileNotFoundError("Exiobase file not found - {}".format(exio_path))

    if exio_path.suffix == ".zip":
        scriptLogger.info("Parsing EXIOBASE3 from {}".format(exio_path.resolve()))
        exio3 = pym.parse_exiobase3(path=exio_path)
    elif exio_path.suffix == ".pkl":
        with exio_path.open('rb') as f:
            scriptLogger.info("Loading EXIOBASE3 from {}".format(exio_path.resolve()))
            exio3 = pkl.load(f)
    else:
        raise TypeError("File type ({}) not recognize for the script (must be zip or pkl) : {}".format(exio_path.suffix,exio_path.resolve()))

    old_sectors = exio3.get_sectors()
    sec_agg_matrix = pd.read_excel(sector_aggregator_path, sheet_name="input", engine="odf", header=None, names=old_sectors)

    # gain some diskspace and RAM by removing unused attributes
    attr = ['Z', 'Y', 'x', 'A', 'L', 'unit', 'population', 'meta', '__non_agg_attributes__', '__coefficients__', '__basic__']
    tmp = list(exio3.__dict__.keys())
    for at in tmp:
        if at not in attr:
            delattr(exio3,at)
    assert isinstance(exio3, IOSystem)

    scriptLogger.info("Done")
    scriptLogger.info("Computing the IO components")
    exio3.calc_all()
    scriptLogger.info("Done")
    scriptLogger.info("Reading aggregation matrix from sheet 'input' in file {}".format(pathlib.Path(sector_aggregator_path).absolute()))
    scriptLogger.info("Aggregating from {} to {} sectors".format(len(exio3.get_sectors()), sec_agg_matrix.shape[0])) #type:ignore
    exio3.aggregate(sector_agg=sec_agg_matrix.to_numpy())
    scriptLogger.info("Done")
    scriptLogger.info("Renaming sectors from {}".format(pathlib.Path(new_sectors_name_path).absolute()))
    exio3.rename_sectors(a)
    exio3.calc_all()
    scriptLogger.info("Done")
    name = save_path
    scriptLogger.info("Saving to {}".format(pathlib.Path(name).absolute()))
    with open(name, 'wb') as f:
        pkl.dump(exio3, f)

    if params:
        scriptLogger.info("Generation new mrio params from {}".format(pathlib.Path(old_mrio_params_path).absolute()))
        new_params = new_params_from_old(exio3.get_sectors(),old_mrio_params,sec_agg_matrix) #type:ignore
        scriptLogger.info("Saving these new params to {}".format(pathlib.Path(name).absolute()))
        with pathlib.Path(mrio_params_save_path).open('w') as f:
            json.dump(new_params, f, indent=4)
        scriptLogger.info("Done")

if __name__ == '__main__':
    args = parser.parse_args()
    name = pathlib.Path(args.exio_path).stem
    aggreg(args.exio_path, args.aggreg_path, args.sector_names_json_path, old_mrio_params_path=args.original_mrio_params, save_path=args.output, mrio_params_save_path=args.params_output)
