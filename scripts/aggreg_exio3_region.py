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

cc = coco.CountryConverter()

parser = argparse.ArgumentParser(description='Aggregate an EXIOBASE3 MRIO table in less regions')
parser.add_argument('exio_path', type=str, help='The str path to the exio3 (zip file or already pre-treated pkl file)')
parser.add_argument('regions_aggregator', type=str,
                    help="""A coco (country converter) classification to aggregate to, or the path to the json file with the regions aggregation.
                    Valid classification are : {} \n Not all were tested, use with care and check result !""".format(cc.valid_class))
parser.add_argument('original_mrio_params', type=str, help='A path to the json file of the original mrio parameters file', nargs='?', default=None)
parser.add_argument('-o', "--output", type=str, help='The str path to save the pickled mrio to', nargs='?', default='./mrio_dump')
parser.add_argument('-po', "--params_output", type=str, help='The path to save the new params to')

args = parser.parse_args()
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S")
scriptLogger = logging.getLogger("EXIO3_Region_Aggregator")
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

def most_common(lst):
    return max(set(lst), key=lst.count)

def aggreg(exio_path,  regions_aggregator, old_mrio_params_path, save_path=None, mrio_params_save_path=None):
    scriptLogger.info("Loading region aggregator")
    scriptLogger.info("Make sure you use the same python environment as the one loading the pickle file (especial pymrio and pandas version !)")
    scriptLogger.info("Your current environment is: {}".format(os.environ['CONDA_PREFIX']))
    params=False
    if old_mrio_params_path is not None:
        old_mrio_params_path = pathlib.Path(old_mrio_params_path)
        if not old_mrio_params_path.exists():
            raise FileNotFoundError("Given mrio params file not found - {}".format(old_mrio_params_path))
        else:
            with pathlib.Path(old_mrio_params_path).open('r') as f:
                old_mrio_params = json.load(f)
            params=True


    region_agg = None
    if "json" in regions_aggregator:
        regions_aggregator = pathlib.Path(regions_aggregator)
        if not regions_aggregator.exists():
            raise FileNotFoundError("Region aggregation file not found - {}".format(regions_aggregator))
        else:
            with pathlib.Path(regions_aggregator).open('r') as f:
                region_agg = json.load(f)
            json_agg = True
    else:
        if regions_aggregator not in cc.valid_class:
            raise ValueError("Given aggregator ({}) is not a valid country_converter class, choose one in: {}".format(regions_aggregator,cc.valid_class))
        else:
            json_agg = False

    scriptLogger.info("Done")
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

    original_regions = exio3.get_regions()

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
    exio3 = lexico_reindex(exio3)
    scriptLogger.info("Done")
    if not json_agg:
        region_agg = {region: cc.convert(region, src="EXIO3",to=regions_aggregator) for region in original_regions}

    for k,v in region_agg.items():
        if isinstance(v, list):
            agg = most_common(v)
            scriptLogger.warning("Multiple possible aggregate found for region {} : {}, selecting most common: {}".format(k,set(v),agg))
            region_agg[k] = agg

    regions_aggregator = coco.agg_conc(original_countries=exio3.get_regions(),
                                           aggregates=region_agg)
    exio3.aggregate(region_agg=regions_aggregator)
    exio3.calc_all()
    exio3 = lexico_reindex(exio3)
    name = save_path
    scriptLogger.info("Saving to {}".format(pathlib.Path(name).absolute()))
    with open(name, 'wb') as f:
        pkl.dump(exio3, f)

    if params:
        scriptLogger.info("Generation new mrio params from {}".format(pathlib.Path(old_mrio_params_path).absolute()))
        new_params = old_mrio_params
        scriptLogger.info("Saving these new params to {}".format(pathlib.Path(mrio_params_save_path).absolute()))
        with pathlib.Path(mrio_params_save_path).open('w') as f:
            json.dump(new_params, f, indent=4)
        scriptLogger.info("Done")

if __name__ == '__main__':
    args = parser.parse_args()
    name = pathlib.Path(args.exio_path).stem
    aggreg(args.exio_path, regions_aggregator=args.regions_aggregator, old_mrio_params_path=args.original_mrio_params, save_path=args.output, mrio_params_save_path=args.params_output)
