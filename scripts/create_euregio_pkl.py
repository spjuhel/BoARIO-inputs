from __future__ import annotations
import pandas as pd
from pathlib import Path
import argparse
from collections.abc import Sequence
import pickle
import logging
import os
import pymrio


def build_from_folder(folder, year, inv_treatment=True):
    folder = Path(folder).resolve()
    cols_z = [2, 5] + [i for i in range(6, 3730, 1)]
    ioz = pd.read_csv(
        folder / f"EURegionalIOtable_{year}.csv",
        index_col=[0, 1],
        usecols=cols_z,
        engine="c",
        names=None,
        header=None,
        skiprows=8,
        nrows=3724,
        decimal=".",
        low_memory=False,
    )
    ioz.rename_axis(index=["region", "sector"], inplace=True)
    ioz.columns = ioz.index
    ioz.fillna(value=0.0, inplace=True)

    cols_y = [3733, 3736] + [i for i in range(3737, 3737 + 1064, 1)]
    fd_index = pd.read_csv(folder / "fd_index.csv", usecols=[0, 1, 2, 3]).columns
    ioy = pd.read_csv(
        folder / f"EURegionalIOtable_{year}.csv",
        index_col=[0, 1],
        usecols=cols_y,
        engine="c",
        names=None,
        header=None,
        skiprows=8,
        nrows=3724,
        decimal=".",
        low_memory=False,
    )
    ioy.rename_axis(index=["region", "sector"], inplace=True)
    ioy.columns = pd.MultiIndex.from_product(
        [ioy.index.get_level_values(0).unique(), fd_index]
    )
    ioy.fillna(value=0.0, inplace=True)

    iova = pd.read_csv(
        folder / f"EURegionalIOtable_{year}.csv",
        index_col=[5],
        engine="c",
        header=[0, 3],
        skiprows=3735,
        nrows=6,
        decimal=".",
        low_memory=False,
    )
    iova.rename_axis(index=["va_cat"], inplace=True)
    iova.fillna(value=0.0, inplace=True)
    iova.drop(iova.iloc[:, :5].columns, axis=1, inplace=True)
    iova.drop(iova.iloc[:, 3724:].columns, axis=1, inplace=True)

    # ioz = ioz.rename_axis(["region","sector"])
    # ioz = ioz.rename_axis(["region","sector"],axis=1)

    ioy = ioy.rename_axis(["region", "sector"])
    ioy = ioy.rename_axis(["region", "category"], axis=1)
    if inv_treatment:
        invs = ioy.loc[:, (slice(None), "Inventory_adjustment")].sum(axis=1)
        invs.name = "Inventory_use"
        invs_neg = pd.DataFrame(-invs).T
        invs_neg[invs_neg < 0] = 0
        iova = pd.concat([iova, invs_neg], axis=0)
        ioy = ioy.clip(lower=0)

    return ioz, ioy, iova


def dir_path(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(f"readable_dir:{path} is not a valid path")


logFormatter = logging.Formatter(
    "%(asctime)s [%(levelname)-5.5s] %(name)s %(message)s", datefmt="%H:%M:%S"
)
scriptLogger = logging.getLogger("build_euregio_from_csv")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
scriptLogger.addHandler(consoleHandler)
scriptLogger.setLevel(logging.INFO)
# scriptLogger.propagate = False


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build euregio pkl file from csvs")
    parser.add_argument(
        "-i",
        "--input-folder",
        type=dir_path,
        help="The path to the folder with EUREGIO CSVs",
        required=True,
    )
    parser.add_argument(
        "-y", "--year", type=int, help="The year to parse/build", required=True
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="The name of the pkl file to create",
        required=True,
    )
    parser.add_argument(
        "-I",
        "--no-inventory",
        action="store_true",
        help="if set, doesn't swap negative values in inventory_adjustment to VA",
    )
    args = parser.parse_args(argv)

    scriptLogger.info(
        "Make sure you use the same python environment as the one loading the pickle file (especial pymrio and pandas version !)"
    )
    scriptLogger.info(
        "Your current environment is: {} (assuming conda/mamba)".format(
            os.environ["CONDA_PREFIX"]
        )
    )
    folder = args.input_folder
    year = args.year
    output = args.output
    inventory = not args.no_inventory
    if inventory:
        unitsize = 5
    else:
        unitsize = 4
    ioz, ioy, iova = build_from_folder(folder, year, inv_treatment=inventory)
    euregio = pymrio.IOSystem(
        Z=ioz,
        Y=ioy,
        year=year,
        unit=pd.DataFrame(
            data=["2010_€_MILLIONS"] * unitsize, index=iova.index, columns=["unit"]
        ),
    )
    with Path(output).open("wb") as f:
        pickle.dump(euregio, f)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
