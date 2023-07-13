import argparse
import glob
from uf_csv_loader import LoadCsvUf
from pathlib import Path


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="A script to convert a directory of Urban Flows .csv files to .parquet"
    )
    parser.add_argument("-p", "--path", help="Path to the directory containing Urban Flows .csv files")
    parser.add_argument("-s", "--save", help="Path to directory to save results")
    return parser.parse_args()

def csv_loader(path: Path):
    files = glob.glob(path + '/*.csv')
    if len(files) == 0:
        raise FileNotFoundError("Data path provided contains no .csv files")
    for file in files:
        csv_file = LoadCsvUf(file)
        yield csv_file

if __name__ == "__main__":
    args = get_args()
    path_save = Path(args.save)
    path_save.mkdir(parents=True, exist_ok=True)
    for file in csv_loader(args.path):
        file.to_parquet_file(str(path_save / file.path.stem) + ".parquet")


