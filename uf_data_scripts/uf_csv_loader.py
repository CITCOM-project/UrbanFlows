from pathlib import Path
import pandas as pd
import csv
import re


class LoadCsvUf:
    """
    Class to load CSV files from the Urban Flows Observatory into different open formats
    """

    def __init__(self, file_path: str):
        self.path = Path(file_path)
        self.columns = list()
        self.num_tables = None

    def to_dataframe(self):
        self._get_metadata()
        df = pd.read_csv(self.path, skiprows=self.data_line_count, names=self.columns)
        df = df.set_index('time')
        return df

    def to_parquet_file(self, path: str):
        df = self.to_dataframe()
        df = df.pivot(columns='sensor', values='flow')
        df.to_parquet(path)

    def get_sensors(self):
        df = self.to_dataframe()
        sensors = list(set(df['sensor']))
        return sensors

    def _get_metadata(self):
        with open(self.path, "r") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            column_pattern = r"Colu_\d(.*)(?=\{)"
            for row in csv_reader:
                if row:
                    if "Number" in row[0]:
                        self.num_tables = int(row[0].split()[-1])  # The number is last 'word' in the string
                    if "Colu" in row[0]:
                        result = re.search(column_pattern, row[0])
                        if result:
                            self.columns.append(result.group(1).strip())
                    if "nEntries" in row[0]:
                        break
                line_count += 1
            self.data_line_count = line_count + 1
