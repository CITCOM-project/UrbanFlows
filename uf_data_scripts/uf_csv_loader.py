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
        self.columns = dict()
        self.num_tables = None
        self.table_start_lines = list()
        self.num_entries = list()

    def to_dataframe(self):
        self._get_metadata()
        df = pd.DataFrame()
        for table in range(len(self.table_start_lines)):
            df_temp = pd.read_csv(self.path, skiprows=self.table_start_lines[table], names=self.columns[table],
                                  nrows=self.num_entries[table])
            df_temp = df_temp[:-1]  # Drop End of table string
            df_temp = df_temp.set_index('time')
            df_temp = self._gen_column_name(df_temp)
            df = pd.concat([df, df_temp], axis=1)
        return df

    def _gen_column_name(self, df):
        sensor_name = df['sensor'].values[0]
        df = df.drop(columns=['sensor'])
        df.columns.values[0] = sensor_name + "_" + df.columns.values[0]
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
        current_table = 0
        line_count = 1
        with open(self.path, "r") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                if row:
                    if "Number" in row[0]:
                        self.num_tables = int(row[0].split()[-1])  # The number is last 'word' in the string
                        for i in range(self.num_tables):
                            self.columns[i] = list()
                    if "Colu" in row[0]:
                        self._get_table_metadata(row[0], current_table)
                    if "nEntries" in row[0]:
                        self.table_start_lines.append(line_count)
                        self.num_entries.append(int(row[0].split()[-1]))
                    if "End" in row[0]:
                        current_table += 1
                line_count += 1

    def _get_table_metadata(self, row: str, current_table: int):

        column_pattern = r"Colu_\d(.*)(?=\{)"
        result = re.search(column_pattern, row)
        if result:
            self.columns[current_table].append(result.group(1).strip())


if __name__ == "__main__":
    uf = LoadCsvUf(Path('example_data/SufoExtract_20230713_to_20230713.csv'))
    df = uf.to_dataframe()
    print(df.head())
