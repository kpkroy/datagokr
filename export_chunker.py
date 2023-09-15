import os
import csv
import datetime
import pandas as pd


class ExportChunker:
    """
    export "data" to local in csv format
    """
    def __init__(self, export_path,
                 export_file_name,
                 is_append=True,       # if True, append to existing file (if there is a existing file)
                 begin_time=None,
                 attach_note='',
                 id_col_name=None):
        self.export_path = export_path
        self.export_file_name = export_file_name
        self.is_append = is_append
        self.chunk_list = []
        self.chunk_df = pd.DataFrame()
        self.chunk_item_num = 0
        self.chunk_item_max = 1000
        self.chunk_batch_num = 1
        self.begin_time = begin_time
        self.field_name = []
        self.file_path = os.path.join(self.export_path, self.export_file_name)
        self.attach_note = attach_note
        self.id_col_name = id_col_name
        self.setup_environment()

    def set_chunk_size(self, chunk_size):
        self.chunk_item_max = chunk_size

    def setup_environment(self):
        if not os.path.exists(self.export_path):
            os.mkdir(self.export_path)
        file_exists = os.path.isfile(self.file_path)
        if file_exists and not self.is_append:
            os.remove(self.file_path)

    def set_field_name(self, field_name: list):
        self.field_name = field_name

    def add_chunk(self, data_list: []):
        self.chunk_list.extend(data_list)
        self.chunk_item_num += len(data_list)
        if self.chunk_item_num > self.chunk_item_max * self.chunk_batch_num:
            self.export_csv_local()
            self.chunk_list = []
            if self.begin_time:
                print('{0} : exported row num {1:,}'.format(datetime.datetime.now() - self.begin_time,
                                                            self.chunk_item_num))
            self.chunk_batch_num += 1

    def filter_by_field_name(self, df: pd.DataFrame):
        if not self.field_name:
            return df

        df_col = set(df.columns)
        fn_col = set(self.field_name)
        # we want fn_col to be a subset of df_col
        if fn_col.issubset(df_col):
            return df[self.field_name]
        else:
            # for column names in field_name not in df, add a column of None
            for col_nf_in_df in fn_col - df_col:
                df[col_nf_in_df] = None
            return df[self.field_name]

    def export_csv_local(self):
        if not self.chunk_list:
            return
        if not self.export_file_name:
            return

        df = pd.DataFrame(self.chunk_list)
        if self.attach_note:
            df['_notes_'] = self.attach_note

        if os.path.isfile(self.file_path):
            self.field_name = self.get_field_name_from_existing_file()
        df = self.filter_by_field_name(df)
        df = self.reorder_dataframe_column(df)
        if os.path.isfile(self.file_path):
            df.to_csv(self.file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
        else:
            df.to_csv(self.file_path, index=False, encoding='utf-8-sig')

    def reorder_dataframe_column(self, df):
        if self.id_col_name:
            columns = list(df.columns)
            columns.remove(self.id_col_name)
            columns.insert(0, self.id_col_name)
            if list(df.columns) != columns:
                df = df[columns]
        return df

    def get_field_name_from_existing_file(self):
        with open(self.file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
        return header

    def export_remaining(self):
        self.export_csv_local()
        self.chunk_list = []

    def set_chunk_list(self, data_list: []):
        self.chunk_list = data_list
