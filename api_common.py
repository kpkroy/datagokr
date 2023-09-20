import helper as h
from export_chunker import ExportChunker
import pandas as pd
import abc
import re
import os


class ApiBlueprint:
    def __init__(self):
        self.begin_time = h.get_now_time()
        self.src = ''
        self.api_url = ''
        self.p = dict()
        self.quota = None
        self.quota_count = 0

        self.current_result = None
        self.error_list = []

    def get_quota(self) -> int:
        return self.quota

    def set_quota(self, quota: int):
        self.quota = quota

    @abc.abstractmethod
    def call_api(self, addr):
        pass

    @abc.abstractmethod
    def get_col_names(self) -> list:
        pass

    @abc.abstractmethod
    def has_result(self) -> bool:
        pass

    @abc.abstractmethod
    def get_result(self, idx=0) -> dict:
        pass

    @staticmethod
    def cleanse_addr(addr):
        addr_ = addr.split('(')[0]
        addr_ = re.sub(r'(\D)(\d)', r'\1 \2', addr_)
        addr_ = addr_.replace('  ', ' ')
        print(f'new addr : {addr_}')
        return addr_

    def export_errors(self, work_dir, out_name):
        fp = os.path.join(work_dir, 'error_' + out_name)
        with open(fp, 'w') as file:
            for item in self.error_list:
                file.write('%s\n' % item)

    def call_api_cleansed(self, addr):
        cleansed = self.cleanse_addr(addr)
        self.call_api(cleansed)
        if not self.get_result():
            print(f'new_addr : {cleansed} NOT FOUND!')

    def add_api_col_to_csv(self, df: pd.DataFrame, addr_col_name: str, work_dir: str, out_name: str):
        # returns undone
        print(f'Starting Process for [{self.src}]')
        field_names = list(df.columns)
        field_names.extend(self.get_col_names())

        ec = ExportChunker(export_path=work_dir, export_file_name=out_name, begin_time=self.begin_time)
        ec.set_field_name(field_names)
        ec.set_chunk_size(100)
        table = df.to_dict('records')
        empty_rec = {x: '' for x in self.get_col_names()}
        empty_rec['src'] = self.src

        i = 0
        for row in table:
            if self.quota and self.quota_count >= self.quota:
                break
            i += 1
            addr = row.get(addr_col_name)       # call api
            self.call_api(addr)
            if not self.has_result():
                self.call_api_cleansed(addr)

            api_result = self.get_result()      # get result
            if not api_result:
                api_result = empty_rec
            row.update(api_result)
            ec.add_chunk([row])
        ec.export_csv_local()
        self.export_errors(work_dir, out_name)
        return df[i:]
