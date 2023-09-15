import datetime
from export_chunker import ExportChunker
import pandas as pd
import abc
import re
import os


class ApiBlueprint:
    def __init__(self):
        self.api_url = ''
        self.p = dict()
        self.current_result = None
        self.error_list = []
        self.begin_time = datetime.datetime.now()

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

    def read_file(self, addr_col_name, ifp, start_from, start_col) -> []:
        df = pd.read_csv(ifp, encoding='utf-8-sig')
        df.dropna(subset=[addr_col_name], inplace=True)
        if start_from:
            df = df[df[start_col] > start_from]
        table = df.to_dict('records')
        field_names = list(df.columns)
        field_names.extend(self.get_col_names())
        return table, field_names

    def add_api_col_to_csv(self, addr_col_name: str, ifp: str, work_dir: str, out_name: str, start_from=None, start_col=None):
        table, field_names = self.read_file(addr_col_name, ifp, start_from, start_col)
        ec = ExportChunker(export_path=work_dir, export_file_name=out_name, begin_time=self.begin_time)
        ec.set_field_name(field_names)
        ec.set_chunk_size(1000)

        i = 0
        for row in table:
            i += 1

            addr = row.get(addr_col_name)       # call api
            self.call_api(addr)
            if not self.has_result():
                cleansed = self.cleanse_addr(addr)
                self.call_api(cleansed)

            api_result = self.get_result()      # get result
            row.update(api_result)
            ec.add_chunk([row])
        ec.export_csv_local()
        self.export_errors(work_dir, out_name)
