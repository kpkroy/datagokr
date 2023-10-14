import helper as h
import datetime
from export_chunker import ExportChunker
import pandas as pd
import abc
import re
import os


class RegConverter:
    def __init__(self):
        self.converter={'강원특별자치도', '경기도', '경상남도', '경상북도', '대전광역시', '부산광역시', '서울특별시', '세종특별자치시', '울산광역시',
         '인천광역시', '전라남도', '전라북도', ',충청남도', '충청북도', '광주광역시', '대구광역시', '제주특별자치도'}


class ApiBlueprint:
    def __init__(self):
        self.begin_time = datetime.datetime.now()
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
        print(addr)
        addr_ = addr.split('(')[0]
        addr_ = re.sub(r'(\D)(\d)', r'\1 \2', addr_)
        addr_ = addr_.replace('  ', ' ')
        addr_ = addr_.replace('- ', '-')
        addr_ = addr_.replace(' , ', ',')
        addr_ = addr_.replace(' .', '')

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

    @staticmethod
    def get_shorter(addr) -> []:
        addr = re.sub(r'\([^)]*\)', '', addr)
        addr_comp = addr.split(" ")
        addr_shorter = [' '.join(addr_comp[:5]), ' '.join(addr_comp[:4]), ' '.join(addr_comp[:6])]
        return addr_shorter

    def add_address_candidates(self, row, addr_col_name):
        addresses = [row.get(addr_col_name, '')]
        col_names = ['addr', 'ori', 'frcs_addr', 'parcel', 'road', 'refined', 'refined_']
        new_addreses = [row.get(col_name, '') for col_name in col_names if col_name != addr_col_name]
        addresses.extend(new_addreses)
        addresses = [x for x in addresses if x]
        addresses = list(set(addresses))
        shorter_list = []
        for x in addresses:
            shorter_list.extend(self.get_shorter(x))
        shorter_list = list(set(shorter_list))
        for s in shorter_list:
            if s not in addresses:
                addresses.append(s)
        return addresses

    def add_api_col_to_csv(self, df: pd.DataFrame, addr_col_name: str, work_dir: str, out_name: str):
        # returns undone
        print(f'Starting Process for [{self.src}]')
        field_names = list(df.columns)
        field_names.extend(self.get_col_names())
        field_names.append('used_addr')

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
            row['used_addr'] = ''
            addresses = self.add_address_candidates(row, addr_col_name)
            if addresses:
                for addr in addresses:
                    self.call_api(addr)
                    if self.has_result():
                        row['used_addr'] = addr
                        break
            api_result = self.get_result()      # get result
            if not api_result:
                api_result = empty_rec
            row.update(api_result)
            ec.add_chunk([row])
        ec.export_csv_local()
        self.export_errors(work_dir, out_name)
        return df[i:]

    def add_juso_xy(self, df: pd.DataFrame, addr_col_name: str, work_dir: str, out_name: str):
        # returns undone
        print(f'Starting Process for [{self.src}]')
        field_names = ['brno', 'bzmn_stts_nm', 'crtr_ymd', 'frcs_addr', 'frcs_dtl_addr',
                       'frcs_nm', 'frcs_zip', 'lat', 'lot', 'addr']
        field_names.extend(self.get_col_names())
        field_names.append('refined_')

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
            row = self.update_process(row, empty_rec)
            ec.add_chunk([row])
            i += 1
        ec.export_csv_local()
        self.export_errors(work_dir, out_name)
        return df[i:]

    def update_process(self, row, empty_rec):
        lat = row.get('lat')
        addr = row.get('frcs_addr', '')
        if addr is None:
            addr = ''
        det = row.get('frcs_dtl_addr', '')
        if det is None:
            det = ''
        addr = addr + ' ' + str(det)
        if addr and addr[:1] == '[':
            addr = re.sub(r'\[.*?\]', '', addr)
        addr = addr.strip()
        row['refined_'] = addr

        if lat and self.is_juso_split_sanity(addr) and self.is_juso_number_sanity(addr):
            row.update(empty_rec)
            row['src'] = 'add_region_code'
            return row
        if addr:
            self.call_api(addr)
            if not self.has_result():
                self.call_api_cleansed(addr)
            api_result = self.get_result()  # get result
            if not api_result:
                api_result = empty_rec
            row.update(api_result)
        else:
            row['src'] = 'no_addr'
        return row

    def get_right_addr(self, row):
        addr = row.get('addr')
        if self.is_juso_number_sanity(addr) and self.is_juso_split_sanity(addr):
            return addr
        addr = row.get('frcs_addr') + ' ' + row.get('frcs_dtl_addr')
        if self.is_juso_number_sanity(addr) and self.is_juso_split_sanity(addr):
            return addr
        addr = row.get('')

    @staticmethod
    def is_juso_number_sanity(addr):
        return any(i.isdigit() for i in addr)

    @staticmethod
    def is_juso_split_sanity(addr):
        if len(re.split(' ', addr)) > 2:
            return True
        return False
