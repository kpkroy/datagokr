import requests
import pandas as pd
import os
from export_chunker import ExportChunker
import datetime
import time
import re


class VworldFran:
    def __init__(self):
        self.api_url = "https://api.vworld.kr/req/search?"
        self.p = {
            "service": "search",
            "request": "search",
            "version": "2.0",
            "crs": "epsg:4326",
            "size": "1000",
            "format": "json",
            "errorformat": "json",
            "type": "place",
            "key": "19D00DCE-5245-3A0A-A016-FEBB57784B06",
            "page": "",
            "query": ""
        }
        self.current_result = None
        self.use_cols = ['id', 'title', 'category', 'address', 'point']
        self.depth2 = {'address': ['road', 'parcel'], 'point': ['x', 'y']}
        self.error_list = []

        '''
        {"id": "POI0100000097ARXA", "title": "CU", "category": "음ㆍ식료품위주종합소매업 > 체인화편의점",
         "address": {"road": "경기도 고양시 일산동구 한류월드로 281", "parcel": "경기도 고양시 일산동구 장항동 1775대"},
         "point": {"x": "14110260.46889237", "y": "4531748.415737647"}
        '''
    def hit_api(self, fran, page_num=1):
        self.p['query'] = fran
        self.p['page'] = page_num
        response = requests.get(self.api_url, params=self.p)
        self.current_result = response.json()

    def get_col_names(self):
        col_list = self.use_cols
        for k, v in self.depth2:
            col_list.append(v)
        return col_list

    def is_ok(self) -> bool:
        try:
            if self.current_result['response']['status'] == 'OK':
                return True
            return False
        except Exception as e:
            return False

    def has_result(self):
        if self.is_ok():
            if len(self.current_result['response']['result']['items']) > 0:
                return True
        return False

    def create_rec(self, response):
        rec = {}
        for col in self.use_cols:
            if col in self.depth2:
                for item in self.depth2.get(col):
                    rec[item] = response[col][item]
            else:
                rec[col] = response[col]
        return rec

    def get_results(self):
        if self.has_result():
            result_list = []
            response_list = self.current_result['response']['result']['items']
            for response in response_list:
                result_list.append(self.create_rec(response))
            return result_list
        else:
            self.error_list.append(self.p['query'])
            print('error on')
    # todo : loop through pages

    def export_errors(self, work_dir, out_name):
        fp = os.path.join(work_dir, 'error_' + out_name)
        with open(fp, 'w') as file:
            for item in self.error_list:
                file.write('%s\n' % item)


class VworldXy:
    def __init__(self):
        self.api_url = "https://api.vworld.kr/req/address?"
        self.p = {
            "service": "address",
            "request": "getcoord",
            "version": "2.0",
            "crs": "epsg:4326",
            "address": "",
            "format": "json",
            "type": "road",
            "key": "19D00DCE-5245-3A0A-A016-FEBB57784B06"
        }
        self.alt_type = {'road': 'parcel', 'parcel': 'road'}
        self.current_result = None
        self.col_d1 = 'depth1'
        self.col_d2 = 'depth2'
        self.col_d3 = 'depth3'
        self.col_x = 'x'
        self.col_y = 'y'
        self.col_refined = 'refined'
        self.col_type = 'type'
        self.error_list = []

    def hit_api(self, juso, j_type='road'):
        if juso:
            self.p['address'] = juso
        self.p['type'] = j_type

        response = requests.get(self.api_url, params=self.p)
        try:
            self.current_result = response.json()
        except Exception as e:
            time.sleep(2)
            print(f'Request Parsing Error : {e}')
            self.error_list.append(e)

        if not self.is_ok():
            self.p['type'] = self.alt_type.get(j_type)
            response = requests.get(self.api_url, params=self.p)
            self.current_result = response.json()

    def is_ok(self) -> bool:
        try:
            if self.current_result['response']['status'] == 'OK':
                return True
            return False
        except Exception as e:
            return False

    def has_result(self):
        if self.is_ok():
            return True
        return False

    def get_x(self):
        return self.current_result['response']['result']['point']['x']

    def get_y(self):
        return self.current_result['response']['result']['point']['y']

    def get_refined(self):
        return self.current_result['response']['refined']['text']

    def get_type(self):
        return self.current_result['response']['input']['type']

    def get_depth1(self):
        return self.current_result['response']['refined']['structure']['level1']

    def get_depth2(self):
        return self.current_result['response']['refined']['structure']['level2']

    def get_depth3(self):
        if self.current_result['response']['refined']['structure']['level3']:
            return self.current_result['response']['refined']['structure']['level3']
        return self.current_result['response']['refined']['structure']['level4A']

    def get_result(self):
        if self.has_result():
            return {self.col_refined: self.get_refined(),
                    self.col_type: self.get_type(),
                    self.col_d1: self.get_depth1(),
                    self.col_d2: self.get_depth2(),
                    self.col_d3: self.get_depth3(),
                    self.col_x: self.get_x(),
                    self.col_y: self.get_y(),
                    }
        else:
            print(f"[error] : {self.p['address']}")
            self.error_list.append(self.p['address'])
            return {self.col_x: '',
                    self.col_y: '',
                    self.col_d1: '',
                    self.col_d2: '',
                    self.col_d3: '',
                    self.col_refined: '',
                    self.col_type: ''}

    def get_col_names(self):
        return [self.col_refined, self.col_type, self.col_d1, self.col_d2, self.col_d3, self.col_x, self.col_y]

    def export_errors(self, work_dir, out_name):
        fp = os.path.join(work_dir, 'error_' + out_name)
        with open(fp, 'w') as file:
            for item in self.error_list:
                file.write('%s\n' % item)


class JusoSearch:
    def __init__(self):
        self.token = 'U01TX0FVVEgyMDIzMDkwNzExMjAxMDExNDA4MzY='
        self.api_url = 'https://www.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1'
        self.p = {'confmKey': self.token,
                  'resultType': 'json',
                  'countPerPage': 10}
        self.current_result = None
        self.col_region = 'region_code'
        self.col_d1 = 'depth1'
        self.col_d2 = 'depth2'
        self.col_d3 = 'depth3'
        self.error_list = []

    def hit_api(self, juso: str):
        if juso:
            self.p['keyword'] = juso
        req = requests.get(self.api_url, params=self.p)
        self.current_result = req.json()

    def is_ok(self):
        try:
            if self.get_result_of():
                return True
            else:
                return False
        except Exception as e:
            return False

    def get_bcode(self, idx=0):
        return self.get_result_of()[idx]['admCd']

    def get_x(self, idx=0):
        return self.get_result_of()[idx]['entX']

    def get_y(self, idx=0):
        return self.get_result_of()[idx]['entY']

    def get_depth1(self, idx=0):
        return self.get_result_of()[idx]['siNm']

    def get_depth2(self, idx=0):
        return self.get_result_of()[idx]['sggNm']

    def get_depth3(self, idx=0):
        return self.get_result_of()[idx]['emdNm']

    def get_result_of(self):
        return self.current_result['results']['juso']

    def get_result(self):
        if self.has_result():
            return {self.col_region: self.get_bcode(),
                    self.col_d1: self.get_depth1(),
                    self.col_d2: self.get_depth2(),
                    self.col_d3: self.get_depth3()}
        print(f"[b_code] error with address : {self.p['keyword']}")
        self.error_list.append(self.p['keyword'])
        return {self.col_region: '',
                self.col_d1: '',
                self.col_d2: '',
                self.col_d3: ''}

    def get_col_names(self):
        return [self.col_region, self.col_d1, self.col_d2, self.col_d3]

    def has_result(self):
        try:
            self.get_bcode()
            return True
        except Exception as e:
            return False

    def export_errors(self, work_dir, out_name):
        fp = os.path.join(work_dir, 'error_' + out_name)
        with open(fp, 'w') as file:
            for item in self.error_list:
                file.write('%s\n' % item)


class JusoXyCsvHandler:
    def __init__(self):
        self.api_type = {'juso': JusoSearch(), 'xy': VworldXy()}

    def create_csv(self, addr_col_name: str, ifp: str, work_dir: str, out_name: str, api_name: str):
        df = pd.read_csv(ifp, encoding='utf-8-sig')
        df.dropna(subset=[addr_col_name], inplace=True)
        table = df.to_dict('records')
        if not table:
            return

        use_api = self.api_type.get(api_name)

        field_names = list(df.columns)
        field_names.extend(use_api.get_col_names())
        ec = ExportChunker(export_path=work_dir, export_file_name=out_name)
        ec.set_field_name(field_names)

        i = 0
        for row in table:
            i += 1
            addr = row.get(addr_col_name)
            if not addr:
                continue
            use_api.hit_api(addr)
            if not use_api.has_result():
                addr_ = addr.split('(')[0]
                addr_ = re.sub(r'(\D)(\d)', r'\1 \2', addr_)
                addr_ = addr_.replace('  ', ' ')
                print(f'new addr : {addr_}')
                use_api.hit_api(addr_)
            row.update(use_api.get_result())
            if i % 1000 == 0:
                now_time = datetime.datetime.now().strftime('%m%d_%H%M')
                print(f'[{now_time}] processing table num {i}')
            ec.add_chunk([row])
        ec.export_csv_local()
        use_api.export_errors(work_dir, out_name)


if __name__ == '__main__':
    '''
    a = JusoSearch()
    j = '인천광역시 연수구 청학동 567-4'
    a.hit_api(j)
    print(a.get_result())

    b = VworldXy()
    k = '경상남도 창녕군 대합면 등지리 888'
    b.hit_api(k, j_type='parcel')
    print(b.get_result())
    '''

    addr_ = 'addr'
    input_file_path = os.path.join('data', 'brno_c_0.csv')
    wd = 'data'
    d_num = datetime.datetime.now().strftime('%m%d_%H%M')
    o_name = f'{d_num}_result.csv'

    c = JusoXyCsvHandler()
    c.create_csv(addr_, input_file_path, wd, o_name, 'xy')
