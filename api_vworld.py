from api_common import ApiBlueprint
import requests
import os


class VworldXy(ApiBlueprint):
    def __init__(self):
        super().__init__()
        self.available_keys = ['19D00DCE-5245-3A0A-A016-FEBB57784B06',
                               'BBBEA9AB-CA7E-3430-BEF2-404D8FD62BAC',
                               '56ADEBE7-7ABD-32DF-BBA5-3873FC1FAD4D']
        self.quota = 40000
        self.api_url = "https://api.vworld.kr/req/address?"
        self.p = {
            "service": "address",
            "request": "getcoord",
            "version": "2.0",
            "crs": "epsg:4326",
            "address": "",
            "format": "json",
            "type": "road",
            "key": self.available_keys[0]
        }
        self.alt_type = {'road': 'parcel', 'parcel': 'road'}
        self.current_result = None
        self.col_d1 = 'depth1'
        self.col_d2 = 'depth2'
        self.col_d3 = 'depth3'
        self.col_x = 'x'
        self.col_y = 'y'
        self.col_type = 'type'
        self.error_list = []
        self.src = 'vworld_xy'

    def use_key(self, key_num: int):
        self.p['key'] = self.available_keys[key_num]

    def call_api(self, juso, j_type='road'):
        if juso:
            self.p['address'] = juso
        self.p['type'] = j_type

        response = requests.get(self.api_url, params=self.p)
        self.quota_count += 1
        try:
            self.current_result = response.json()
        except Exception as e:
            print(f'Request Parsing Error : {e}')
            self.error_list.append(e)

        if not self.is_ok():
            self.p['type'] = self.alt_type.get(j_type)
            response = requests.get(self.api_url, params=self.p)
            self.quota_count += 1
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

    def get_result(self, idx=0):
        if self.has_result():
            return {self.get_type(): self.get_refined(),
                    self.alt_type.get(self.get_type()): '',
                    self.col_d1: self.get_depth1(),
                    self.col_d2: self.get_depth2(),
                    self.col_d3: self.get_depth3(),
                    self.col_x: self.get_x(),
                    self.col_y: self.get_y(),
                    'src': self.src,
                    'title': '',
                    'region_code': '',
                    'category': ''
                    }
        return {}

    def get_col_names(self):
        return [self.col_d1, self.col_d2, self.col_d3, self.col_x, self.col_y, 'src', 'title', 'region_code'
                'road', 'parcel', 'category']

    def export_errors(self, work_dir, out_name):
        fp = os.path.join(work_dir, 'error_' + out_name)
        with open(fp, 'w') as file:
            for item in self.error_list:
                file.write('%s\n' % item)


class VworldFran(ApiBlueprint):
    def __init__(self):
        super().__init__()
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
        self.two_depth_cols = {'address': ['road', 'parcel'], 'point': ['x', 'y']}
        self.error_list = []
        self.src = 'vworld_fran'

        '''
        {"id": "POI0100000097ARXA", "title": "CU", "category": "음ㆍ식료품위주종합소매업 > 체인화편의점",
         "address": {"road": "경기도 고양시 일산동구 한류월드로 281", "parcel": "경기도 고양시 일산동구 장항동 1775대"},
         "point": {"x": "14110260.46889237", "y": "4531748.415737647"}
        '''
    def call_api(self, fran, page_num=1):
        self.p['query'] = fran
        self.p['page'] = page_num
        response = requests.get(self.api_url, params=self.p)
        self.quota_count += 1
        self.current_result = response.json()

    def get_col_names(self):
        col_list = self.use_cols
        for k, v in self.two_depth_cols:
            col_list.extend(v)
        col_list.append('src')
        return col_list

    def has_result(self):
        try:
            if self.current_result['response']['status'] == 'OK':
                if len(self.current_result['response']['result']['items']) > 0:
                    return True
            return False
        except Exception as e:
            return False

    def create_rec(self, response):
        rec = {'src': self.src}
        for col in self.use_cols:
            if col in self.two_depth_cols:
                for item in self.two_depth_cols.get(col):
                    rec[item] = response[col][item]
            else:
                rec[col] = response[col]
        return rec

    def get_result(self, idx=0) -> dict:
        pass

    def get_all_results(self):
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


if __name__ == '__main__':
    b = VworldXy()
    k = '경상남도 창녕군 대합면 등지리 888'
    b.call_api(k, j_type='parcel')
    print(b.get_result())
