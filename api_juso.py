import requests
from api_common import ApiBlueprint
import time


class JusoAddr(ApiBlueprint):
    def __init__(self):
        super().__init__()
        self.src = 'juso'
        self.token = 'U01TX0FVVEgyMDIzMDkwNzExMjAxMDExNDA4MzY='
        self.api_url = 'https://www.juso.go.kr/addrlink/addrLinkApi.do?'
        self.p = {'confmKey': self.token, 'resultType': 'json', 'countPerPage': 10}
        self.current_result = None
        self.use_cols = ['admCd', 'siNm', 'sggNm', 'emdNm', 'bdNm', 'jibunAddr', 'roadAddr', 'detBdNmList']
        self.error_list = []
        self.param_xy = ['admCd', 'rnMgtSn', 'udrtYn', 'buldMnnm', 'buldSlno']
        self.col_rename = {'admCd': 'region_code', 'siNm': 'depth1', 'sggNm': 'depth2', 'emdNm': 'depth3',
                           'bdNm': 'title', 'jibunAddr': 'parcel', 'roadAddr': 'road', 'detBdNmList': 'category',
                           'x': 'x', 'y': 'y', 'src': 'src'}

    def call_api(self, juso: str):
        self.p['keyword'] = juso
        retry_count = 0
        while retry_count < 3:
            retry_count += 1
            time_wait = 3
            try:
                req = requests.get(self.api_url, params=self.p)
                self.current_result = req.json()
                break
            except Exception as e:
                print(e)
                time.sleep(time_wait)
                print(f'Waiting...{time_wait}')
                time_wait += 3

    def call_api_cleansed(self, addr):
        pass

    def has_result(self, idx=0):
        try:
            if self.current_result['results']['common']['errorCode'] == '0':
                if idx < int(self.current_result['results']['common']['totalCount']):
                    return True
            return False
        except Exception as e:
            return False

    def get_param_for_xy(self, idx=0):
        if self.has_result:
            return {p: self.get_item_list()[idx][p] for p in self.param_xy}
        return None

    def get_item_list(self):
        return self.current_result['results']['juso']

    def get_all_results(self):
        result_list = []
        if self.has_result():
            for i in range(len(self.get_item_list())):
                result_list.append(self.get_result(i))
        return result_list

    def get_result(self, idx=0):
        ori_result = self.get_ori_result(idx)
        if ori_result:
            renamed = {self.col_rename.get(x): y for x, y in ori_result.items()}
            renamed['src'] = self.src
            return renamed
        return{}

    def get_ori_result(self, idx=0):
        if self.has_result(idx):
            ori_result = {col: self.get_item_list()[idx][col] for col in self.use_cols}
            return ori_result
        return {}

    def get_col_names(self):
        cols = list(self.col_rename.values())
        return cols


if __name__ == '__main__':
    j_addr = JusoAddr()
    j_addr.call_api('인천광역시 연수구 청학동 567-4')
    print(f'ori result : {j_addr.get_ori_result()}')
    print(f'renamed    : {j_addr.get_result()}')
    print(f'col name   : {j_addr.get_col_names()}')
    print()

    '''
    j_xy = JusoXy()
    j_xy.call_api(j_addr.get_param_for_xy())
    print(j_xy.get_result())
    print('---- Note: This is transformed using pyproj. Not very accurate----')
    '''
    '''
    from pyproj import Transformer, CRS
    class JusoXy(JusoAddr):
        def __init__(self):
            super().__init__()
            self.api_url = 'https://business.juso.go.kr/addrlink/addrCoordApi.do?'
            self.p = {'confmKey': 'U01TX0FVVEgyMDIzMDkwNjE2MjI1MDExNDA4MTU=', 'resultType': 'json'}
            self.use_cols = ['x', 'y']
            self.transformer = Transformer.from_crs(CRS.from_epsg(5179), CRS.from_epsg(4326), always_xy=True)
            self.src = 'juso_xy'

        def call_api(self, param):
            if param:
                param.update(self.p)
                req = requests.get(self.api_url, params=param)
                self.current_result = req.json()
            else:
                self.current_result = None

        def get_result(self, idx=0):
            if self.has_result():
                ent_x = self.get_item_list()[idx]['entX']
                ent_y = self.get_item_list()[idx]['entY']
                x, y = self.transformer.transform(ent_x, ent_y)
                # return {col: self.get_item_list()[col] for col in self.use_cols}
                return {'x': x, 'y': y, 'src': self.src}
            return {}

        def get_col_names(self):
            return ['x', 'y', 'src']
    '''
