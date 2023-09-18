from api_common import ApiBlueprint
from PyKakao import Local


class KakaoApi(ApiBlueprint):
    def __init__(self):
        super().__init__()
        self.keys = ['564cc09454b9458b0c8c2e1e558e70d5', '93812f2cb47c11a5c533333793ed520d']
        self.quota = 100000
        self.token = '564cc09454b9458b0c8c2e1e558e70d5'
        self.api_url = 'https://dapi.kakao.com/v2/local/search/address.'
        self.api = Local(service_key=self.keys[0])
        self.use_cols = ['x', 'y']
        self.two_depth_cols = {'address': ['address_name', 'b_code', 'region_1depth_name', 'region_2depth_name',
                                           'region_3depth_name'],
                               'road_address': ['address_name', 'building_name']}
        self.col_rename = {'road_address_address_name': 'road',
                           'road_address_building_name': 'title',
                           'address_address_name': 'parcel',
                           'address_b_code': 'region_code',
                           'address_region_1depth_name': 'depth1',
                           'address_region_2depth_name': 'depth2',
                           'address_region_3depth_name': 'depth3',
                           'x': 'x', 'y': 'y', 'category': 'category', 'src': 'src'}
        self.src = 'kakao'

    def use_key(self, key_val):
        self.api = Local(service_key=self.keys[key_val])

    def get_col_names(self):
        return list(self.col_rename.values())

    def get_all_results(self):
        result_list = []
        if self.has_result():
            for i in range(len(self.get_item_list())):
                result_list.append(self.get_result(i))
        return result_list

    def get_item_list(self):
        return self.current_result['documents']

    def get_result(self, idx=0):
        ori_result = self.get_ori_result(idx)
        if ori_result:
            ori_result = {self.col_rename.get(x): y for x, y in ori_result.items()}
            ori_result['src'] = self.src
            ori_result['category'] = ''
            return ori_result
        return {}

    def get_ori_result(self, idx=0):
        if self.has_result(idx):
            item = self.get_item_list()[idx]
            if item.get('address_type') == 'REGION':
                rec = {col: '' for col in self.use_cols}  # if no address
            else:
                rec = {col: item[col] for col in self.use_cols}

            for col in self.two_depth_cols:
                if col in item and item.get(col) is not None:
                    for in_col in self.two_depth_cols.get(col):
                        rec[col + '_' + in_col] = item[col][in_col]
            return rec
        return {}

    def call_api(self, addr):
        self.current_result = self.api.search_address(addr)
        self.quota_count += 1

    def call_api_cleansed(self, addr):
        pass

    def has_result(self, idx=None) -> bool:
        try:
            if not idx:
                if len(self.current_result['documents']) > 0:
                    return True
            else:
                if self.current_result['documents'][idx]:
                    return True
            return False
        except:
            return False


if __name__ == '__main__':
    ka = KakaoApi()
    ka.call_api('인천 연수구 청학동 567-4')
    print(ka.get_result())

