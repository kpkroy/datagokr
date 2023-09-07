import requests


class Vworld:
    def __init__(self):
        self.api_url = "https://api.vworld.kr/req/address?"
        self.p = {
            "service": "address",
            "request": "getcoord",
            "crs": "epsg:4326",
            "address": "",
            "format": "json",
            "type": "road",
            "key": "19D00DCE-5245-3A0A-A016-FEBB57784B06"
        }
        self.alt_type = {'road': 'parcel', 'parcel': 'road'}
        self.current_result = None

    def hit_api(self, juso, j_type='road'):
        if juso:
            self.p['address'] = juso
        self.p['type'] = j_type

        response = requests.get(self.api_url, params=self.p)
        self.current_result = response.json()

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

    def get_x(self):
        return self.current_result['response']['result']['point']['x']

    def get_y(self):
        return self.current_result['response']['result']['point']['y']

    def get_refined(self):
        return self.current_result['response']['refined']['text']

    def get_type(self):
        return self.current_result['response']['input']['type']

    def get_result(self):
        return {'x': self.get_x(),
                'y': self.get_y(),
                'refined': self.get_refined(),
                'type': self.get_type()}


class JusoSearch:
    def __init__(self):
        self.token = 'U01TX0FVVEgyMDIzMDkwNzExMjAxMDExNDA4MzY='
        self.api_url = 'https://www.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1'
        self.p = {'confmKey': self.token,
                  'resultType': 'json',
                  'countPerPage': 10}
        self.current_result = None

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
        return {'region_code': self.get_bcode(),
                'depth1': self.get_depth1(),
                'depth2': self.get_depth2(),
                'depth3': self.get_depth3()}


if __name__ == '__main__':
    a = JusoSearch()
    j = '인천광역시 연수구 청학동 567-4'
    a.hit_api(j)
    print(a.get_result())

    b = Vworld()
    k = '인천광역시 연수구 청학동 567-4'
    b.hit_api(k, j_type='parcel')
    print(b.get_result())
