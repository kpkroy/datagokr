import requests
import time
import pandas as pd
import datetime


class DataGo:
    def __init__(self):
        self.name = 'Datago'
        self.s_key = 'RPhzV1mq7cMIwWp4intcHUvyvIQKhxPCCIbtbna1FfD23yFJFnbktcEVbX/auQgrruR2bWz0bhom1lGPjJdg6Q=='
        self.end_point = ''
        self.page_num = 2
        self.p = {'serviceKey': self.s_key, 'pageNo': 1, 'numOfRows': 5000, 'resultType': 'json'}

    def download(self, year=0, date_num=''):
        if year:
            self.p['yr'] = year
        else:
            year = ''

        df = pd.DataFrame()
        '''
        for i in range(1, self.page_num):
            self.p['pageNo'] = i
            req = requests.get(self.end_point, params=self.p)
            time.sleep(2)
            new_df = self.req_to_df(i, req)
            if len(new_df) == 0:
                break
            df = pd.concat([df, new_df])
        '''
        i = 0
        while True:
            i += 1
            self.p['pageNo'] = i
            req = requests.get(self.end_point, params=self.p)
            time.sleep(2)
            new_df = self.req_to_df(i, req)
            if len(new_df) == 0:
                break

        file_name = '{0}_{1}_{2}.csv'.format(date_num, self.name, year)
        df.to_csv(file_name, encoding='utf-8-sig')
        print('.... EXPORTED [{1}] {0} rows'.format(len(df), file_name))

    def req_to_df(self, i, req):
        new_df = pd.DataFrame.from_dict(req.json().get('items'))
        print('.... [{1}] - {0}: {2} rows downloaded'.format(i, self.name, len(new_df)))
        return new_df


class FtcHQ(DataGo):
    def __init__(self):
        super().__init__()
        self.name = 'ftc_hq'
        self.end_point = 'http://apis.data.go.kr/1130000/FftcJnghdqrtrsGnrlDtlService/getJnghdqrtrsGnrlDtl'


class FtcHQSub(DataGo):
    def __init__(self):
        super().__init__()
        self.name = 'ftc_hq_sub'
        self.end_point = 'http://apis.data.go.kr/1130000/FftcjnghdqrtrsInfoChghstService/getJnghdqrtrsInfoChghst'


class BrandComp(DataGo):
    def __init__(self):
        super().__init__()
        self.name = 'brand_comp_'
        self.end_point = 'http://apis.data.go.kr/1130000/FftcBrandCompInfoService/getBrandCompInfo'


class Bohum(DataGo):
    def __init__(self):
        super().__init__()
        self.name = 'bohom'
        self.end_point = 'http://apis.data.go.kr/B490001/gySjbPstateInfoService/getGySjBoheomBsshItem'
        self.p['resultType'] = 'xml'

    def req_to_df(self, i, req):
        new_df = pd.read_xml(req.content, xpath='.//items//item')
        print('....[{1}] - {0}: {2} rows downloaded'.format(i, self.name, len(new_df)))
        return new_df


if __name__ == '__main__':
    d_num = datetime.datetime.now().strftime('%m%d_%H%M')
    for y in range(2017, 2024):
        FtcHQ().download(year=y, date_num=d_num)
        time.sleep(1)
    Bohum().download(date_num=d_num)
    BrandComp().download(year=2022)



