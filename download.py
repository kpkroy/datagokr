import requests
import time
import pandas as pd
import datetime


class DataGo:
    def __init__(self, date_num=''):
        self.name = 'Datago'
        self.s_key = 'RPhzV1mq7cMIwWp4intcHUvyvIQKhxPCCIbtbna1FfD23yFJFnbktcEVbX/auQgrruR2bWz0bhom1lGPjJdg6Q=='
        self.end_point = ''
        self.page_num = 2
        self.p = {'serviceKey': self.s_key, 'numOfRows': 10000, 'resultType': 'json'}
        self.year = ''
        self.date_num = date_num
        self.start_page_num = 0

    def req_to_df(self, req):
        if self.p['resultType'] == 'json':
            return pd.DataFrame.from_dict(req.json().get('items'))
        elif self.p['resultType'] == 'xml':
            return pd.read_xml(req.content, xpath='.//items//item')

    def set_year(self, year):
        self.year = year
        self.p['yr'] = year

    def set_starting_page_num(self, start_page: int):
        self.start_page_num = start_page

    def download(self, year=0):
        if year:
            self.set_year(year)
        page_num = self.start_page_num

        df = pd.DataFrame()
        while True:
            time.sleep(2)
            page_num += 1
            self.p['pageNo'] = page_num
            new_df = self.get_page_data()
            print('....[{1}] - {0}: {2} rows downloaded'.format(page_num, self.name, len(new_df)))
            df = pd.concat([df, new_df])
            if len(new_df) < self.p['numOfRows']:
                break

        file_name = '{0}_{1}_{2}.csv'.format(self.date_num, self.name, self.year)
        df.to_csv(file_name, encoding='utf-8-sig')
        print('.... EXPORTED [{1}] {0} rows'.format(len(df), file_name))

    def get_page_data(self):
        retry_count = 0
        new_df = pd.DataFrame()
        time.sleep(2)
        req = requests.get(self.end_point, params=self.p)

        while retry_count < 4:
            try:
                new_df = self.req_to_df(req)
                break
            except Exception as e:
                time.sleep(2)
                retry_count += 1
                req = requests.get(self.end_point, params=self.p)
        return new_df


class FranHQ(DataGo):
    # 공정거래위원회_가맹정보_가맹본부 일반정보 상세 제공 서비스
    # 년도, 사업자번호, 법인등록번호를 통하여 사업자번호, 법인등록번호, 개인/법인 구분코드, 법인명, 우편번호, 소재지주소, 소재지 주소상세, 대표자명 등의 정보를 조회하는 기능
    # 목적 : fran 에 대한 본사 주소

    def __init__(self, date_num=''):
        super().__init__(date_num)
        self.name = 'ftc_hq'
        self.end_point = 'http://apis.data.go.kr/1130000/FftcJnghdqrtrsGnrlDtlService/getJnghdqrtrsGnrlDtl'


class FranHQHistory(DataGo):
    # 공정거래위원회_가맹정보_가맹본부 일반 정보 변경 이력 현황 제공 서비스
    # 가맹본부 일반 정보 변경 이력 현황 조회
    # 년도, 사업자등록번호, 법인등록번호 통하여 사업자번호, 법인등록번호, 개인/법인 구분, 법인명, 우편번호, 주소, 주소상세 등의 정보를 조회하는 기능

    # 목적 : fran 에 대한 본사 주소
    def __init__(self, date_num=''):
        super().__init__(date_num)
        self.name = 'ftc_hq_hist'
        self.end_point = 'http://apis.data.go.kr/1130000/FftcjnghdqrtrsInfoChghstService/getJnghdqrtrsInfoChghst'


class FranMas(DataGo):
    # 목적 : franchise 및 브랜드 추가용
    # 공정거래위원회_가맹정보_브랜드 목록 제공 서비
    # 브랜드 목록 조회
    # 년도를 통하여 가맹정보공개서등록번호, 사업자등록번호, 법인등록번호, 법인명, 브랜드명, 대표자명, 업종 대분류명, 업종 중분류명등의 정보를 조회하는 기능

    def __init__(self, date_num=''):
        super().__init__(date_num)
        self.name = 'fran_brand_mas'
        self.end_point = 'http://apis.data.go.kr/1130000/FftcBrandRlsInfoService/getBrandRlsInfo'


class FranBranch(DataGo):
    # 공정거래위원회_가맹정보_브랜드 가맹점 및 직영점 정보 제공 서비스
    # 브랜드 가맹점 및 직영점 정보 조회
    # 년도, 등록번호를 통하여 전체점포수, 지역명, 브랜드명, 직영점포수, 가맹점수, 업종대분류명, 업종중분류명 등의 정보를 조회하는 기능
    # 목적 : 적절한 fran 목록 뽑아보기
    def __init__(self, date_num=''):
        super().__init__(date_num)
        self.name = 'ftc_branch'
        self.end_point = 'http://apis.data.go.kr/1130000/FftcBrandFrcsDropInfoService/getBrandFrcsDropInfo'


class BrandComp(DataGo):
    # 공정거래위원회_가맹정보_브랜드 비교 목록 제공 서비스
    # 브랜드 비교 목록 조회
    # 년도, 등록번호를 이용하여 가맹정보공개서등록번호, 사업자등록번호, 법인등록번호, 법인명, 브랜드명, 대표자명, 업종대분류명, 업종중분류명, 가맹사업개시일자 등의 정보를 조회하는 기능
    # 목적 : fran <-> brand 에 대한 converter 리스트 추가용
    def __init__(self, date_num=''):
        super().__init__(date_num)
        self.name = 'brand_comp'
        self.end_point = 'http://apis.data.go.kr/1130000/FftcBrandCompInfoService/getBrandCompInfo'


class Bohum(DataGo):
    # 목적 : fran / none fran 등 모든 업체 사업자등록번호
    def __init__(self, date_num=''):
        super().__init__(date_num)
        self.name = 'bohom'
        self.end_point = 'http://apis.data.go.kr/B490001/gySjbPstateInfoService/getGySjBoheomBsshItem'
        self.p['resultType'] = 'xml'
        self.p['numOfRows'] = 100000


if __name__ == '__main__':
    d_num = datetime.datetime.now().strftime('%m%d_%H%M')
    # Bohum(d_num).download()

    fm = FranMas(d_num)
    hq = FranHQ(d_num)
    hqh = FranHQHistory(d_num)
    fr = FranBranch(d_num)

    for y in range(2017, 2024):
        fm.download(y)
        time.sleep(2)
        hq.download(y)
        time.sleep(2)
        fr.download(y)
        time.sleep(2)

    for y in range(2022, 2024):
        hqh.download(y)
        time.sleep(2)
