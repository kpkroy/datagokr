import time
import pandas as pd
import datetime
import os
import requests
import lxml
import argparse
import helper as h


class DataGo:
    def __init__(self, date_num='', work_dir='data'):
        self.begin_time = h.get_now_time()
        self.date_num = date_num
        self.work_dir = work_dir
        self.src = 'datago'
        self.token = 'RPhzV1mq7cMIwWp4intcHUvyvIQKhxPCCIbtbna1FfD23yFJFnbktcEVbX/auQgrruR2bWz0bhom1lGPjJdg6Q=='
        self.api_url = ''
        self.p = {'serviceKey': self.token, 'numOfRows': 10000, 'resultType': 'json'}

    def get_elapsed(self):
        pass

    def download(self, start_page_num=0):
        page_num = start_page_num
        df = pd.DataFrame()
        while True:
            page_num += 1
            self.p['pageNo'] = page_num
            new_df = self.get_response()
            df = pd.concat([df, new_df])
            print(f'....[{self.src}] - {page_num}: {len(new_df)} rows downloaded')
            if len(new_df) < self.p['numOfRows']:
                break
        self.export_to_work_dir(df)

    def get_update(self, prev_num, now_num=None):
        if now_num is None:
            now_num = self.date_num
        fp_now = os.path.join(self.work_dir, self.get_file_name(now_num))
        fp_prev = os.path.join(self.work_dir, self.get_file_name(prev_num))
        fp_updated = os.path.join(self.work_dir, self.get_file_name(self.date_num, '_update'))
        use_cols = ['addr', 'saeopjangNm', 'saeopjaDrno']
        d_types = {'addr': 'string', 'saeopjangNm': 'string', 'saeopjangDrno': float}

        updated_df = h.get_update_without_id(fp_prev, fp_now, use_cols, d_types)
        updated_df.to_csv(fp_updated, encoding='utf-8-sig')

    def get_response(self):
        retry_count = 0
        while retry_count < 4:
            time.sleep(2)
            try:
                req = requests.get(self.api_url, params=self.p)
                return self.to_dataframe(req)
            except Exception as e:
                print(e)
                retry_count += 1
                time.sleep(2)
        return pd.DataFrame()

    def to_dataframe(self, req):
        if self.p['resultType'] == 'json':
            return pd.DataFrame.from_dict(req.json().get('items'))
        elif self.p['resultType'] == 'xml':
            return pd.read_xml(req.content, xpath='.//items//item')

    def get_file_name(self, date_num, post_fix=''):
        return f'{date_num}_{self.src}_{post_fix}.csv'

    def export_to_work_dir(self, df: pd.DataFrame):
        file_name = self.get_file_name(self.date_num)
        fp = os.path.join(self.work_dir, file_name)
        if os.path.isfile(fp):
            df.to_csv(fp, encoding='utf-8-sig', index=False, mode='a', header=False)
        else:
            df.to_csv(fp, encoding='utf-8-sig', index=False)
        print(f'....[{self.src}] - {file_name}: {len(df)} rows exported')


class CardFranchise(DataGo):
    # 한국조폐공사_카드_가맹점기본정보
    # 지역사랑상품권 운영대행사들로부터 수집한 카드 가맹점에 대한 가맹점명, 대표전화번호, 주소, 위경도, 사업자 상태, 표준산업분류코드 등을 제공한다.
    # 코나아이: I0000001    # 한국간편결제진흥원: I0000002    # 신한카드: I0000003    # 한국조폐공사: I0000004    # KT: I0000005    # 농협은행: I0000006    # 광주은행: I0000007    # 대구은행: I0000008    # ITS&G: I0000009    # NICE 정보통신: I0000010    # KIS 정보통신: I0000011    # 인조이웍스: I0000012    # KIS 정보통신(2): I0000014
    def __init__(self, date_num='', work_dir='data'):
        super().__init__(date_num, work_dir)
        self.src = 'card_fran'
        self.api_url = 'https://apis.data.go.kr/B190001/cardFranchisesV2/cardV2'


class FranHQ(DataGo):
    # 공정거래위원회_가맹정보_가맹본부 일반정보 상세 제공 서비스
    # 년도, 사업자번호, 법인등록번호를 통하여 사업자번호, 법인등록번호, 개인/법인 구분코드, 법인명, 우편번호, 소재지주소, 소재지 주소상세, 대표자명 등의 정보를 조회하는 기능
    # 목적 : fran 에 대한 본사 주소

    def __init__(self, date_num='', work_dir='data'):
        super().__init__(date_num, work_dir)
        self.src = 'ftc_hq'
        self.api_url = 'http://apis.data.go.kr/1130000/FftcJnghdqrtrsGnrlDtlService/getJnghdqrtrsGnrlDtl'


class FranHQHistory(DataGo):
    # 공정거래위원회_가맹정보_가맹본부 일반 정보 변경 이력 현황 제공 서비스
    # 가맹본부 일반 정보 변경 이력 현황 조회
    # 년도, 사업자등록번호, 법인등록번호 통하여 사업자번호, 법인등록번호, 개인/법인 구분, 법인명, 우편번호, 주소, 주소상세 등의 정보를 조회하는 기능

    # 목적 : fran 에 대한 본사 주소
    def __init__(self, date_num='', work_dir='data'):
        super().__init__(date_num, work_dir)
        self.src = 'ftc_hq_hist'
        self.api_url = 'http://apis.data.go.kr/1130000/FftcjnghdqrtrsInfoChghstService/getJnghdqrtrsInfoChghst'


class FranMas(DataGo):
    # 목적 : franchise 및 브랜드 추가용
    # 공정거래위원회_가맹정보_브랜드 목록 제공 서비
    # 브랜드 목록 조회
    # 년도를 통하여 가맹정보공개서등록번호, 사업자등록번호, 법인등록번호, 법인명, 브랜드명, 대표자명, 업종 대분류명, 업종 중분류명등의 정보를 조회하는 기능

    def __init__(self, date_num='', work_dir='data'):
        super().__init__(date_num, work_dir)
        self.src = 'fran_brand_mas'
        self.api_url = 'http://apis.data.go.kr/1130000/FftcBrandRlsInfoService/getBrandRlsInfo'


class FranBranch(DataGo):
    # 공정거래위원회_가맹정보_브랜드 가맹점 및 직영점 정보 제공 서비스
    # 브랜드 가맹점 및 직영점 정보 조회
    # 년도, 등록번호를 통하여 전체점포수, 지역명, 브랜드명, 직영점포수, 가맹점수, 업종대분류명, 업종중분류명 등의 정보를 조회하는 기능
    # 목적 : 적절한 fran 목록 뽑아보기
    def __init__(self, date_num='', work_dir='data'):
        super().__init__(date_num, work_dir)
        self.src = 'ftc_branch'
        self.api_url = 'http://apis.data.go.kr/1130000/FftcBrandFrcsDropInfoService/getBrandFrcsDropInfo'


class BrandComp(DataGo):
    # 공정거래위원회_가맹정보_브랜드 비교 목록 제공 서비스
    # 브랜드 비교 목록 조회
    # 년도, 등록번호를 이용하여 가맹정보공개서등록번호, 사업자등록번호, 법인등록번호, 법인명, 브랜드명, 대표자명, 업종대분류명, 업종중분류명, 가맹사업개시일자 등의 정보를 조회하는 기능
    # 목적 : fran <-> brand 에 대한 converter 리스트 추가용
    def __init__(self, date_num='', work_dir='data'):
        super().__init__(date_num, work_dir)
        self.src = 'brand_comp'
        self.api_url = 'http://apis.data.go.kr/1130000/FftcBrandCompInfoService/getBrandCompInfo'


class Bohum(DataGo):
    # 목적 : fran / none fran 등 모든 업체 사업자등록번호
    def __init__(self, date_num='', work_dir='data'):
        super().__init__(date_num, work_dir)
        self.src = 'bohom'
        self.api_url = 'http://apis.data.go.kr/B490001/gySjbPstateInfoService/getGySjBoheomBsshItem'
        self.p['resultType'] = 'xml'
        self.p['numOfRows'] = 100000


class DataGoApi:
    def __init__(self, date_n=None, work_dir='data'):
        if date_n is None:
            date_n = datetime.datetime.now().strftime('%m%d_%H%M')
        self.date_n = date_n
        self.b = Bohum(self.date_n, work_dir)
        self.brand = FranMas(self.date_n, work_dir)
        self.hq_addr = FranHQ(self.date_n, work_dir)
        self.branch = FranBranch(self.date_n, work_dir)
        self.card = CardFranchise(self.date_n, work_dir)
        self.hqh = FranHQHistory(self.date_n, work_dir)

    def mk_dir(self, work_dir='data'):
        wd = os.path.join(os.getcwd(), work_dir)
        if not os.path.isdir(wd):
            os.mkdir(wd)

    def set_year(self, year):
        print(f'Setting year to {year}')
        self.brand.p['yr'] = year
        self.hq_addr.p['yr'] = year
        self.branch.p['yr'] = year

    def download(self, work_dir='data', year=2023):
        self.mk_dir(work_dir)
        self.download_company_info()
        self.download_fran_info(year)

    def download_company_info(self):
        self.b.download()
        self.card.download()

    def download_fran_info(self, year=2023):
        self.set_year(year)
        self.brand.download()
        self.hq_addr.download()
        self.branch.download()

    def get_updated(self, prev_num, now_num=None):
        self.b.get_update(prev_num, now_num)
        self.brand.get_update(prev_num, now_num)
        self.hq_addr.get_update(prev_num, now_num)
        self.branch.get_update(prev_num, now_num)


if __name__ == '__main__':
    d_num = datetime.datetime.now().strftime('%m%d_%H%M')
    c = DataGoApi(d_num)
    for yr in range(2017, 2024):
        c.download_fran_info(yr)
    # c.download()
    #c.get_updated('0628_1954', d_num)
