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
        self.page_name = 'pageNo'
        self.page_per_name = 'numOfRows'
        self.p = {'serviceKey': self.token, self.page_per_name: 10000, 'resultType': 'json'}

    def get_elapsed(self):
        pass

    def download(self, start_page_num=0):
        page_num = start_page_num
        df = pd.DataFrame()
        while True:
            page_num += 1
            self.p[self.page_name] = page_num
            new_df = self.get_response()
            df = pd.concat([df, new_df])
            print(f'....[{self.src}] - {page_num}: {len(new_df)} rows downloaded')
            if len(new_df) < self.p[self.page_per_name]:
                break
        self.export_to_work_dir(df)

    def get_update(self, prev_num, now_num=None):
        if now_num is None:
            now_num = self.date_num
        fp_now = self.get_fpath(now_num)
        fp_prev = self.get_fpath(prev_num)
        fp_updated = self.get_fpath(self.date_num, '_update')
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

    def get_fpath(self, date_num=None, post_fix=''):
        if date_num is None:
            date_num = self.date_num
        fn = f'{date_num}_{self.src}{post_fix}.csv'
        fp = os.path.join(self.work_dir, fn)
        return fp

    def export_to_work_dir(self, df: pd.DataFrame):
        fp = self.get_fpath()
        if os.path.isfile(fp):
            df.to_csv(fp, encoding='utf-8-sig', index=False, mode='a', header=False)
        else:
            df.to_csv(fp, encoding='utf-8-sig', index=False)
        print(f'....[{self.src}] - {fp}: {len(df)} rows exported')


class CardFranchise(DataGo):
    # 한국조폐공사_카드_가맹점기본정보
    # 지역사랑상품권 운영대행사들로부터 수집한 카드 가맹점에 대한 가맹점명, 대표전화번호, 주소, 위경도, 사업자 상태, 표준산업분류코드 등을 제공한다.
    # 코나아이: I0000001    # 한국간편결제진흥원: I0000002    # 신한카드: I0000003    # 한국조폐공사: I0000004    # KT: I0000005    # 농협은행: I0000006    # 광주은행: I0000007    # 대구은행: I0000008    # ITS&G: I0000009    # NICE 정보통신: I0000010    # KIS 정보통신: I0000011    # 인조이웍스: I0000012    # KIS 정보통신(2): I0000014
    def __init__(self, date_num='', work_dir='data'):
        super().__init__(date_num, work_dir)
        self.page_name = 'pageNo'
        self.page_per_name = 'numOfRows'
        self.p = {'serviceKey': self.token, self.page_per_name: 10000}
        self.src = 'card_fran'
        self.api_url = 'https://apis.data.go.kr/B190001/localFranchises/franchise'


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


class FranBrand(DataGo):
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
        self.api_url = 'https://apis.data.go.kr/1130000/FftcBrandFrcsStatsService/getBrandFrcsStats'


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
        self.brand = FranBrand(self.date_n, work_dir)
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

    def download(self, work_dir='data'):
        self.mk_dir(work_dir)
        self.b.download()
        self.card.download()
        self.download_fran_info()

    def download_fran_branch_count(self):
        for y in range(2017, 2024):
            self.branch.p['yr'] = y
            self.branch.download()
        h.rm_duplicates_from_file(self.branch.get_fpath(), sort_by=['yr'], drop_duplicates_by=['jngIfrmpRgsno'])

    def download_fran_info(self, start_year=2017, end_year=2024):
        for y in range(2017, 2024):
            self.set_year(y)
            self.brand.download()
            self.hq_addr.download()  # Need only the last
        h.rm_duplicates_from_file(self.hq_addr.get_fpath(), sort_by=['yr'], drop_duplicates_by=['brno'])
        h.rm_duplicates_from_file(self.brand.get_fpath(), sort_by=['yr'], drop_duplicates_by=['brno', 'brandNm'])

    def get_updated(self, prev_num, now_num=None):
        self.b.get_update(prev_num, now_num)
        self.brand.get_update(prev_num, now_num)
        self.hq_addr.get_update(prev_num, now_num)


if __name__ == '__main__':
    d_num = datetime.datetime.now().strftime('%m%d_%H%M')
    c = DataGoApi(d_num)
    c.card.download()
    print('a')
    # c.download_fran_info()
    # c.download()
    #c.get_updated('0628_1954', d_num)
