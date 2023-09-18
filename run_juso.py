from api_kakao import KakaoApi
from api_vworld import VworldXy
import helper as h
import os
import datetime


if __name__ == '__main__':

    wd = 'data'
    input_fp = os.path.join(wd, 'brno_companies.txt')
    df = h.read_file_and_drop_na(input_fp, ['addr'], '|')
    prev_result_fps = [os.path.join(wd, x) for x in ['0914_0057_result.csv',
                                                     '0915_2052_juso_result.csv', '0915_2105_juso_result.csv',
                                                     '0915_2300_juso_result.csv',
                                                     '0916_0003_juso_result.csv', '0916_0215_juso_result.csv',
                                                     '0917_2031_juso_result.csv', '0917_2236_juso_result.csv',
                                                     '0918_1015_juso_result.csv', '0918_1211_juso_result.csv',
                                                     '0918_0806_juso_result.csv']]
    df = h.get_undone(df, prev_result_fps, 'id')
    num = datetime.datetime.now().strftime('%m%d_%H%M')

    ka = KakaoApi()
    ka.set_quota(99900)
    df = ka.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')

    v = VworldXy()
    v.set_quota(39950)
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')
    v = VworldXy()
    v.use_key(1)
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')
    v = VworldXy()
    v.use_key(2)
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')

    ka = KakaoApi()
    ka.use_key(1)
    ka.set_quota(99000)
    df = ka.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')

    ka = KakaoApi()
    ka.use_key(2)
    ka.set_quota(99900)
    df = ka.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')

