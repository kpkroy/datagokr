from api_kakao import KakaoApi
from api_vworld import VworldXy
import helper as h
import os
import datetime


if __name__ == '__main__':

    wd = 'data'
    input_fp = os.path.join(wd, 'brno_companies.txt')
    df = h.read_file_and_drop_na(input_fp, ['addr'])
    prev_result_fps = [os.path.join(wd, x) for x in ['0914_0057_result.csv',
                                                     '0915_2052_juso_result.csv', '0915_2105_juso_result.csv',
                                                     '0915_2300_juso_result.csv', '0916_0003_juso_result.csv',
                                                     '0916_0215_juso_result.csv', '0917_2031_juso_result',
                                                     '0917_2236_juso_result']]
    df = h.get_undone(df, prev_result_fps, 'id')

    ka = KakaoApi()
    ka.set_quota(99900)
    num = datetime.datetime.now().strftime('%m%d_%H%M')
    df = ka.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')

    v = VworldXy()
    v.set_quota(39950)
    num = datetime.datetime.now().strftime('%m%d_%H%M')
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')
    v = VworldXy()
    v.use_key(1)
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')
    v = VworldXy()
    v.use_key(2)
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')

