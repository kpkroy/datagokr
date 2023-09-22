from api_kakao import KakaoApi
from api_vworld import VworldXy
import helper as h
import os
import datetime
import pandas as pd


if __name__ == '__main__':
    '''
    wd = 'data'
    input_fp = os.path.join(wd, 'brno_companies.txt')
    df = h.read_file_and_drop_na(input_fp, ['addr'], '|')
    prev_result_fps = [os.path.join(wd, x) for x in []]
    df = h.get_undone(df, prev_result_fps, 'id')
    '''
    wd = 'data'
    fn = 'brno_new.csv'
    df = pd.read_csv(os.path.join(wd, fn), encoding='utf-8-sig')
    num = datetime.datetime.now().strftime('%m%d_%H%M')
    print(f'running run_juso.py')
    print(f'using file {fn}')

    ka = KakaoApi()
    ka.set_quota(99900)
    df = ka.add_api_col_to_csv(df, 'frcs_addr', wd, f'{num}_juso_{fn}')

    ka = KakaoApi()
    ka.use_key(1)
    ka.set_quota(99000)
    df = ka.add_api_col_to_csv(df, 'frcs_addr', wd, f'{num}_juso_{fn}')

    ka = KakaoApi()
    ka.use_key(2)
    ka.set_quota(99900)
    df = ka.add_api_col_to_csv(df, 'frcs_addr', wd, f'{num}_juso_{fn}')

    v = VworldXy()
    v.set_quota(39950)
    df = v.add_api_col_to_csv(df, 'frcs_addr', wd, f'{num}_juso_{fn}')

    v = VworldXy()
    v.use_key(1)
    v.set_quota(39950)
    df = v.add_api_col_to_csv(df, 'frcs_addr', wd, f'{num}_juso_{fn}')

    v = VworldXy()
    v.use_key(2)
    v.set_quota(39950)
    df = v.add_api_col_to_csv(df, 'frcs_addr', wd, f'{num}_juso_{fn}')

