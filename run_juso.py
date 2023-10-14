from api_kakao import KakaoApi
from api_vworld import VworldXy
import helper as h
import os
import datetime
import pandas as pd
import numpy as np


if __name__ == '__main__':
    wd = 'data/brno'
    fn = '5_brno_retry_juso.csv'

    df = pd.read_csv(os.path.join(wd, fn), encoding='utf-8-sig')
    print(f'length of df {len(df)}')

    # num = datetime.datetime.now().strftime('%m%d_%H%M')
    num = ''
    print(f'running run_juso.py')
    print(f'using file {fn}')

    df.replace({np.nan: None}, inplace=True)
    addr_col_name = 'addr'

    ka = KakaoApi()
    ka.use_key(1)
    ka.set_quota(99950)
    df = ka.add_api_col_to_csv(df, addr_col_name, wd, f'{num}_juso_{fn}')

    ka = KakaoApi()
    ka.use_key(2)
    ka.set_quota(99950)
    df = ka.add_api_col_to_csv(df, addr_col_name, wd, f'{num}_juso_{fn}')

    ka = KakaoApi()
    ka.set_quota(99950)
    df = ka.add_api_col_to_csv(df, addr_col_name, wd, f'{num}_juso_{fn}')

    v = VworldXy()
    v.set_quota(30000)
    df = v.add_api_col_to_csv(df, addr_col_name, wd, f'{num}_juso_{fn}')

    v = VworldXy()
    v.use_key(1)
    v.set_quota(39950)
    df = v.add_api_col_to_csv(df, addr_col_name, wd, f'{num}_juso_{fn}')

    v = VworldXy()
    v.use_key(2)
    v.set_quota(39950)
    df = v.add_api_col_to_csv(df, addr_col_name, wd, f'{num}_juso_{fn}')
