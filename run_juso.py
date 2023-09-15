from api_kakao import KakaoApi
from api_vworld import VworldXy
import pandas as pd
import os
import datetime


def read_file(ifp, input_addr_col) -> pd.DataFrame:
    df = pd.read_csv(ifp, encoding='utf-8-sig', delimiter='|')
    df.dropna(subset=[input_addr_col], inplace=True)
    return df


def get_undone(idf: pd.DataFrame, fps: list, key_col: str):
    rdfs = [pd.read_csv(x, encoding='utf-8-sig') for x in fps]
    rdf = pd.concat(rdfs)
    undone = idf[~idf[key_col].isin(rdf[key_col])]
    return undone


if __name__ == '__main__':

    wd = 'data'
    input_fp = os.path.join(wd, 'brno_companies.txt')
    df = read_file(input_fp, 'addr', )
    prev_result_fps = [os.path.join(wd, x) for x in ['0914_0057_result.csv',
                                                     '0915_2052_juso_result.csv']]
    df = get_undone(df, prev_result_fps, 'id')

    ka = KakaoApi()
    ka.set_quota(91000)
    num = datetime.datetime.now().strftime('%m%d_%H%M')
    df = ka.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')

    v = VworldXy()
    v.set_quota(39950)
    num = datetime.datetime.now().strftime('%m%d_%H%M')
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')
    v.use_key(1)
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')
    v.use_key(2)
    df = v.add_api_col_to_csv(df, 'addr', wd, f'{num}_juso_result.csv')

