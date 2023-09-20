from api_juso import JusoAddr
import helper as h
import os
import pandas as pd


if __name__ == '__main__':
    wd = 'data'
    input_name = 'comp_addr_1.csv'
    print(input_name)

    df = pd.read_csv(os.path.join(wd, input_name),  encoding='utf-8-sig')
    prev_result_fps = [os.path.join(wd, x) for x in ['0918_1821_b_code_added.csv']]
    df = h.get_undone(df, prev_result_fps, 'company_id')

    a = JusoAddr()
    num = h.get_now_time()
    a.add_api_col_to_csv(df, 'ori', wd, f'{num}_bcode_{input_name}')

