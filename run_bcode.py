from api_juso import JusoAddr
import helper as h
import os
import pandas as pd
import argparse
import numpy as np


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--d", help="working directory.", type=str, default='data')
    parser.add_argument("--i", help="[INPUT] file name body", type=str)
    parser.add_argument("--n", help="id column to check for prev", type=str)
    parser.add_argument("--c", help='main address column', type=str, default='addr')
    parser.add_argument("--k", help="key column", type=str)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arg()
    wd = args.d
    file_body_name = args.i
    file_num = args.n
    addr_col = args.c
    key_column = args.k

    if file_num:
        input_name = f'{file_body_name}_{file_num}.csv'
    else:
        input_name = file_body_name + '.csv'
    output_name = f'bcode_{input_name}'
    print(f'input file name is {input_name}')

    df = pd.read_csv(os.path.join(wd, input_name),  encoding='utf-8-sig')
    print(f'length of ori {len(df)}')
    out_path = os.path.join(os.getcwd(), wd, output_name)

    if os.path.isfile(out_path) and key_column:
        df = h.get_undone(df, [out_path], key_column)

    df.replace({np.nan: None}, inplace=True)
    print(f'length of undone {len(df)}')
    a = JusoAddr()
    a.add_api_col_to_csv(df, addr_col, wd, output_name)


# venv\scripts\python run_bcode.py --d data/brno --i 2_brno_juso_coordo_redo --n 0   <--when file num exists
# venv\scripts\python run_bcode.py --d data/brno --i 5_brno_retry_juso     <-- when file num does not exist