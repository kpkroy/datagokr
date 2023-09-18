from api_juso import JusoAddr
import pandas as pd
import os


if __name__ == '__main__':
    wd = 'data'
    ifp = 'company_tb.csv'

    fp = os.path.join(wd, ifp)
    df = pd.read_csv(fp, encoding='utf-8-sig')
    df.dropna(subset=['addr'], inplace=True)

    a = JusoAddr()
    a.add_api_col_to_csv(df, 'addr', wd, 'b_code_added.csv')
