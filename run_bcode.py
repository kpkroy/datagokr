from api_juso import JusoAddr
import pandas as pd
import os


if __name__ == '__main__':
    wd = 'data'
    ifp = 'company_addresses.txt'

    fp = os.path.join(wd, ifp)
    df = pd.read_csv(fp, encoding='utf-8-sig', delimiter='|')
    df.dropna(subset=['ori'], inplace=True)

    a = JusoAddr()
    a.add_api_col_to_csv(df, 'ori', wd, 'b_code_added.csv')
