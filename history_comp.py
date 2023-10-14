import helper as h
import pandas as pd
import numpy as np
import os


def merge_all_regioncode_result():
    rfp = h.get_file_path_list_with_suffix('data/c_addresses/b_code', 'b_code_added.csv')
    dfs = [pd.read_csv(x, encoding='utf-8-sig') for x in rfp]
    df = pd.concat(dfs)
    df = df.sort_values(by=['geo_lat', 'region_code'], na_position='last').drop_duplicates(subset='id', keep='first')
    df.to_csv('data/c_addresses/comp_1.csv', encoding='utf-8-sig', index=False)


def make_company_addresses():
    cdf = pd.read_csv('data/comp_1.csv', encoding='utf-8-sig')
    cdf.drop(columns=['Unnamed: 0'], inplace=True)
    cdf['refined'] = np.nan
    cdf['refined'].fillna(cdf['road'], inplace=True)
    cdf['refined'].fillna(cdf['parcel'], inplace=True)
    use_cols = ['company_id', 'ori', 'geo_lat', 'geo_long', 'region_code', 'depth1', 'depth2', 'depth3', 'road', 'parcel', 'refined', 'src']
    cdf = cdf[use_cols]
    cdf = cdf.sort_values(by=['refined', 'region_code'], na_position='last').drop_duplicates(subset='company_id', keep='first')
    cdf.to_csv('data/comp_2.csv', encoding='utf-8-sig', index=False)


def redo_company_2():
    df = pd.read_csv('data/c_addresses/comp_2.csv', encoding='utf-8-sig')
    # make irregular numbers as Np.nan
    condition = (df['geo_long'] < 120) | (df['geo_long'] > 130)
    df.loc[condition, ['geo_long', 'geo_lat']] = np.nan

    # try to look for same address with ok lat/long. for same address, fill.
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', 'refined'], how='any').drop_duplicates(subset='refined', keep='first').set_index('refined')
    for col in ['depth1', 'depth2', 'depth3']:
        mapping = df['refined'].map(df_ok[col])
        # Find indexes where df[col] is NaN and mapped value is not NaN
        idx_to_fill = df.index[df[col].isna() & mapping.notna()]
        # Update values for the given column and 'src' column
        df.loc[idx_to_fill, col] = mapping[idx_to_fill]
        df.loc[idx_to_fill, 'src'] = 'filled_by_dep_ref'

    df_ok = df.dropna(subset=['geo_long', 'geo_lat', 'refined'], how='any').drop_duplicates(subset='refined', keep='first').set_index('refined')
    for col in ['geo_long', 'geo_lat']:
        mapping = df['refined'].map(df_ok[col])
        # Find indexes where df[col] is NaN and mapped value is not NaN
        idx_to_fill = df.index[df[col].isna() & mapping.notna()]
        # Update values for the given column and 'src' column
        df.loc[idx_to_fill, col] = mapping[idx_to_fill]
        df.loc[idx_to_fill, 'src'] = 'filled_by_geo_ref'

    df_ok = df.dropna(subset=['region_code', 'refined'], how='any').drop_duplicates(subset='refined', keep='first').set_index('refined')
    for col in ['region_code']:
        mapping = df['refined'].map(df_ok[col])
        # Find indexes where df[col] is NaN and mapped value is not NaN
        idx_to_fill = df.index[df[col].isna() & mapping.notna()]
        # Update values for the given column and 'src' column
        df.loc[idx_to_fill, col] = mapping[idx_to_fill]
        df.loc[idx_to_fill, 'src'] = 'filled_by_reg_ref'

    df_ok = df.dropna(subset=['region_code', 'depth1', 'depth2', 'depth3'], how='any').drop_duplicates(subset=['depth1', 'depth2', 'depth3'], keep='first').set_index(['depth1', 'depth2', 'depth3'])
    for col in ['region_code']:
        df['composite_key'] = list(zip(df['depth1'], df['depth2'], df['depth3']))

        # Generate the mapping for the column
        mapping = df['composite_key'].map(df_ok[col])

        # Find indexes where df[col] is NaN and mapped value is not NaN
        idx_to_fill = df.index[df[col].isna() & mapping.notna()]

        # Update values for the given column and 'src' column
        df.loc[idx_to_fill, col] = mapping[idx_to_fill]
        df.loc[idx_to_fill, 'src'] = 'filled_by_reg_dep'
    df.drop('composite_key', axis=1, inplace=True)

    condition = df['geo_long'].isna()
    df_bad = df[condition]
    rename_cols = ['region_code', 'depth1', 'depth2', 'depth3', 'road', 'parcel', 'refined', 'road', 'parcel']
    df_bad.rename(columns={x: 'ori_' + x for x in rename_cols}, inplace=True)
    df_bad.to_csv('data/c_addresses/company_noxy.csv', index=False, encoding='utf-8-sig')
    df.rename(columns={'geo_lat': 'y', 'geo_long': 'x'}, inplace=True)
    # run_juso (company_noxy.csv) -> '_xy__company_noxy.csv'
    df_r = pd.read_csv('data/c_addresses/_xy__juso_company_noxy.csv', encoding='utf-8-sig')
    df_r = df_r.dropna(subset=['y', 'x'])

    df.set_index(['id', 'company_id'], inplace=True)
    df_r.set_index(['id', 'company_id'], inplace=True)
    cols_to_update = ['y', 'x', 'region_code', 'road', 'parcel']
    df.update(df_r[cols_to_update])
    # Reset index
    df.reset_index(inplace=True)
    df.to_csv('data/c_addresses/comp_3.csv', encoding='utf-8-sig', index=False)


def add_depth_company():
    q = pd.read_csv('data/c_addresses/comp_3.csv', encoding='utf-8-sig')
    depth = q[q['depth1'].notna()]
    depth['refined'].fillna(depth['road'], inplace=True)
    depth['refined'].fillna(depth['parcel'], inplace=True)
    depth['refined'].fillna(depth['ori'], inplace=True)
    depth.drop(columns=['road', 'parcel', 'ori'], inplace=True)
    depth['zone_no'] = None
    depth.to_csv('data/c_addresses/comp_3_depth_done.csv', encoding='utf-8-sig', index=False)

    nodepth = q[q['depth1'].isna()]
    nodepth.drop(columns=['depth1', 'depth2', 'depth3', 'region_code'], inplace=True)
    # run bcode
    brno_dir = os.path.join('data', 'c_addresses')
    bcode_fps = [os.path.join(brno_dir, f) for f in os.listdir(brno_dir) if 'comp_3_nodepth_' in f]
    bcode_dfs = [pd.read_csv(x, encoding='utf-8-sig') for x in bcode_fps]
    bcode_df = pd.concat(bcode_dfs)
    bcode_df['used_addr'].fillna(bcode_df['refined'], inplace=True)
    bcode_df['used_addr'].fillna(bcode_df['parcel.1'], inplace=True)
    bcode_df['used_addr'].fillna(bcode_df['road.1'], inplace=True)
    bcode_df['used_addr'].fillna(bcode_df['road'], inplace=True)
    bcode_df['used_addr'].fillna(bcode_df['parcel'], inplace=True)
    bcode_df['used_addr'].fillna(bcode_df['ori'], inplace=True)
    bcode_df.drop(columns=['Unnamed: 0', 'refined', 'ori', 'parcel.1', 'parcel', 'road.1', 'road', 'category', 'x.1', 'y.1', 'title', 'src.1'], inplace=True)
    bcode_df.rename(columns={'used_addr': 'refined', 'zipNo': 'zone_no'}, inplace=True)
    bcode_df.to_csv('data/c_addresses/comp_3_nodepth_done.csv', encoding='utf-8-sig', index=False)


def create_addr_table():
    t1 = pd.read_csv('data/c_addresses/comp_3_depth_done.csv', encoding='utf-8-sig')
    t2 = pd.read_csv('data/c_addresses/comp_3_nodepth_done.csv', encoding='utf-8-sig')
    t = pd.concat([t1, t2])
    t = t.drop(columns=['id'])
    comp = pd.read_csv('data/ori_input/companies.txt', delimiter='|')
    tm = t.merge(comp[['id', 'company_id']], on='company_id', how='inner')
    tm.drop(columns=['company_id'], inplace=True)
    tm.rename(columns={'id': 'company_id'}, inplace=True)
    tm.to_csv('data/c_addresses/comp_addr.csv', encoding='utf-8-sig', index=False)


'''
def redo_company():
    cdf = pd.read_csv('data/c_addresses/companies_region.csv', encoding='utf-8-sig')
    cdf_ok = cdf[~(cdf['depth1'].isna())]
    cdf_ok.rename(columns={'geo_lat': 'y', 'geo_long': 'x'}, inplace=True)
    cdf_ok['refined'].fillna(cdf['road'], inplace=True)
    cdf_ok['refined'].fillna(cdf['parcel'], inplace=True)
    cdf_ok.to_csv('data/valid_company.csv', index=False, encoding='utf-8-sig')

    fdf = cdf[cdf['geo_long'] == -1]
    fdf.to_csv('data/notvalid_company.csv', encoding='utf-8-sig', index=False)
    # run_juso(fdf) : _xy__juso_company_noxy.csv
    fdf2 = cdf[cdf['depth1'].isna()]
    fdf2.to_csv('data/notvalid_company2.csv', index=False, encoding='utf-8-sig')
    # run_juso(fdf2) + run_juso(fdf)
    finished = ['0925_1643_juso_notvalid_company.csv',
                '0925_1655_juso_notvalid_company2.csv',
                '0925_1643_juso_notvalid_company.csv',
                '0925_1836_juso_notvalid_company2.csv',
                '0926_1002_juso_notvalid_company2.csv',
                '0926_1108_juso_notvalid_company2.csv',
                '0926_1354_juso_notvalid_company2.csv']
    ff = pd.concat([pd.read_csv('data/'+x, encoding='utf-8-sig') for x in finished])
    ff['refined'].fillna(cdf['road'], inplace=True)
    ff['refined'].fillna(cdf['parcel'], inplace=True)

    use_cols = ['company_id', 'refined', 'region_code', 'depth1', 'depth2', 'depth3', 'y', 'x', 'src']
    ca = pd.concat([ff, cdf_ok])
    ca = ca[use_cols]
    ca = ca.sort_values(by=['refined', 'region_code', 'x'], na_position='last').drop_duplicates(subset='company_id',
                                                                                                keep='first')
    tester = ca[ca['x'].isna()]
    print(len(tester))
    ca.to_csv('data/c_addresses/comp_addr_.csv', index=False, encoding='utf-8-sig')


def supplement_comp_addr():
    df = pd.read_csv('data/c_addresses/comp_addr.csv', encoding='utf-8-sig')
    reference_df = df.dropna(subset=['region_code', 'depth1', 'depth2', 'depth3', 'y', 'x'], how='all').drop_duplicates(subset='refined', keep='first').set_index('refined')

    # Fill NaN values using the dictionary
    for col in ['region_code', 'depth1', 'depth2', 'depth3', 'y', 'x']:
        df.loc[df[col].isna(), col] = df['refined'].map(reference_df[col])

    tester = df[df['x'].isna()]         # should be less than prev tester
    print(len(tester))
    df.to_csv('data/c_addresses/comp_addr_refilled.csv', index=False, encoding='utf-8-sig')
'''
