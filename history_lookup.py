import pandas as pd
import numpy as np


def create_card_lookup_file():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    card = pd.read_csv('data/ori_input/all_card_fran.csv', encoding='utf-8-sig')
    card['addr'] = card['frcs_addr'] + ' ' + card['frcs_dtl_addr']
    card['addr'] = card['addr'].str.strip()

    # Step 1: Sort the DataFrame
    card = card.sort_values(by=['brno', 'crtr_ymd', 'lat', 'lot'], ascending=[True, False, False, False])
    # Step 2: Remove duplicates
    n_card = card.drop_duplicates(subset='brno', keep='first').copy()
    # Step 3: Create Lookup table
    lookup_df = card[card['lat'].notna()].sort_values(by='crtr_ymd', ascending=False).groupby(['brno', 'frcs_zip']).first().reset_index()

    # Merge the deduplicated dataframe with the lookup table
    merged = n_card.merge(lookup_df[['brno', 'frcs_zip', 'lat', 'lot']], on=['brno', 'frcs_zip'], how='left',
                                     suffixes=('', '_lookup'))
    # Fill NaN values using the lookup table
    merged['lat'].fillna(merged['lat_lookup'], inplace=True)
    merged['lot'].fillna(merged['lot_lookup'], inplace=True)

    # Drop the additional columns from the merge
    merged.drop(columns=['lat_lookup', 'lot_lookup'], inplace=True)
    n_card.drop(
        columns=['alt_text', 'ppr_frcs_aply_yn', 'bzmn_stts', 'onl_dlvy_ent_use_yn', 'pos_use_yn', 'bk_awa_perf_hd_yn',
                 'te_gds_hd_yn', 'qr_reg_conm'], inplace=True)
    n_card.to_csv('data/brno/lookup.csv', index=False, encoding='utf-8-sig')


def update_lookup():
    df = pd.read_csv('data/lookup/lookup_juso.csv', encoding='utf-8-sig')
    df['x'].fillna(df['lot'], inplace=True)
    df['y'].fillna(df['lat'], inplace=True)

    use_col = 'refined_'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'road'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'addr'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'frcs_addr'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', 'region_code'], how='any').drop_duplicates(subset=['depth1', 'depth2', 'depth3'], keep='first')
    for col in ['region_code']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    df.to_csv('data/lookup/lookup_juso_refi.csv', index=False, encoding='utf-8-sig')

    q = df[df['x'].isna()]
    q.to_csv('data/lookup/lookup_juso_refi_xy.csv', encoding='utf-8-sig', index=False)
    # run_juso ()
    q = df[df['region_code'].isna()]
    q.to_csv('data/lookup/lookup_juso_refi_bcode.csv', encoding='utf-8-sig', index=False)
    q = pd.read_csv('data/lookup/lookup_juso_refi_bcode.csv', encoding='utf-8-sig')
    rename_cols = ['region_code', 'depth1', 'depth2', 'depth3', 'road', 'parcel', 'refined', 'road', 'parcel']
    q.rename(columns={x: 'ori_' + x for x in rename_cols}, inplace=True)
    q.to_csv('data/lookup/lookup_juso_refi_bcode.csv', encoding='utf-8-sig', index=False)
    # run_bcode()


def match_lookup_with_brno():
    dir_path = os.path.join(os.getcwd(), 'data', 'lookup')
    file_str = 'bcode_lookup_juso_refi_bcode'
    file_names = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if file_str in f]
    dfs = [pd.read_csv(x, encoding='utf-8-sig') for x in file_names]
    df = pd.concat(dfs)
    df['region_code'].fillna(df['region_code.1'], inplace=True)
    df['depth1'].fillna(df['depth1.1'], inplace=True)
    df['depth2'].fillna(df['depth2.1'], inplace=True)
    df['depth3'].fillna(df['depth3.1'], inplace=True)
    df['road'].fillna(df['road.1'], inplace=True)
    df['parcel'].fillna(df['parcel.1'], inplace=True)
    df['x'].fillna(df['x.1'], inplace=True)
    df['y'].fillna(df['y.1'], inplace=True)

    df.drop(columns=['parcel.1', 'road.1', 'category.1', 'x.1', 'y.1', 'src.1', 'src', 'Unnamed: 0',
                     'depth1.1', 'depth2.1', 'depth3.1', 'region_code.1'], inplace=True)
    df.to_csv('data/lookup/lookup_juso_refi_bcode_done_a.csv', encoding='utf-8-sig', index=False)
    # a part

    q = df[df['region_code'].isna()]
    q.drop(columns=['ori_region_code', 'ori_depth1', 'ori_depth2', 'ori_depth3', 'depth1', 'depth2', 'depth3',
                    'region_code'], inplace=True)
    q.to_csv('data/lookup/bcode_lookup_juso_refi_bcode_retry.csv', encoding='utf-8-sig', index=False)
    # run_juso on q, using 'frcs_addr' :  결과:  __juso_bcode_lookup_juso_refi_bcode_retry.csv  -> b part


def merge_all_lookup():
    a = pd.read_csv('data/lookup/lookup_juso_refi_bcode_done_a.csv', encoding='utf-8-sig')
    b = pd.read_csv('data/lookup/__juso_bcode_lookup_juso_refi_bcode_retry.csv', encoding='utf-8-sig')
    c = pd.read_csv('data/lookup/_juso_lookup_juso_refi_xy.csv', encoding='utf-8-sig')
    d = pd.read_csv('data/lookup/lookup_juso.csv', encoding='utf-8-sig')
    df = pd.concat([a, b, c, d])

    # ori = pd.read_csv('data/lookup/lookup.csv', encoding='utf-8-sig')
    # not_processed = ori[~ori['brno'].isin(df['brno'])]
    # len(not_processed)  ->  0

    # df columns = Index(['brno', 'bzmn_stts_nm', 'crtr_ymd', 'frcs_addr', 'frcs_dtl_addr',
    #        'frcs_nm', 'frcs_zip', 'lat', 'lot', 'addr', 'road', 'title', 'parcel',
    #        'x', 'y', 'category', 'refined_', 'title.1', 'zipNo', 'used_addr',
    #        'ori_road', 'ori_parcel', 'road.1', 'title.2', 'parcel.1',
    #        'region_code', 'depth1', 'depth2', 'depth3', 'x.1', 'y.1', 'category.1',
    #        'src', 'zone_no', 'used_addr.1', 'depth1.1', 'depth2.1', 'depth3.1',
    #        'src.1', 'region_coderoad', 'frcs_reg_se', 'frcs_reg_se_nm',
    #        'frcs_rprs_telno', 'frcs_stlm_info_se', 'frcs_stlm_info_se_nm',
    #        'ksic_cd', 'ksic_cd_nm', 'pvsn_inst_cd', 'usage_rgn_cd'],
    #       dtype='object')
    # ['used_addr.1', 'title.1', 'title.2', 'src.1', 'depth3.1' 'x.1' 'parcel.1' , 'road.1'

    # a = df[~(df['used_addr'].isna() & df['used_addr.1'].isna())]
    # b = a[a['used_addr'] != a['used_addr.1']]

    # len(df[df['used_addr'].isna() & ~df['used_addr.1'].isna()])   -> 0
    # len(df[(df['x'] != df['x.1']) & ~df['x.1'].isna()])  ->0

    df['title'].fillna(df['title.1'], inplace=True)
    df['title'].fillna(df['title.2'], inplace=True)
    df['x'].fillna(df['x.1'], inplace=True)
    df['y'].fillna(df['y.1'], inplace=True)
    df['parcel'].fillna(df['parcel.1'], inplace=True)
    df['road'].fillna(df['road.1'], inplace=True)
    df['zone_no'].fillna(df['zipNo'], inplace=True)
    df['region_code'].fillna(df['region_coderoad'], inplace=True)
    df['used_addr'].fillna(df['used_addr.1'], inplace=True)
    df.drop(columns=['used_addr.1', 'title.1', 'title.2', 'x.1', 'y.1', 'src.1', 'parcel.1', 'road.1',
                     'category.1', 'depth1.1', 'depth2.1', 'depth3.1',
                     'region_coderoad', 'zipNo'], inplace=True)

    df['ori_road'].fillna(df['road'], inplace=True)
    df['ori_parcel'].fillna(df['parcel'], inplace=True)
    df['used_addr'].fillna(df['refined_'], inplace=True)
    df.drop(columns=['road', 'parcel', 'refined_'], inplace=True)

    df['x'].fillna(df['lot'], inplace=True)
    df['y'].fillna(df['lat'], inplace=True)
    df['zone_no'].fillna(df['frcs_zip'], inplace=True)
    df.to_csv('data/lookup/lookup_juso_merged.csv', encoding='utf-8-sig', index=False)


def fill_by_peer():
    df = pd.read_csv('data/lookup/lookup_juso_merged.csv', encoding='utf-8-sig')

    # fill depth
    use_col = 'used_addr'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'ori_road'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'ori_parcel'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col],
                      how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'zone_no'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col],
                      how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'region_code'
    df_ok = df.dropna(subset=['depth1', 'depth2', 'depth3', use_col],
                      how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['depth1', 'depth2', 'depth3']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    # fill region_code
    use_col = 'used_addr'
    df_ok = df.dropna(subset=['region_code', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['region_code']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'ori_road'
    df_ok = df.dropna(subset=['region_code', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['region_code']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'ori_parcel'
    df_ok = df.dropna(subset=['region_code', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['region_code']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    use_col = 'zone_no'
    df_ok = df.dropna(subset=['region_code', use_col], how='any').drop_duplicates(subset=[use_col], keep='first').set_index(use_col)
    for col in ['region_code']:
        df.loc[df[col].isna(), 'src'] = 'filled_by_ref'
        df.loc[df[col].isna(), col] = df[use_col].map(df_ok[col])

    # sort and remove duplicate
    rdf = df.sort_values(by=['x', 'region_code', 'depth1'], na_position='last').drop_duplicates(subset='brno', keep='first')
    rdf.to_csv('data/lookup/lookup_juso_filled.csv', encoding='utf-8-sig', index=False)
    q = rdf[rdf['depth1'].isna()]
    q.to_csv('data/lookup/lookup_juso_filled_nodepth.csv', encoding='utf-8-sig', index=False)
    # manually filled region code on [q]
    q = pd.read_csv('data/lookup/lookup_juso_filled_nodepth.csv', encoding='utf-8-sig')
    rdf = pd.concat([rdf, q])
    rdf = rdf.sort_values(by=['region_code', 'depth1', 'x'], na_position='last').drop_duplicates(subset='brno',
                                                                                                keep='first')
    rdf.to_csv('data/lookup/lookup_juso_filled.csv', encoding='utf-8-sig', index=False)


def add_alt_text():
    # add alt_text to lookup, lookup_juso_filled.csv
    lu = pd.read_csv('data/lookup/lookup.csv', encoding='utf-8-sig')
    ac = pd.read_csv('data/ori_input/all_card_fran.csv', encoding='utf-8-sig')
    ac = ac.drop_duplicates()
    ac = ac.sort_values(by='usage_rgn_cd', ascending=False)
    ac = ac.drop_duplicates(subset=['brno', 'crtr_ymd', 'frcs_addr', 'pvsn_inst_cd', 'lat'], keep='first')
    # Identify rows in ac that are still duplicated based on the specified columns
    # duplicated_rows = ac[ac.duplicated(subset=['brno', 'crtr_ymd', 'frcs_addr', 'pvsn_inst_cd', 'lat'], keep=False)]
    # If needed, sort the duplicated_rows for easier viewing
    # duplicated_rows = duplicated_rows.sort_values(by=['brno', 'crtr_ymd', 'frcs_addr', 'pvsn_inst_cd', 'lat'])
    ac.to_csv('data/ori_input/all_card_fran_unique.csv', encoding='utf-8-sig', index=False)
    lu = lu.merge(ac[['brno', 'crtr_ymd', 'frcs_addr', 'pvsn_inst_cd', 'lat', 'alt_text']],
                  on=['brno', 'crtr_ymd', 'frcs_addr', 'pvsn_inst_cd', 'lat'],
                  how='left')
    lu.to_csv('data/lookup/lookup.csv', encoding='utf-8-sig', index=False)
    # duplicates = lu[lu['brno'].duplicated(keep=False)].sort_values(by='brno')
    # len(duplicates) == 0

    lj = pd.read_csv('data/lookup/lookup_juso_filled.csv', encoding='utf-8-sig')
    lj.drop(
        columns=['Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32',
                 'Unnamed: 33',
                 'Unnamed: 34'], inplace=True)
    lj.drop(columns=['src'], inplace=True)
    lj['region_code'].fillna(lj['ori_region_code'], inplace=True)
    lj.drop(columns=['ori_region_code', 'ori_depth1', 'ori_depth2', 'ori_depth3'], inplace=True)
    lj.rename(columns={'ori_road': 'road', 'ori_parcel': 'parcel'}, inplace=True)
    lju = lj.merge(lu[['brno', 'frcs_reg_se_nm', 'frcs_rprs_telno', 'frcs_stlm_info_se_nm', 'ksic_cd', 'pvsn_inst_cd', 'usage_rgn_cd', 'alt_text']], on=['brno'], how='left')
    lju.to_csv('data/lookup/lookup_juso_filled_done.csv', encoding='utf-8-sig', index=False)
