import os
import json
import pandas as pd
import numpy as np


def merge_card_json(dir_path, prefix):
    fps = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.startswith(prefix)]
    rdfs = []
    for fp in fps:
        with open(fp, 'r', encoding='utf-8-sig') as j:
            data = json.load(j)
        rdfs.append(pd.DataFrame(data.get('data')))
    rdf = pd.concat(rdfs)
    rdf.to_csv(os.path.join(dir_path, 'all_card_fran.csv'), encoding='utf-8-sig', index=False)


def result_separate_by_coord():
    # brn = pd.read_csv('data/ori_input/brno_companies.txt', encoding='utf-8-sig', delimiter='|')   # ori
    # run juso on brno_companies.txt  -> 1_brno_juso.csv
    brn_j = pd.read_csv('data/brno/1_brno_juso.csv', encoding='utf-8-sig')              # 1
    j_use = ['id', 'brno', 'region_code', 'depth1', 'depth2', 'depth3', 'x', 'y', 'road', 'parcel', 'refined', 'type',
             'src']
    brn_j = brn_j[j_use]
    brn_j = brn_j[brn_j['x'].notna()]  # brn_j should have rows that has value in column 'x'
    brn_j.to_csv('data/brno/2_brno_juso_coordo.csv', encoding='utf-8-sig', index=False)     # has coord


def add_region_to_coordo():
    # from brno_juso_valid.csv, there are those without region_code.
    cdf = pd.read_csv('data/brno/2_brno_juso_coordo.csv', encoding='utf-8-sig')  # has coord.
    # run bcode on cdf[cdf['region_code'].isna()]
    ndf = pd.read_csv('data/brno/2_brno_juso_coordo_add_region.csv', encoding='utf-8-sig')  # region added
    ca = pd.concat([cdf, ndf])
    ca = ca.sort_values(by=['refined', 'region_code', 'x'], na_position='last').drop_duplicates(subset='company_id',
                                                                                                keep='first')
    ca.to_csv('data/brno/2_brno_juso_coordo_done_1.csv', index=False, encoding='utf-8-sig')

    # add original brno_comp to ca
    ori = pd.read_csv('data/ori_input/brno_companies.txt', encoding='utf-8-sig', delimiter='|')
    mrg = pd.merge(ori, ca, left_on=['id'], right_on=['brno_company_id'], how='inner')
    use_cols = ['id', 'brno_x', 'company_name', 'addr', 'upjong_code', 'upjong_name',
                'fran', 'fran_id', 'cate_name', 'cate_code', 'src_from', 'is_omitted',
                'is_deleted', 'region_code', 'depth1', 'depth2', 'depth3', 'x', 'y', 'road', 'parcel',
                'refined']
    mrg = mrg[use_cols]
    mrg.rename(columns={'id': 'brno_company_id', 'brno_x': 'brno'}, inplace=True)
    mrg.to_csv('data/brno/2_brno_juso_coordo_done_2.csv', index=False, encoding='utf-8-sig')

    q = ca[['brno_company_id', 'brno', 'road', 'parcel', 'refined']]
    q.to_csv('data/brno/2_brno_juso_coordo_redo.csv', encoding='utf-8-sig', index=False)
    dir_w = os.path.join('data', 'brno')
    fps = [os.path.join(dir_w, f) for f in os.listdir(dir_w) if '_2_brno_juso_coordo_redo' in f]
    zone_df = pd.concat([pd.read_csv(f, encoding='utf-8-sig') for f in fps])

    mrg = mrg.merge(zone_df[['brno_company_id', 'zone_no', 'used_addr']], on='brno_company_id', how='inner')
    mrg.to_csv('data/brno/2_brno_juso_coordo_done_3.csv', index=False, encoding='utf-8-sig')


def split_coord_no_into_two():
    brn = pd.read_csv('data/ori_input/brno_companies.txt', encoding='utf-8-sig', delimiter='|')   # ori
    brn_coord = pd.read_csv('data/brno/2_brno_juso_coordo.csv', encoding='utf-8-sig')  # has coord.

    # Get IDs that are in brn but not in brn_j
    brn.rename(columns={'id': 'brno_company_id'}, inplace=True)
    no_coord_id = set(brn['brno_company_id']) - set(brn_coord['brno_company_id'])
    brn_coordx = brn[brn['brno_company_id'].isin(no_coord_id)]

    has_addr = brn_coordx[brn_coordx['addr'].notna()]
    no_addr = brn_coordx[brn_coordx['addr'].isna()]
    has_addr.to_csv('data/brno/3_brn_coordx_addr.csv', encoding='utf-8-sig', index=False)
    no_addr.to_csv('data/brno/4_brn_coordx_addrx.csv', encoding='utf-8-sig', index=False)


def run_juso_and_bcode_to_rows_with_addr():
    # run bcode on '3_brn_coordx_addr'
    brno_dir = os.path.join('data', 'brno')
    bcode_fps = [os.path.join(brno_dir, f) for f in os.listdir(brno_dir) if 'bcode_brn_coordx_addr' in f]
    has_addr_bcode = [pd.read_csv(x, encoding='utf-8-sig') for x in bcode_fps]
    has_addr_bcode_df = pd.concat(has_addr_bcode)
    has_addr_bcode_df.drop(columns=['Unnamed: 0'], inplace=True)
    has_addr_bcode_df.rename(columns={'depth1': 'ori_depth1', 'depth2': 'ori_depth2', 'depth3': 'ori_depth3',
                                      'region_code': 'ori_region_code'}, inplace=True)
    has_addr_bcode_df.to_csv('data/brno/bcode_brno_coordx_addr_.csv', encoding='utf-8-sig', index=False)
    # run juso on this. save it as...
    # 3_juso_bcode_brno_coordx_addr_done.csv
    c = pd.read_csv('data/brno/3_juso_bcode_brno_coordx_addr_done.csv', encoding='utf-8-sig')
    c.drop(columns=['Unnamed: 0', 'created_at', 'updated_at', 'category', 'category.1', 'src', 'src.1'], inplace=True)
    c['zone_no'].fillna(c['zipNo'], inplace=True)
    c.drop(columns=['x.1', 'y.1', 'region_code.1', 'used_addr.1', 'depth1.1', 'depth2.1', 'depth3.1', 'road.1', 'parcel.1', 'title.1', 'title'], inplace=True)
    c.drop(columns=['zipNo'], inplace=True)
    c = c[c['x'].notna()]
    c.to_csv('data/brno/3_juso_bcode_brno_coordx_addr_done_2.csv', encoding='utf-8-sig', index=False)


def add_info_from_lookup_using_name_and_brno():
    no_addr = pd.read_csv('data/brno/4_brn_coordx_addrx.csv', encoding='utf-8-sig')
    lookup_done = pd.read_csv('data/lookup/lookup_juso_filled.csv', encoding='utf-8-sig')
    # drop the ones that does not have b_code
    merged_df = pd.merge(no_addr, lookup_done,  left_on=['brno', 'company_name'], right_on=['brno', 'frcs_nm'],
                         how='inner')
    merged_df.drop(columns=['Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32', 'Unnamed: 33', 'Unnamed: 34'], inplace=True)
    merged_df.drop(columns=['src'], inplace=True)
    merged_df.drop(columns=['addr_x', 'addr_y'], inplace=True)
    merged_df.drop(columns=['updated_at', 'created_at'], inplace=True)
    merged_df['region_code'].fillna(merged_df['ori_region_code'], inplace=True)
    merged_df.drop(columns=['ori_region_code', 'ori_depth1', 'ori_depth2', 'ori_depth3'], inplace=True)
    merged_df.rename(columns={'used_addr': 'addr'}, inplace=True)
    merged_df.rename(columns={'ori_road': 'road', 'ori_parcel': 'parcel'}, inplace=True)
    merged_df.to_csv('data/brno/4_brno_coordx_addrx_merged.csv', encoding='utf-8-sig', index=False)


def merge_all_brno_comp():
    # brno_companies.txt -> juso result : 1_brno_juso.csv
    # brno_juso1_.csv -> split to two based on coord : 2_brno_juso_coordo,  brno_juso_coordx

    b = pd.read_csv('data/brno/2_brno_juso_coordo_done_3.csv', encoding='utf-8-sig')
    c = pd.read_csv('data/brno/3_juso_bcode_brno_coordx_addr_done_2.csv', encoding='utf-8-sig')
    m = pd.concat([b, c])
    m['used_addr'].fillna(m['refined'], inplace=True)
    m['used_addr'].fillna(m['road'], inplace=True)
    m['used_addr'].fillna(m['parcel'], inplace=True)
    m.drop(columns=['refined', 'company_name', 'addr', 'upjong_code', 'upjong_name'], inplace=True)
    m.drop(columns=['fran', 'fran_id', 'cate_name', 'cate_code', 'src_from'], inplace=True)
    m.drop(columns=['is_omitted', 'is_deleted', 'road', 'parcel'], inplace=True)
    m.rename(columns={'used_addr': 'refined'}, inplace=True)

    brno_t = pd.read_csv('data/brno_companies_exist.csv', encoding='utf-8-sig')         # 2m rows
    brno_t.rename(columns={'id': 'brno_company_id'}, inplace=True)
    brno_t_addr = m[m['brno_company_id'].isin(brno_t['brno_company_id'])]
    brno_t_addr['src'] = 'ori_brno_juso_api'
    brno_t_addr.to_csv('data/brno_companies_exist_addr.csv', encoding='utf-8-sig', index=False)             # 1m rows

    brno_t = pd.read_csv('data/brno_companies_updated.csv', encoding='utf-8-sig')       # 1.3m rows
    brno_t.rename(columns={'id': 'brno_company_id'}, inplace=True)
    lu = pd.read_csv('data/lookup/lookup_juso_filled_done.csv', encoding='utf-8-sig')
    lu['x'].fillna(lu['lot'], inplace=True)
    lu['y'].fillna(lu['lat'], inplace=True)
    lu['used_addr'].fillna(lu['refined'], inplace=True)
    lu['used_addr'].fillna(lu['frcs_addr'], inplace=True)
    lu['used_addr'].fillna(lu['road'], inplace=True)
    lu['used_addr'].fillna(lu['parcel'], inplace=True)
    lu['zone_no'].fillna(lu['frcs_zip'], inplace=True)
    lu.rename(columns={'used_addr': 'refined'}, inplace=True)
    luj = lu[['brno', 'refined', 'region_code', 'depth1', 'depth2', 'depth3', 'x', 'y', 'zone_no']]
    luj.to_csv('data/lookup/lookup_addr_col_done.csv', encoding='utf-8-sig', index=False)

    luj['src'] = 'jope_lookup'
    luj = luj.merge(brno_t[['brno', 'brno_company_id']], on='brno', how='inner')
    luj.to_csv('data/brno_companies_updated_addr.csv', encoding='utf-8-sig', index=False)             # 0.9m rows

    brno_t = pd.read_csv('data/brno_companies_new.csv', encoding='utf-8-sig')  # 1.3m rows
    brno_t.rename(columns={'id': 'brno_company_id'}, inplace=True)
    luj = pd.read_csv('data/lookup/lookup_addr_col_done.csv', encoding='utf-8-sig')
    luj['src'] = 'jope_lookup'
    luj = luj.merge(brno_t[['brno', 'brno_company_id']], on='brno', how='inner')
    # dup = luj[luj['brno_company_id'].duplicated(keep=False)]          length is 0. good.
    luj.to_csv('data/brno_companies_new_addr.csv', encoding='utf-8-sig', index=False)             # 0.4m rows


def merge_addr():
    bta = pd.read_csv('data/brno_companies_exist_addr.csv', encoding='utf-8-sig')
    btau = pd.read_csv('data/brno_companies_updated_addr.csv', encoding='utf-8-sig')
    btan = pd.read_csv('data/brno_companies_new_addr.csv', encoding='utf-8-sig')
    bt = pd.concat([bta, btau, btan])
    bt = bt.dropna(subset=['depth1', 'region_code'])
    bt.to_csv('data/brno_companies_addr.csv', encoding='utf-8-sig', index=False)
