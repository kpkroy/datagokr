import pandas as pd
import numpy as np
import os
import argparse
import datetime
import re
import json


def split_and_export(work_d, f_name, file_counts=4):

    ifp = os.path.join(work_d, f_name)
    idf = pd.read_csv(ifp, encoding='utf-8-sig')
    sdf = np.array_split(idf, file_counts, axis=0)

    f_name_body = f_name.split('.')[0]
    for idx, split_df in enumerate(sdf):
        f_name = f'{f_name_body}_{idx}.csv'
        split_df.to_csv(os.path.join(work_d, f_name), encoding='utf-8-sig')
    return


def read_file_and_drop_na(ifp, drop_subset: list, delim='') -> pd.DataFrame:
    if delim:
        main_df = pd.read_csv(ifp, encoding='utf-8-sig', delimiter=delim)
    else:
        main_df = pd.read_csv(ifp, encoding='utf-8-sig')
    main_df.dropna(subset=drop_subset, inplace=True)        # drop rows where drop_subset column is null
    return main_df


def get_undone(idf: pd.DataFrame, fps: list, key_col: str):
    rdfs = [pd.read_csv(x, encoding='utf-8-sig') for x in fps]
    rdf = pd.concat(rdfs)
    undone = idf[~idf[key_col].isin(rdf[key_col])]
    return undone


def get_file_path_list_with_suffix(dir_path: str, suffix: str):
    file_names = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith(suffix)]
    return file_names


def consolidate_and_export(rfp: list, out_fp):
    dfs = [pd.read_csv(x, encoding='utf-8-sig') for x in rfp]
    df = pd.concat(dfs)
    df['refined'].fillna(df['road'], inplace=True)
    df['refined'].fillna(df['parcel'], inplace=True)
    df = df.sort_values(by=['refined', 'region_code'], na_position='last').drop_duplicates(subset='id', keep='first')
    df.to_csv(out_fp, encoding='utf-8-sig')


def get_now_time():
    return datetime.datetime.now().strftime('%m%d_%H%M')


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--d", help="working directory.", type=str, default='data')
    parser.add_argument("--i", help="[INPUT] file name", type=str)
    parser.add_argument("--prev", help="[INPUT] prev files, delimited by '|'", type=str)
    parser.add_argument("--key_col", help="id column to check for prev", type=str)
    return parser.parse_args()


def get_update_without_id(fp_prev, fp_now, use_cols, d_types):
    df_prev = pd.read_csv(fp_prev, encoding='utf-8-sig', dtype=d_types, usecols=use_cols)
    df_now = pd.read_csv(fp_now, encoding='utf-8-sig', dtype=d_types, usecols=use_cols)
    df_now.drop_duplicates(inplace=True)
    df_prev.drop_duplicates(inplace=True)

    # Find rows in B that are not in A
    return df_now.merge(df_prev, how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)


def get_update_with_id(fp_prev, fp_now, id_col_name):
    pass


def rm_duplicates_from_file(fp, sort_by, drop_duplicates_by):
    df = pd.read_csv(fp, encoding='utf-8-sig')
    df = df.sort_values(by=sort_by, na_position='last').drop_duplicates(subset=drop_duplicates_by, keep='first')
    df.to_csv(fp, encoding='utf-8-sig', index=False)


def add_branch_name_col():
    d_types = {'id': int, 'brno': float, 'company_name': 'string', 'addr': 'string', 'fran': 'string',
               'cate_name': 'string', 'src_from': 'string'}
    df = pd.read_csv(os.path.join('data', 'brno_companies.txt'), encoding='utf-8-sig', delimiter='|',
                     usecols=list(d_types.keys()), dtype=d_types)
    dff = df[(df['fran'].notna()) & (df['company_name'].notna())]
    dff['fran_branch_name'] = dff.apply(lambda row: get_branch_name_only(row['company_name'], row['fran']), axis=1)
    dff = dff[['id', 'brno', 'company_name', 'fran', 'fran_branch_name']]
    dff.to_csv('data//brno_branch_info.csv', encoding='utf-8-sig', index=False)


def get_branch_name_only(company_name: str, fran: str):
    if not fran:
        return None
    joms = ['편의점', '할인점', '백화점', '전문점', '면세점', '과자점', '주점', '제과점']
    if company_name.endswith('점'):
        for j in joms:
            if company_name.endswith(j):
                return None
        if isinstance(fran, str) and fran and fran in company_name:
            pattern = r'[^\w\s]|{}'.format(fran)  # Using str.format()
            result = re.split(pattern, company_name)[-1]
            if result:
                result = re.sub(r'[^\w\s]', '', result)
                if result != company_name:
                    print(f'company_name: {company_name} result {result}')
                    return result
    return None


def merge_card_fran_json():
    fps = get_file_path_list_with_suffix('data', '.json')
    rdfs = []
    for fp in fps:
        with open(fp, 'r', encoding='utf-8-sig') as j:
            data = json.load(j)
        rdfs.append(pd.DataFrame(data.get('data')))
    rdf = pd.concat(rdfs)
    rdf.to_csv('data/all_card_fran.csv', encoding='utf-8-sig', index=False)


def get_empty_addresses_from():

    brn = pd.read_csv('data/brno_companies.txt', encoding='utf-8-sig', delimiter='|')
    brn_j = pd.read_csv('data/brno_juso_result.csv', encoding='utf-8-sig')
    j_use = ['id', 'brno', 'region_code', 'depth1', 'depth2', 'depth3', 'x', 'y', 'road', 'parcel', 'refined', 'type',
             'src']
    brn_j = brn_j[j_use]
    all_card = pd.read_csv('data/all_card_fran.csv', encoding='utf-8-sig')

    # brn_j should have rows that has value in column 'x'
    # from brn, select rows that does not exist in brn_j. use column 'id' for this.
    # create a new dataframe by inner joining the selected rows with all_card, using column 'brno'
    # Filter brn_j to only have rows with non-null values in the 'x' column
    brn_j = brn_j[brn_j['x'].notna()]

    # Get IDs that are in brn but not in brn_j
    ids_not_in_brn_j = set(brn['id']) - set(brn_j['id'])

    # Filter rows from brn based on these IDs
    selected_brn_rows = brn[brn['id'].isin(ids_not_in_brn_j)]

    # Inner join selected rows with all_card on 'brno'
    result_df = selected_brn_rows.merge(all_card, on='brno', how='inner')
    use_cols = ['id', 'brno', 'company_name', 'frcs_nm', 'frcs_addr', 'lat', 'lot']
    result_df = result_df[use_cols]
    recol = {'frcs_addr': 'addr'}
    result_df.rename(recol, inplace=True)
    result_df = result_df.sort_values(by=['frcs_addr', 'lat'], na_position='last').drop_duplicates(subset='id', keep='first')
    result_df_notna = result_df[result_df['lat'].notna()]
    result_df_notna.to_csv('data/brno_new_hasxy.csv', encoding='utf-8-sig', index=False)    # only region code missing
    result_df_na = result_df[result_df['lat'].isna()]
    result_df_na.to_csv('data/brno_new_noxy.csv', encoding='utf-8-sig', index=False)  # must get xy as well
    result_df.to_csv('data/brno_new.csv', encoding='utf-8-sig', index=False)


if __name__ == '__main__':
    rfp = get_file_path_list_with_suffix('data//b_code', 'b_code_added.csv')
    dfs = [pd.read_csv(x, encoding='utf-8-sig') for x in rfp]
    df = pd.concat(dfs)
    df = df.sort_values(by=['geo_lat', 'region_code'], na_position='last').drop_duplicates(subset='id', keep='first')
    df.to_csv('data//comp_b_code', encoding='utf-8-sig', index=False)


