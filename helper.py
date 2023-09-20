import pandas as pd
import numpy as np
import os


def split_and_export(df: pd.DataFrame, file_name, file_counts=4):
    sdf = np.array_split(df, file_counts, axis=0)
    for idx, split_df in enumerate(sdf):
        split_df.to_csv(f'{file_name}_{idx}.csv', encoding='utf-8-sig')
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

