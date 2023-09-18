import pandas as pd
import numpy as np


def split_and_export(df: pd.DataFrame, file_name, file_counts=4):
    sdf = np.array_split(df, file_counts, axis=0)
    for idx, split_df in enumerate(sdf):
        split_df.to_csv(f'{file_name}_{idx}.csv', encoding='utf-8-sig')
    return


def read_file_and_drop_na(ifp, drop_subset: list) -> pd.DataFrame:
    main_df = pd.read_csv(ifp, encoding='utf-8-sig', delimiter='|')
    main_df.dropna(subset=drop_subset, inplace=True)        # drop rows where drop_subset column is null
    return main_df


def get_undone(idf: pd.DataFrame, fps: list, key_col: str):
    rdfs = [pd.read_csv(x, encoding='utf-8-sig') for x in fps]
    rdf = pd.concat(rdfs)
    undone = idf[~idf[key_col].isin(rdf[key_col])]
    return undone




