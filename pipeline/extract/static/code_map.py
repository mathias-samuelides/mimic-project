import pandas as pd
from pipeline.file_info.code_map import MAP_PATH, MAP_NDC_PATH


def load_static_icd_map() -> pd.DataFrame:
    return pd.read_csv(MAP_PATH, delimiter="\t")


def load_ndc_mapping() -> pd.DataFrame:
    return pd.read_csv(MAP_NDC_PATH, delimiter="\t")
