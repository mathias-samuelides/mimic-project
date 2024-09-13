import pandas as pd
import numpy as np

from pipeline.file_info.code_map import IcdMapHeader
from pipeline.extract.static.code_map import load_static_icd_map
from pipeline.file_info.raw.hosp import DiagnosesIcd


ROOT_ICD_CONVERT = "root_icd10_convert"


class IcdConverter:
    def __init__(self):
        # Load the ICD-9 to ICD-10 conversion dictionary upon initialization
        self.conversions_icd_9_10 = self._load_icd_9_to_10_mapping()

    def _load_icd_9_to_10_mapping(self) -> dict:
        """Loads and returns a dictionary for converting ICD-9 to ICD-10 codes."""
        icd_map_df = load_static_icd_map()

        # Filter for ICD-9 root codes (3 characters)
        icd_9_root_codes = icd_map_df[
            icd_map_df[IcdMapHeader.DIAGNOSIS_CODE].str.len() == 3
        ]

        # Remove duplicates and create the mapping dictionary
        icd_9_root_codes = icd_9_root_codes.drop_duplicates(
            subset=IcdMapHeader.DIAGNOSIS_CODE
        )
        return dict(
            zip(
                icd_9_root_codes[IcdMapHeader.DIAGNOSIS_CODE],
                icd_9_root_codes[IcdMapHeader.ICD10],
            )
        )

    def convert_icd(self, code: str, version: int) -> str:
        """Converts ICD-9 to ICD-10 if applicable, otherwise returns the original code."""
        if version == 9:
            return self.conversions_icd_9_10.get(
                code[:3], np.nan
            )  # Use root of ICD-9 code
        return code  # Return ICD-10 code as is

    def standardize_icd(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes ICD codes in a DataFrame by converting ICD-9 to ICD-10."""
        df["CONVERTED_ICD_CODE"] = df.apply(
            lambda row: self.convert_icd(
                row[DiagnosesIcd.ICD_CODE], row[DiagnosesIcd.ICD_VERSION]
            ),
            axis=1,
        )

        # Extract root of standardized ICD-10 codes (first 3 characters)
        df[DiagnosesIcd.ROOT] = df["CONVERTED_ICD_CODE"].apply(
            lambda x: x[:3] if isinstance(x, str) else np.nan
        )
        return df

    def get_pos_ids(self, diag: pd.DataFrame, icd10_code: str) -> pd.Series:
        """Extracts unique hospital admission IDs where the ICD-10 root matches a given code."""
        matching_rows = diag[diag[DiagnosesIcd.ROOT].str.contains(icd10_code, na=False)]
        return matching_rows[DiagnosesIcd.HOSPITAL_ADMISSION_ID].unique()
