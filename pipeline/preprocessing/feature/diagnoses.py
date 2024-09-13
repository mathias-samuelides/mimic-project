from enum import StrEnum
from pipeline.conversion.icd import IcdConverter
from pipeline.preprocessing.feature.feature_abc import Feature, FeatureGroup
import logging
import pandas as pd
from pipeline.file_info.preproc.feature.diagnoses import (
    DiagnosesFeatureHeader,
    DiagnosesFeatureWithIcuHeader,
)
from pipeline.file_info.preproc.cohort import CohortHeader, CohortWithIcuHeader
from pipeline.extract.raw.hosp import load_diagnosis_icd

logger = logging.getLogger()


class IcdGroupOption(StrEnum):
    KEEP = "Keep both ICD-9 and ICD-10 codes"
    CONVERT = "Convert ICD-9 to ICD-10 codes"
    GROUP = "Convert ICD-9 to ICD-10 and group ICD-10 codes"


MEAN_FREQUENCY_HEADER = "mean_frequency"


class Diagnoses(Feature):
    def __init__(self, use_icu: bool, df: pd.DataFrame = pd.DataFrame()):
        self.use_icu = use_icu
        self.df = df

    def group() -> str:
        return FeatureGroup.DIAGNOSES

    def extract_from(self, cohort: pd.DataFrame) -> pd.DataFrame:
        logger.info("[EXTRACTING DIAGNOSIS DATA]")
        hosp_diagnose = load_diagnosis_icd()
        admissions_cohort_cols = (
            [
                CohortHeader.HOSPITAL_ADMISSION_ID,
                CohortWithIcuHeader.STAY_ID,
                CohortHeader.LABEL,
            ]
            if self.use_icu
            else [CohortHeader.HOSPITAL_ADMISSION_ID, CohortHeader.LABEL]
        )
        diag = hosp_diagnose.merge(
            cohort[admissions_cohort_cols],
            on=DiagnosesFeatureHeader.HOSPITAL_ADMISSION_ID,
        )
        icd_converter = IcdConverter()
        diag = icd_converter.standardize_icd(diag)
        diag = diag[
            [h.value for h in DiagnosesFeatureHeader]
            + ([DiagnosesFeatureWithIcuHeader.STAY_ID] if self.use_icu else [])
        ]
        self.df = diag
        return diag
