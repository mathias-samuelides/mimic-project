from enum import StrEnum
from pipeline.file_info.preproc.feature.path_prefix import FEATURE_EXTRACT_PATH

FEATURE_DIAG_WITH_ICU_PATH = FEATURE_EXTRACT_PATH / "diagnoses_with_icu.csv.gz"
FEATURE_DIAG_WITHOUT_ICU_PATH = FEATURE_EXTRACT_PATH / "diagnoses_without_icu.csv.gz"


class DiagnosesFeatureHeader(StrEnum):
    PATIENT_ID = "subject_id"
    HOSPITAL_ADMISSION_ID = "hadm_id"
    ICD_CODE = "icd_code"
    CONVERTED_ICD_CODE = "root_icd10_convert"
    ROOT = "root"


class DiagnosesFeatureWithIcuHeader(StrEnum):
    STAY_ID = "stay_id"
