from enum import StrEnum
from pipeline.file_info.preproc.feature.path_prefix import FEATURE_EXTRACT_PATH

FEATURE_PROCEDURES_WITH_ICU_PATH = FEATURE_EXTRACT_PATH / "procedures_with_icu.csv.gz"
FEATURE_PROCEDURES_WITHOUT_ICU_PATH = (
    FEATURE_EXTRACT_PATH / "procedures_without_icu.csv.gz"
)


class ProceduresFeatureHeader(StrEnum):
    PATIENT_ID = "subject_id"
    HOSPITAL_ADMISSION_ID = "hadm_id"


class ProceduresFeatureWithIcuHeader(StrEnum):
    STAY_ID = "stay_id"
    ITEM_ID = "itemid"
    START_TIME = "starttime"
    IN_TIME = "intime"
    EVENT_TIME_FROM_ADMIT = "event_time_from_admit"


class ProceduresFeatureWithoutIcuHeader(StrEnum):
    ICD_CODE = "icd_code"
    ICD_VERSION = "icd_version"
    CHART_DATE = "chartdate"
    ADMIT_TIME = "admittime"
    PROC_TIME_FROM_ADMIT = "proc_time_from_admit"
