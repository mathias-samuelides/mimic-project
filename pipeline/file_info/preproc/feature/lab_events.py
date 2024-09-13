from enum import StrEnum
from pipeline.file_info.preproc.feature.path_prefix import FEATURE_EXTRACT_PATH

FEATURE_LAB_EVENTS_PATH = FEATURE_EXTRACT_PATH / "lab_events.csv.gz"


class LabEventsFeatureHeader(StrEnum):
    PATIENT_ID = "subject_id"
    HOSPITAL_ADMISSION_ID = "hadm_id"
    ITEM_ID = "itemid"
    CHART_TIME = "charttime"
    ADMIT_TIME = "admittime"
    LAB_TIME_FROM_ADMIT = "lab_time_from_admit"
    VALUE_NUM = "valuenum"
