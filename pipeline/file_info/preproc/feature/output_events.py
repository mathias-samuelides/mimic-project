from enum import StrEnum
from pipeline.file_info.preproc.feature.path_prefix import FEATURE_EXTRACT_PATH

FEATURE_OUTPUT_EVENTS_PATH = FEATURE_EXTRACT_PATH / "output_events.csv.gz"


class OutputEventsFeatureHeader(StrEnum):
    PATIENT_ID = "subject_id"
    HOSPITAL_ADMISSION_ID = "hadm_id"
    STAY_ID = "stay_id"
    ITEM_ID = "itemid"
    CHART_TIME = "charttime"
    IN_TIME = "intime"
    EVENT_TIME_FROM_ADMIT = "event_time_from_admit"
