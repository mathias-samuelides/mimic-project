from enum import StrEnum
from pipeline.file_info.preproc.feature.path_prefix import FEATURE_EXTRACT_PATH

FEATURE_MEDICATIONS_WITH_ICU_PATH = FEATURE_EXTRACT_PATH / "med_icu.csv.gz"
FEATURE_MEDICATIONS_WITHOUT_ICU_PATH = FEATURE_EXTRACT_PATH / "med.csv.gz"


class MedicationsFeatureHeader(StrEnum):
    PATIENT_ID = "subject_id"
    HOSPITAL_ADMISSION_ID = "hadm_id"
    START_TIME = "starttime"
    START_HOURS_FROM_ADMIT = "start_hours_from_admit"
    STOP_HOURS_FROM_ADMIT = "stop_hours_from_admit"


class MedicationsFeatureWithIcuHeader(StrEnum):
    STAY_ID = "stay_id"
    ITEM_ID = "itemid"
    END_TIME = "endtime"
    RATE = "rate"
    AMOUNT = "amount"
    ORDER_ID = "orderid"


class MedicationsFeatureWithoutIcuHeader(StrEnum):
    STOP_TIME = "stoptime"
    DRUG = "drug"
    NON_PROPRIEATARY_NAME = "nonproprietaryname"
    DOSE_VAL_RX = "dose_val_rx"
    EPC = "EPC"
