from enum import StrEnum
import pandas as pd
from pipeline.file_info.path_prefix import RAW_PATH

"""
The ICU module contains information collected from the clinical information system used within the ICU.

"""
ICU = "icu"

ICUSTAY_PATH = RAW_PATH / ICU / "icustays.csv.gz"
INPUT_EVENT_PATH = RAW_PATH / ICU / "inputevents.csv.gz"
OUTPUT_EVENT_PATH = RAW_PATH / ICU / "outputevents.csv.gz"
CHART_EVENTS_PATH = RAW_PATH / ICU / "chartevents.csv.gz"
PROCEDURE_EVENTS_PATH = RAW_PATH / ICU / "procedureevents.csv.gz"


# information regarding ICU stays
class IcuStays(StrEnum):
    PATIENT_ID = "subject_id"  # patient id
    ID = "stay_id"  # icu stay id
    HOSPITAL_ADMISSION_ID = "hadm_id"  # patient hospitalization id
    INTIME = "intime"  #  datetime the patient was transferred into the ICU.
    OUTTIME = "outtime"  #  datetime the patient was transferred out the ICU.
    LOS = "los"  # length of stay for the patient for the given ICU stay in fractional days.
    ADMITTIME = "admittime"


# Information regarding patient outputs including urine, drainage...
class OuputputEvents(StrEnum):
    SUBJECT_ID = "subject_id"  # patient id
    HOSPITAL_ADMISSION_ID = "hadm_id"  # patient hospitalization id
    STAY_ID = "stay_id"  # patient icu stay id
    ITEM_ID = "itemid"  # single measurement type id
    CHART_TIME = "charttime"  # time of an output event


class ChartEvents(StrEnum):
    STAY_ID = "stay_id"
    CHARTTIME = "charttime"
    ITEMID = "itemid"
    VALUENUM = "valuenum"
    VALUEOM = "valueuom"


class InputEvents(StrEnum):
    SUBJECT_ID = "subject_id"
    STAY_ID = "stay_id"
    ITEMID = "itemid"
    STARTTIME = "starttime"
    ENDTIME = "endtime"
    RATE = "rate"
    AMOUNT = "amount"
    ORDERID = "orderid"


class ProceduresEvents(StrEnum):
    STAY_ID = "stay_id"
    START_TIME = "starttime"
    ITEM_ID = "itemid"
