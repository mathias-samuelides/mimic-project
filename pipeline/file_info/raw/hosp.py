from enum import StrEnum
import pandas as pd
from pipeline.file_info.path_prefix import RAW_PATH

""" 
The Hosp module provides all data acquired from the hospital wide electronic health record
"""


HOSP = "hosp"
HOSP_DIAGNOSES_ICD_PATH = RAW_PATH / HOSP / "diagnoses_icd.csv.gz"
HOSP_PATIENTS_PATH = RAW_PATH / HOSP / "patients.csv.gz"
HOSP_LAB_EVENTS_PATH = RAW_PATH / HOSP / "labevents.csv.gz"
HOSP_ADMISSIONS_PATH = RAW_PATH / HOSP / "admissions.csv.gz"
HOSP_PREDICTIONS_PATH = RAW_PATH / HOSP / "prescriptions.csv.gz"
HOSP_PROCEDURES_ICD_PATH = RAW_PATH / HOSP / "procedures_icd.csv.gz"


# information regarding a patient
class Patients(StrEnum):
    ID = "subject_id"  # patient id
    ANCHOR_YEAR = "anchor_year"  # shifted year for the patient
    ANCHOR_AGE = "anchor_age"  # patient’s age in the anchor_year
    ANCHOR_YEAR_GROUP = "anchor_year_group"  # anchor_year occurred during this range
    DOD = "dod"  # de-identified date of death for the patient
    GENDER = "gender"


# information regarding a patient’s admission to the hospital
class Admissions(StrEnum):
    ID = "hadm_id"  # hospitalization id
    PATIENT_ID = "subject_id"  # patient id
    ADMITTIME = "admittime"  # datetime the patient was admitted to the hospital
    DISCHTIME = "dischtime"  # datetime the patient was discharged from the hospital
    HOSPITAL_EXPIRE_FLAG = "hospital_expire_flag"  # whether the patient died within the given hospitalization
    LOS = "los"
    HOSPITAL_ADMISSION_ID = "hadm_id"
    INSURANCE = "insurance"
    RACE = "race"


class DiagnosesIcd(StrEnum):
    SUBJECT_ID = "subject_id"  # patient id
    HOSPITAL_ADMISSION_ID = "hadm_id"  # patient hospitalization id
    SEQ_NUM = "seq_num"  #  priority assigned to the diagnoses
    ICD_CODE = "icd_code"  #  International Coding Definitions code
    ICD_VERSION = "icd_version"  # version for the coding system
    # added
    ICD10 = "root_icd10_convert"
    ROOT = "root"


class LabEvents(StrEnum):
    PATIENT_ID = "subject_id"
    HOSPITAL_ADMISSION_ID = "hadm_id"
    CHART_TIME = "charttime"
    ITEM_ID = "itemid"
    ADMIT_TIME = "admittime"
    LAB_TIME_FROM_ADMIT = "lab_time_from_admit"
    VALUE_NUM = "valuenum"
    VALUE_UOM = "valueuom"


class ProceduresIcd(StrEnum):
    PATIENT_ID = "subject_id"
    HOSPITAL_ADMISSION_ID = "hadm_id"
    SEQ_NUM = "seq_num"
    CHART_DATE = "chartdate"
    ICD_CODE = "icd_code"
    ICD_VERSION = "icd_version"


class Prescriptions(StrEnum):
    PATIENT_ID = "subject_id"
    HOSPITAL_ADMISSION_ID = "hadm_id"
    DRUG = "drug"
    START_TIME = "starttime"
    STOP_TIME = "stoptime"
    NDC = "ndc"
    DOSE_VAL_RX = "dose_val_rx"
