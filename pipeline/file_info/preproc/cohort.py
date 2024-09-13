from enum import StrEnum
from pipeline.file_info.path_prefix import PREPROC_PATH

COHORT_PATH = PREPROC_PATH / "cohort"


class CohortHeader(StrEnum):
    PATIENT_ID = "subject_id"  # Unique identifier for each patient
    HOSPITAL_ADMISSION_ID = "hadm_id"  # Identifier for each hospital admission
    FIRST_CARE_UNIT = "first_careunit"  # The first ICU unit the patient was admitted to
    LAST_CARE_UNIT = "last_careunit"  # The last ICU unit the patient stayed in
    LOS = "los"  # Length of stay in the ICU (measured in days)
    AGE = "age"  # The patient's age at the time of admission
    MIN_VALID_YEAR = "min_valid_year"  # Minimum valid year for the patient data
    DOD = "dod"  # Date of death for the patient (NaT if the patient is alive)
    GENDER = "gender"  # Gender of the patient
    INSURANCE = "insurance"  # Type of insurance covering the patient
    ETHICITY = "ethnicity"  # Ethnicity of the patient
    LABEL = "label"  # Label used for prediction or classification purposes


# This class defines the headers specific to ICU-related data, used when ICU data is included in the cohort.
class CohortWithIcuHeader(StrEnum):
    STAY_ID = "stay_id"  # Unique identifier for each ICU stay
    IN_TIME = "intime"  # Time when the patient was admitted to the ICU
    OUT_TIME = "outtime"  # Time when the patient was discharged from the ICU


# This class defines the headers for non-ICU cohort data, used when the cohort excludes ICU-specific data.
class CohortWithoutIcuHeader(StrEnum):
    ADMIT_TIME = (
        "admittime"  # Time when the patient was admitted to the hospital (non-ICU)
    )
    DISCH_TIME = (
        "dischtime"  # Time when the patient was discharged from the hospital (non-ICU)
    )
