import pandas as pd
from pipeline.file_info.raw.hosp import (
    HOSP_PATIENTS_PATH,
    HOSP_ADMISSIONS_PATH,
    HOSP_DIAGNOSES_ICD_PATH,
    HOSP_LAB_EVENTS_PATH,
    HOSP_PROCEDURES_ICD_PATH,
    HOSP_PREDICTIONS_PATH,
    PatientsHeader,
    AdmissionsHeader,
    LabEventsHeader,
    ProceduresIcdHeader,
    PrescriptionsHeader,
)


def load_patients() -> pd.DataFrame:
    return pd.read_csv(
        HOSP_PATIENTS_PATH,
        compression="gzip",
        parse_dates=[PatientsHeader.DOD],
    )


def load_admissions() -> pd.DataFrame:
    return pd.read_csv(
        HOSP_ADMISSIONS_PATH,
        compression="gzip",
        parse_dates=[
            AdmissionsHeader.ADMITTIME.value,
            AdmissionsHeader.DISCHTIME.value,
        ],
    )


def load_diagnosis_icd() -> pd.DataFrame:
    return pd.read_csv(HOSP_DIAGNOSES_ICD_PATH, compression="gzip")


def load_lab_events(chunksize: int, use_cols=None) -> pd.DataFrame:
    return pd.read_csv(
        HOSP_LAB_EVENTS_PATH,
        compression="gzip",
        parse_dates=[LabEventsHeader.CHART_TIME],
        chunksize=chunksize,
        usecols=use_cols,
    )


def load_procedures_icd() -> pd.DataFrame:
    return pd.read_csv(
        HOSP_PROCEDURES_ICD_PATH,
        compression="gzip",
        parse_dates=[ProceduresIcdHeader.CHART_DATE.value],
    ).drop_duplicates()


def load_prescriptions() -> pd.DataFrame:
    return pd.read_csv(
        HOSP_PREDICTIONS_PATH,
        compression="gzip",
        usecols=[
            PrescriptionsHeader.PATIENT_ID,
            PrescriptionsHeader.HOSPITAL_ADMISSION_ID,
            PrescriptionsHeader.DRUG,
            PrescriptionsHeader.START_TIME,
            PrescriptionsHeader.STOP_TIME,
            PrescriptionsHeader.NDC,
            PrescriptionsHeader.DOSE_VAL_RX,
        ],
        parse_dates=[PrescriptionsHeader.START_TIME, PrescriptionsHeader.STOP_TIME],
    )
