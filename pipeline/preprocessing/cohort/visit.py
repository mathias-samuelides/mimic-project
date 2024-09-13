from typing import Optional
import pandas as pd
from pipeline.file_info.raw.hosp import Patients, Admissions
from pipeline.file_info.raw.icu import IcuStays
from pipeline.file_info.preproc.cohort import (
    CohortHeader,
    CohortWithIcuHeader,
    CohortWithoutIcuHeader,
)
from pipeline.prediction_task import TargetType, DiseaseCode
from pipeline.conversion.icd import IcdConverter

from pipeline.file_info.raw.hosp import (
    DiagnosesIcd,
)
from pipeline.extract.raw.hosp import load_diagnosis_icd


def make_patients(hosp_patients: pd.DataFrame) -> pd.DataFrame:
    patients = hosp_patients[
        [
            Patients.ID,
            Patients.ANCHOR_YEAR,
            Patients.ANCHOR_YEAR_GROUP,
            Patients.ANCHOR_AGE,
            Patients.DOD,
            Patients.GENDER,
        ]
    ].copy()
    max_anchor_year_group = (
        patients[Patients.ANCHOR_YEAR_GROUP].str.slice(start=-4).astype(int)
    )
    # To identify visits with prediction windows outside the range 2008-2019.
    patients[CohortHeader.MIN_VALID_YEAR] = (
        hosp_patients[Patients.ANCHOR_YEAR] + 2008 - max_anchor_year_group
    )
    patients = patients.rename(columns={Patients.ANCHOR_AGE: CohortHeader.AGE})[
        [
            Patients.ID,
            CohortHeader.AGE,
            CohortHeader.MIN_VALID_YEAR,
            Patients.DOD,
            Patients.GENDER,
        ]
    ]
    return patients


def make_visits_with_icu(
    icustays: pd.DataFrame, patients: pd.DataFrame, target_type: TargetType
) -> pd.DataFrame:
    if target_type != TargetType.READMISSION:
        return icustays
    # Filter out stays where either there is no death or the death occurred after ICU discharge
    patients_dod = patients[[Patients.ID, Patients.DOD]]
    visits = icustays.merge(patients_dod, on=IcuStays.PATIENT_ID)
    filtered_visits = visits.loc[
        (visits[Patients.DOD].isna())
        | (visits[Patients.DOD] >= visits[IcuStays.OUTTIME])
    ]
    return filtered_visits[
        [
            CohortHeader.PATIENT_ID,
            CohortWithIcuHeader.STAY_ID,
            CohortHeader.HOSPITAL_ADMISSION_ID,
            CohortWithIcuHeader.IN_TIME,
            CohortWithIcuHeader.OUT_TIME,
            CohortHeader.LOS,
        ]
    ]


def make_visits_witout_icu(
    admissions: pd.DataFrame, target_type: TargetType
) -> pd.DataFrame:
    admissions[Admissions.LOS] = (
        admissions[Admissions.DISCHTIME] - admissions[Admissions.ADMITTIME]
    ).dt.days

    if target_type == TargetType.READMISSION:
        # Filter out hospitalizations where the patient expired
        admissions = admissions[admissions[Admissions.HOSPITAL_EXPIRE_FLAG] == 0]
    return admissions[
        [
            CohortHeader.PATIENT_ID,
            CohortHeader.HOSPITAL_ADMISSION_ID,
            CohortWithoutIcuHeader.ADMIT_TIME,
            CohortWithoutIcuHeader.DISCH_TIME,
            CohortHeader.LOS,
        ]
    ]


def filter_by_disease(
    visits: pd.DataFrame,
    diag: pd.DataFrame,
    disease: DiseaseCode,
    icd_converter: IcdConverter,
) -> pd.DataFrame:
    """Helper function to filter visits based on a disease."""
    hids = icd_converter.get_pos_ids(diag, disease)
    return visits[visits[CohortHeader.HOSPITAL_ADMISSION_ID].isin(hids)]


def filter_visits(
    visits: pd.DataFrame,
    disease_readmission: Optional[DiseaseCode],
    disease_selection: Optional[DiseaseCode],
) -> pd.DataFrame:
    """# Filter visits based on readmission due to a specific disease and on disease selection"""

    icd_converter = IcdConverter()

    diag = load_diagnosis_icd()[
        [
            DiagnosesIcd.ICD_CODE,
            DiagnosesIcd.ICD_VERSION,
            DiagnosesIcd.HOSPITAL_ADMISSION_ID,
        ]
    ]
    diag = icd_converter.standardize_icd(diag)
    diag.dropna(subset=[DiagnosesIcd.ROOT], inplace=True)
    if disease_readmission:
        visits = filter_by_disease(visits, diag, disease_readmission, icd_converter)

    if disease_selection:
        visits = filter_by_disease(visits, diag, disease_selection, icd_converter)

    return visits
