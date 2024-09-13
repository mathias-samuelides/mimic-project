from typing import Optional
import pandas as pd
from pipeline.file_info.raw.hosp import PatientsHeader
from pipeline.file_info.raw.hosp import AdmissionsHeader
from pipeline.file_info.raw.icu import IcuStaysHeader
from pipeline.file_info.preproc.cohort import (
    CohortHeader,
    CohortWithIcuHeader,
    CohortWithoutIcuHeader,
)
from pipeline.prediction_task import TargetType, DiseaseCode
from pipeline.conversion.icd import IcdConverter

from pipeline.file_info.raw.hosp import (
    DiagnosesIcdHeader,
)
from pipeline.extract.raw.hosp import load_diagnosis_icd


def make_patients(hosp_patients: pd.DataFrame) -> pd.DataFrame:
    patients = hosp_patients[
        [
            PatientsHeader.ID,
            PatientsHeader.ANCHOR_YEAR,
            PatientsHeader.ANCHOR_YEAR_GROUP,
            PatientsHeader.ANCHOR_AGE,
            PatientsHeader.DOD,
            PatientsHeader.GENDER,
        ]
    ].copy()
    max_anchor_year_group = (
        patients[PatientsHeader.ANCHOR_YEAR_GROUP].str.slice(start=-4).astype(int)
    )
    # To identify visits with prediction windows outside the range 2008-2019.
    patients[CohortHeader.MIN_VALID_YEAR] = (
        hosp_patients[PatientsHeader.ANCHOR_YEAR] + 2008 - max_anchor_year_group
    )
    patients = patients.rename(columns={PatientsHeader.ANCHOR_AGE: CohortHeader.AGE})[
        [
            PatientsHeader.ID,
            CohortHeader.AGE,
            CohortHeader.MIN_VALID_YEAR,
            PatientsHeader.DOD,
            PatientsHeader.GENDER,
        ]
    ]
    return patients


def make_visits_with_icu(
    icustays: pd.DataFrame, patients: pd.DataFrame, target_type: TargetType
) -> pd.DataFrame:
    if target_type != TargetType.READMISSION:
        return icustays
    # Filter out stays where either there is no death or the death occurred after ICU discharge
    patients_dod = patients[[PatientsHeader.ID, PatientsHeader.DOD]]
    visits = icustays.merge(patients_dod, on=IcuStaysHeader.PATIENT_ID)
    filtered_visits = visits.loc[
        (visits[PatientsHeader.DOD].isna())
        | (visits[PatientsHeader.DOD] >= visits[IcuStaysHeader.OUTTIME])
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
    admissions[AdmissionsHeader.LOS] = (
        admissions[AdmissionsHeader.DISCHTIME] - admissions[AdmissionsHeader.ADMITTIME]
    ).dt.days

    if target_type == TargetType.READMISSION:
        # Filter out hospitalizations where the patient expired
        admissions = admissions[admissions[AdmissionsHeader.HOSPITAL_EXPIRE_FLAG] == 0]
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
            DiagnosesIcdHeader.ICD_CODE,
            DiagnosesIcdHeader.ICD_VERSION,
            DiagnosesIcdHeader.HOSPITAL_ADMISSION_ID,
        ]
    ]
    diag = icd_converter.standardize_icd(diag)
    diag.dropna(subset=[DiagnosesIcdHeader.ROOT], inplace=True)
    if disease_readmission:
        visits = filter_by_disease(visits, diag, disease_readmission, icd_converter)

    if disease_selection:
        visits = filter_by_disease(visits, diag, disease_selection, icd_converter)

    return visits
