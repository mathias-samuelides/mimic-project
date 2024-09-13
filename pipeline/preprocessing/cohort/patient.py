import pandas as pd
from pipeline.file_info.raw.hosp import PatientsHeader
from pipeline.file_info.preproc.cohort import (
    CohortHeader,
)


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
