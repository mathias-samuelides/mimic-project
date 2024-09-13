from pipeline.preprocessing.feature.feature_abc import Feature, FeatureGroup
import logging
import pandas as pd
from pipeline.file_info.preproc.feature.procedures import (
    ProceduresFeatureHeader,
    ProceduresFeatureWithIcuHeader,
    ProceduresFeatureWithoutIcuHeader,
)
from pipeline.file_info.preproc.cohort import (
    CohortHeader,
    CohortWithIcuHeader,
    CohortWithoutIcuHeader,
)
from pipeline.file_info.raw.hosp import ProceduresIcdHeader
from pipeline.extract.raw.hosp import load_procedures_icd
from pipeline.extract.raw.icu import load_procedure_events

logger = logging.getLogger()


class Procedures(Feature):
    def __init__(
        self, use_icu: bool, df: pd.DataFrame = pd.DataFrame, keep_icd9: bool = True
    ):
        self.use_icu = use_icu
        self.keep_icd9 = keep_icd9
        self.df = df
        self.final_df = pd.DataFrame()
        self.adm_id = (
            CohortWithIcuHeader.STAY_ID
            if self.use_icu
            else CohortHeader.HOSPITAL_ADMISSION_ID
        )
        self.time_from_admit = (
            ProceduresFeatureWithIcuHeader.EVENT_TIME_FROM_ADMIT
            if self.use_icu
            else ProceduresFeatureWithoutIcuHeader.PROC_TIME_FROM_ADMIT
        )

    def group() -> str:
        return FeatureGroup.PROCEDURES

    def extract_from(self, cohort: pd.DataFrame) -> pd.DataFrame:
        logger.info("[EXTRACTING PROCEDURES DATA]")
        raw_procedures = (
            load_procedure_events() if self.use_icu else load_procedures_icd()
        )
        procedures = raw_procedures.merge(
            cohort[
                (
                    [
                        CohortHeader.PATIENT_ID,
                        CohortHeader.HOSPITAL_ADMISSION_ID,
                        CohortWithIcuHeader.STAY_ID,
                        CohortWithIcuHeader.IN_TIME,
                        CohortWithIcuHeader.OUT_TIME,
                    ]
                    if self.use_icu
                    else [
                        CohortHeader.HOSPITAL_ADMISSION_ID,
                        CohortWithoutIcuHeader.ADMIT_TIME,
                        CohortWithoutIcuHeader.DISCH_TIME,
                    ]
                )
            ],
            on=(
                CohortWithIcuHeader.STAY_ID
                if self.use_icu
                else ProceduresIcdHeader.HOSPITAL_ADMISSION_ID
            ),
        )
        procedures[self.time_from_admit] = (
            procedures[
                (
                    ProceduresFeatureWithIcuHeader.START_TIME
                    if self.use_icu
                    else ProceduresFeatureWithoutIcuHeader.CHART_DATE
                )
            ]
            - procedures[
                (
                    ProceduresFeatureWithIcuHeader.IN_TIME
                    if self.use_icu
                    else ProceduresFeatureWithoutIcuHeader.ADMIT_TIME
                )
            ]
        )
        procedures = procedures.dropna()
        procedures = procedures[
            [h.value for h in ProceduresFeatureHeader]
            + [
                h.value
                for h in (
                    ProceduresFeatureWithIcuHeader
                    if self.use_icu
                    else ProceduresFeatureWithoutIcuHeader
                )
            ]
        ]
        self.df = procedures
        return procedures
