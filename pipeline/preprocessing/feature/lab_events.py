from tqdm import tqdm
from pipeline.preprocessing.feature.feature_abc import Feature, FeatureGroup
import logging
import pandas as pd
from pipeline.file_info.preproc.cohort import CohortHeader, CohortWithoutIcuHeader
from pipeline.file_info.raw.hosp import (
    AdmissionsHeader,
    LabEventsHeader,
)
from pipeline.extract.raw.hosp import load_admissions
from pipeline.extract.raw.hosp import load_lab_events
from pipeline.preprocessing.admission_imputer import (
    INPUTED_HOSPITAL_ADMISSION_ID_HEADER,
    impute_hadm_ids,
)

logger = logging.getLogger()


class LabEvents(Feature):
    def group() -> str:
        return FeatureGroup.LAB

    def __init__(self, df: pd.DataFrame = pd.DataFrame(), chunksize: int = 10000000):
        self.df = df
        self.chunksize = chunksize
        self.final_df = pd.DataFrame()

    def df(self):
        return self.df

    def extract_from(self, cohort: pd.DataFrame) -> pd.DataFrame:
        """Process and transform lab events data."""
        logger.info("[EXTRACTING LABS DATA]")
        admissions = load_admissions()[
            [
                AdmissionsHeader.PATIENT_ID,
                AdmissionsHeader.ID,
                AdmissionsHeader.ADMITTIME,
                AdmissionsHeader.DISCHTIME,
            ]
        ]
        usecols = [
            LabEventsHeader.ITEM_ID,
            LabEventsHeader.PATIENT_ID,
            LabEventsHeader.HOSPITAL_ADMISSION_ID,
            LabEventsHeader.CHART_TIME,
            LabEventsHeader.VALUE_NUM,
            LabEventsHeader.VALUE_UOM,
        ]
        processed_chunks = [
            self.process_lab_chunk(chunk, admissions, cohort)
            for chunk in tqdm(
                load_lab_events(chunksize=self.chunksize, use_cols=usecols)
            )
        ]
        labevents = pd.concat(processed_chunks, ignore_index=True)
        labevents = labevents[[h.value for h in LabEventsHeader]]
        self.df = labevents
        return labevents

    def process_lab_chunk(
        self, chunk: pd.DataFrame, admissions: pd.DataFrame, cohort: pd.DataFrame
    ) -> pd.DataFrame:
        """Process a single chunk of lab events."""
        chunk = chunk.dropna(subset=[LabEventsHeader.VALUE_NUM]).fillna(
            {LabEventsHeader.VALUE_UOM: 0}
        )
        chunk = chunk[
            chunk[LabEventsHeader.PATIENT_ID].isin(cohort[CohortHeader.PATIENT_ID])
        ]
        chunk_with_hadm, chunk_no_hadm = (
            chunk[chunk[LabEventsHeader.HOSPITAL_ADMISSION_ID].notna()],
            chunk[chunk[LabEventsHeader.HOSPITAL_ADMISSION_ID].isna()],
        )

        chunk_imputed = impute_hadm_ids(chunk_no_hadm.copy(), admissions)
        chunk_imputed[LabEventsHeader.HOSPITAL_ADMISSION_ID] = chunk_imputed[
            INPUTED_HOSPITAL_ADMISSION_ID_HEADER
        ]
        chunk_imputed = chunk_imputed[
            [
                LabEventsHeader.PATIENT_ID,
                LabEventsHeader.HOSPITAL_ADMISSION_ID,
                LabEventsHeader.ITEM_ID,
                LabEventsHeader.CHART_TIME,
                LabEventsHeader.VALUE_NUM,
                LabEventsHeader.VALUE_UOM,
            ]
        ]
        merged_chunk = pd.concat([chunk_with_hadm, chunk_imputed], ignore_index=True)
        return self.merge_with_cohort_and_calculate_lab_time(merged_chunk, cohort)

    # in utils?
    def merge_with_cohort_and_calculate_lab_time(
        self, chunk: pd.DataFrame, cohort: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge chunk with cohort data and calculate the lab time from admit time."""
        chunk = chunk.merge(
            cohort[
                [
                    CohortHeader.HOSPITAL_ADMISSION_ID,
                    CohortWithoutIcuHeader.ADMIT_TIME,
                    CohortWithoutIcuHeader.DISCH_TIME,
                ]
            ],
            on=LabEventsHeader.HOSPITAL_ADMISSION_ID,
        )
        chunk[LabEventsHeader.CHART_TIME] = pd.to_datetime(
            chunk[LabEventsHeader.CHART_TIME]
        )
        chunk[LabEventsHeader.LAB_TIME_FROM_ADMIT.value] = (
            chunk[LabEventsHeader.CHART_TIME] - chunk[LabEventsHeader.ADMIT_TIME]
        )
        return chunk.dropna()
