import pandas as pd
import numpy as np
import datetime

from pipeline.file_info.preproc.cohort import (
    COHORT_PATH,
    CohortHeader,
    CohortWithIcuHeader,
    CohortWithoutIcuHeader,
)
import logging
from pipeline.file_info.raw.hosp import AdmissionsHeader

from pipeline.prediction_task import PredictionTask, TargetType
from pipeline.extract.csv_tools import save_data

logger = logging.getLogger()


class Cohort:
    def __init__(
        self,
        with_icu: bool,
        name: str,
        df: pd.DataFrame = pd.DataFrame(),
    ):
        self.df = df
        self.with_icu = with_icu
        self.name = name
        self.summary_name = f"summary_{name}"
        self.admit_col = (
            CohortWithIcuHeader.IN_TIME
            if self.with_icu
            else CohortWithoutIcuHeader.ADMIT_TIME
        )
        self.disch_col = (
            CohortWithIcuHeader.OUT_TIME
            if self.with_icu
            else CohortWithoutIcuHeader.DISCH_TIME
        )

    @staticmethod
    def _clean_visits(visits: pd.DataFrame, required_cols: list) -> pd.DataFrame:
        return visits.dropna(subset=required_cols)

    def prepare_mort_labels(self, visits: pd.DataFrame):
        """Prepare mortality labels by checking if the patient died during their hospital stay."""
        # Drop rows with missing admission or discharge times
        visits = self._clean_visits(visits, [self.admit_col, self.disch_col])
        visits[CohortHeader.DOD] = pd.to_datetime(visits[CohortHeader.DOD])
        # Assign mortality labels: 1 if death occurred between admission and discharge, 0 otherwise
        visits[CohortHeader.LABEL] = np.where(
            (visits[CohortHeader.DOD] >= visits[self.admit_col])
            & (visits[CohortHeader.DOD] <= visits[self.disch_col]),
            1,
            0,
        )
        logger.info(
            f"[ MORTALITY LABELS FINISHED: {visits[CohortHeader.LABEL].sum()} Mortality Cases ]"
        )
        return visits

    def prepare_readm_labels(self, visits: pd.DataFrame, nb_days: int) -> pd.DataFrame:
        """Prepare readmission labels based on whether the patient was readmitted within a certain time period."""
        gap = datetime.timedelta(days=nb_days)
        visits["next_admit"] = (
            visits.sort_values(by=[self.admit_col])
            .groupby(CohortHeader.PATIENT_ID)[self.admit_col]
            .shift(-1)
        )
        visits["time_to_next"] = visits["next_admit"] - visits[self.disch_col]
        visits[CohortHeader.LABEL] = (
            visits["time_to_next"].notnull() & (visits["time_to_next"] <= gap)
        ).astype(int)
        readmit_cases = visits[CohortHeader.LABEL].sum()
        logger.info(
            f"[ READMISSION LABELS FINISHED: {readmit_cases} Readmission Cases ]"
        )
        return visits.drop(columns=["next_admit", "time_to_next"])

    def prepare_los_labels(self, visits: pd.DataFrame, nb_days) -> pd.DataFrame:
        """Prepare length of stay labels based on whether the stay exceeded a certain number of days."""
        visits = self._clean_visits(
            visits, [self.admit_col, self.disch_col, CohortHeader.LOS]
        )

        visits[CohortHeader.LABEL] = (visits[CohortHeader.LOS] > nb_days).astype(int)
        logger.info(
            f"[ LOS LABELS FINISHED: {visits[CohortHeader.LABEL].sum()} LOS Cases ]"
        )
        return visits

    def prepare_labels(self, visits: pd.DataFrame, prediction_task: PredictionTask):
        if prediction_task.target_type == TargetType.MORTALITY:
            df = self.prepare_mort_labels(visits)
        elif prediction_task.target_type == TargetType.READMISSION:
            df = self.prepare_readm_labels(visits, prediction_task.nb_days)
        elif prediction_task.target_type == TargetType.LOS:
            df = self.prepare_los_labels(visits, prediction_task.nb_days)
        df = df.sort_values(by=[CohortHeader.PATIENT_ID, self.admit_col])
        self.df = df.rename(columns={AdmissionsHeader.RACE: CohortHeader.ETHICITY})

    def save(self) -> pd.DataFrame:
        save_data(self.df, COHORT_PATH / f"{self.name}.csv.gz", "COHORT")

    def save_summary(self):
        summary = "\n".join(
            [
                f"{self.df} FOR {' ICU' if self.with_icu else ''} DATA",
                f"# Admission Records: {self.df.shape[0]}",
                f"# Patients: {self.df[CohortHeader.PATIENT_ID].nunique()}",
                f"# Positive cases: {self.df[self.df[CohortHeader.LABEL]==1].shape[0]}",
                f"# Negative cases: {self.df[self.df[CohortHeader.LABEL]==0].shape[0]}",
            ]
        )
        with open(COHORT_PATH / f"{self.summary_name}.txt", "w") as f:
            f.write(summary)


def load_cohort(use_icu: bool, file_name: str) -> pd.DataFrame:
    """Load cohort data from a CSV file."""
    cohort_path = COHORT_PATH / f"{file_name}.csv.gz"
    try:
        return pd.read_csv(
            cohort_path,
            compression="gzip",
            parse_dates=[
                (
                    CohortWithIcuHeader.IN_TIME
                    if use_icu
                    else CohortWithoutIcuHeader.ADMIT_TIME
                )
            ],
        )
    except FileNotFoundError:
        logger.error(f"Cohort file not found at {cohort_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading cohort file: {e}")
        raise
