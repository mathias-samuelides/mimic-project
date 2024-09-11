from pipeline.prediction_task import PredictionTask
from pipeline.preprocessing.cohort.cohort import Cohort
from pipeline.extract.raw.hosp import load_patients, load_admissions, Admissions
from pipeline.extract.raw.icu import load_icustays
from pipeline.preprocessing.cohort.visit import (
    make_icu_visits,
    make_no_icu_visits,
    make_patients,
    filter_visits,
)
import pandas as pd
from pipeline.file_info.preproc.cohort import (
    CohortHeader,
)


class CohortExtractor:
    """
    Extract cohorts based on the prediction task.

    Parameters:
    - prediction_task: The prediction task defining the type of cohort to extract.
    - output: Optional output path for saving the extracted cohort.
    """

    def __init__(self, prediction_task: PredictionTask, output: str = None):
        self.prediction_task = prediction_task
        self.output = output

    def fill_output(self) -> None:
        """Fill the output details based on the prediction task"""
        cohort_details = self.prediction_task.target_type.lower().replace(" ", "_")

        if self.prediction_task.nb_days is not None:
            cohort_details += f"_{self.prediction_task.nb_days}"

        if self.prediction_task.disease_readmission:
            cohort_details += f"_{self.prediction_task.disease_readmission}"

        if self.prediction_task.disease_selection:
            cohort_details += f"_{self.prediction_task.disease_selection}"

        self.output = f"cohort_{cohort_details}"

    def make_visits(self, patients, admissions):
        if self.prediction_task.use_icu:
            icu_icustays = load_icustays()
            return make_icu_visits(
                icu_icustays, patients, self.prediction_task.target_type
            )
        else:
            return make_no_icu_visits(admissions, self.prediction_task.target_type)

    def filter_and_merge_visits(
        self,
        visits: pd.DataFrame,
        patients: pd.DataFrame,
        admissions: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters and merges visit records with patient and admission data."""
        # Filter the visits based on readmission and disease selection
        visits = filter_visits(
            visits,
            self.prediction_task.disease_readmission,
            self.prediction_task.disease_selection,
        )
        # Process patient data and filter by age
        patients_data = make_patients(patients)
        patients_filtered = patients_data.loc[patients_data["age"] >= 18]
        admissions_info = admissions[
            [
                Admissions.HOSPITAL_ADMISSION_ID,
                Admissions.INSURANCE,
                Admissions.RACE,
            ]
        ]
        # Merge visits with patients and admissions data
        visits = visits.merge(patients_filtered, on=CohortHeader.PATIENT_ID)
        visits = visits.merge(admissions_info, on=CohortHeader.HOSPITAL_ADMISSION_ID)
        return visits

    def extract(self) -> Cohort:
        if not self.output:
            self.fill_output()
        patients = load_patients()
        admissions = load_admissions()
        visits = self.make_visits(patients, admissions)
        visits = self.filter_and_merge_visits(visits, patients, admissions)
        cohort = Cohort(
            with_icu=self.prediction_task.use_icu,
            name=self.output,
        )
        cohort.prepare_labels(visits, self.prediction_task)
        cohort.save()
        cohort.save_summary()
        return cohort
