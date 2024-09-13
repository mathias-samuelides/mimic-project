from pipeline.preprocessing.feature.feature_abc import Feature, FeatureGroup
from pipeline.file_info.preproc.cohort import (
    CohortWithIcuHeader,
    CohortHeader,
    CohortWithoutIcuHeader,
)
from pipeline.file_info.preproc.feature.medications import (
    MedicationsFeatureHeader,
    MedicationsFeatureWithIcuHeader,
    MedicationsFeatureWithoutIcuHeader,
)
from pipeline.extract.raw.icu import load_input_events
from pipeline.extract.raw.hosp import load_prescriptions
from pipeline.file_info.raw.icu import InputEventsHeader
from pipeline.file_info.raw.hosp import PrescriptionsHeader
import pandas as pd
from pipeline.conversion.ndc import prepare_ndc_mapping, get_EPC, convert_ndc_to_string
from pipeline.file_info.code_map import NdcMapHeader


class Medications(Feature):
    def __init__(
        self,
        with_icu: bool,
        df: pd.DataFrame = pd.DataFrame(),
        group_code: bool = False,
    ):
        self.with_icu = with_icu  # Boolean indicating if ICU data is included
        self.group_code = group_code  # Whether to group medication codes
        self.df = df  # DataFrame containing medication data
        self.preproc_df = pd.DataFrame()  # DataFrame for preprocessed data
        # Use different ID depending on whether ICU data is included
        self.admisson_id = (
            CohortWithIcuHeader.STAY_ID
            if self.with_icu
            else CohortHeader.HOSPITAL_ADMISSION_ID
        )

    def group() -> FeatureGroup:
        """Returns the feature group for medications."""
        return FeatureGroup.MEDICATIONS

    def extract_from(self, cohort: pd.DataFrame) -> pd.DataFrame:
        """
        Extract medication data from the cohort and merge with prescription or ICU input events.

        Args:
            cohort (pd.DataFrame): The cohort DataFrame containing hospital admissions or ICU stays.

        Returns:
            pd.DataFrame: The merged medication data with timing information relative to admission.
        """
        cohort_headers = (
            [
                CohortHeader.HOSPITAL_ADMISSION_ID,
                CohortWithIcuHeader.STAY_ID,
                CohortWithIcuHeader.IN_TIME,
            ]
            if self.with_icu
            else [CohortHeader.HOSPITAL_ADMISSION_ID, CohortWithoutIcuHeader.ADMIT_TIME]
        )

        # Validate that cohort contains necessary columns
        required_columns = cohort_headers
        if not all(col in cohort.columns for col in required_columns):
            raise ValueError(f"Missing required columns: {required_columns}")

        # Select appropriate medication data source (ICU or non-ICU)
        admissions = cohort[cohort_headers]
        raw_med = load_input_events() if self.with_icu else load_prescriptions()

        # Merge medication data with cohort admissions
        medications = raw_med.merge(
            admissions,
            on=self.admisson_id,
        )

        # Calculate medication start and stop times relative to admission
        admit_header = (
            CohortWithIcuHeader.IN_TIME
            if self.with_icu
            else CohortWithoutIcuHeader.ADMIT_TIME
        )
        medications[MedicationsFeatureHeader.START_HOURS_FROM_ADMIT] = (
            medications[InputEventsHeader.STARTTIME] - medications[admit_header]
        )
        medications[MedicationsFeatureHeader.STOP_HOURS_FROM_ADMIT] = (
            medications[
                (
                    InputEventsHeader.ENDTIME
                    if self.with_icu
                    else PrescriptionsHeader.STOP_TIME
                )
            ]
            - medications[admit_header]
        )

        # Clean or normalize data depending on ICU or non-ICU context
        medications = (
            medications.dropna()
            if self.with_icu
            else self.normalize_non_icu(medications)
        )

        # Select relevant columns for medications
        cols = [h.value for h in MedicationsFeatureHeader] + [
            h.value
            for h in (
                MedicationsFeatureWithIcuHeader
                if self.with_icu
                else MedicationsFeatureWithoutIcuHeader
            )
        ]
        medications = medications[cols]

        self.df = medications
        return medications

    def normalize_non_icu(self, med: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize medication data for non-ICU cases.

        Args:
            med (pd.DataFrame): The medication dataframe.

        Returns:
            pd.DataFrame: The normalized dataframe.
        """
        # Clean and normalize the drug names
        med[MedicationsFeatureWithoutIcuHeader.DRUG] = (
            med[MedicationsFeatureWithoutIcuHeader.DRUG]
            .fillna("")
            .astype(str)
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
        )

        # Handle missing NDC codes
        MISSING_NDC = -1
        med[PrescriptionsHeader.NDC] = (
            med[PrescriptionsHeader.NDC].fillna(MISSING_NDC).astype("Int64")
        )

        # Convert NDC codes to strings
        med[NdcMapHeader.NEW_NDC] = med[PrescriptionsHeader.NDC].apply(
            convert_ndc_to_string
        )

        # Merge with NDC mapping table
        ndc_map = prepare_ndc_mapping()
        med = med.merge(ndc_map, on=NdcMapHeader.NEW_NDC, how="left")

        # Extract pharmacological class information
        med[MedicationsFeatureWithoutIcuHeader.EPC] = med["pharm_classes"].apply(
            get_EPC
        )
        return med
