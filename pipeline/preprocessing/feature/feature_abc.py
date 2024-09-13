from abc import ABC, abstractmethod
import pandas as pd
from enum import StrEnum


class FeatureGroup(StrEnum):
    DIAGNOSES = "DIAGNOSES"
    PROCEDURES = "PROCEDURES"
    MEDICATIONS = "MEDICATIONS"
    OUTPUT = "OUTPUT EVENTS"
    CHART = "CHART EVENTS"
    LAB = "LAB EVENTS"


class Feature(ABC):
    @staticmethod
    @abstractmethod
    def group() -> FeatureGroup:
        pass

    """
    Abstract base class for a feature in the dataset.
    """

    @abstractmethod
    def extract_from(self, cohort: pd.DataFrame) -> pd.DataFrame:
        """
        Generate the feature data from a cohort and return it as a DataFrame.
        """
        pass
