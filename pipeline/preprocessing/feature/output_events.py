from pipeline.preprocessing.feature.feature_abc import Feature, FeatureGroup
import logging
import pandas as pd
from pipeline.file_info.preproc.feature.output_events import OutputEventsFeatureHeader
from pipeline.file_info.preproc.cohort import CohortWithIcuHeader
from pipeline.file_info.raw.icu import OutputEventsHeader
from pipeline.extract.raw.icu import load_output_events

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class OutputEvents(Feature):
    def __init__(self, df: pd.DataFrame = pd.DataFrame()):
        self.df = df
        self.final_df = pd.DataFrame()

    def group() -> str:
        return FeatureGroup.OUTPUT

    def extract_from(self, cohort: pd.DataFrame) -> pd.DataFrame:
        """Function for getting hosp observations pertaining to a pickled cohort.
        Function is structured to save memory when reading and transforming data."""
        logger.info("[EXTRACTING OUTPUT EVENTS DATA]")
        raw_out = load_output_events()
        out = raw_out.merge(
            cohort[
                [
                    CohortWithIcuHeader.STAY_ID,
                    CohortWithIcuHeader.IN_TIME,
                    CohortWithIcuHeader.OUT_TIME,
                ]
            ],
            on=CohortWithIcuHeader.STAY_ID,
        )
        out[OutputEventsFeatureHeader.EVENT_TIME_FROM_ADMIT] = (
            out[OutputEventsHeader.CHART_TIME] - out[CohortWithIcuHeader.IN_TIME]
        )
        out = out.dropna()

        # Print unique counts and value_counts
        logger.info(f"# Unique Events: {out[OutputEventsHeader.ITEM_ID].nunique()}")
        logger.info(f"# Admissions: {out[OutputEventsHeader.STAY_ID].nunique()}")
        logger.info(f"Total rows: {out.shape[0]}")
        out = out[[h.value for h in OutputEventsFeatureHeader]]
        self.df = out
        return out
