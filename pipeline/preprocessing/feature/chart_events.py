from tqdm import tqdm
from pipeline.preprocessing.feature.feature_abc import Feature, FeatureGroup
import logging
import pandas as pd

from pipeline.file_info.raw.icu import ChartEventsHeader
from pipeline.extract.raw.icu import load_chart_events
from pipeline.file_info.preproc.cohort import CohortWithIcuHeader
from pipeline.file_info.preproc.feature.chart_events import ChartEventsFeatureHeader
from pipeline.conversion.uom import drop_wrong_uom

logger = logging.getLogger()


class ChartEvents(Feature):
    def __init__(self, df: pd.DataFrame = pd.DataFrame(), chunksize: int = 10000000):
        self.df = df
        self.chunksize = chunksize
        self.final_df = pd.DataFrame()

    def group() -> str:
        return FeatureGroup.CHART

    def df(self) -> pd.DataFrame:
        return self.df

    def extract_from(self, cohort: pd.DataFrame) -> pd.DataFrame:
        """Function for processing hospital observations from a pickled cohort, optimized for memory efficiency."""
        logger.info("[EXTRACTING CHART EVENTS DATA]")
        processed_chunks = [
            self.process_chunk_chart_events(chunk, cohort)
            for chunk in tqdm(load_chart_events(self.chunksize))
        ]
        chart = pd.concat(processed_chunks, ignore_index=True)

        """Log statistics about the chart events before drop."""
        logger.info(
            f"# Unique Events: {chart[ChartEventsFeatureHeader.ITEM_ID].nunique()}"
        )
        logger.info(
            f"# Admissions: {chart[ChartEventsFeatureHeader.STAY_ID].nunique()}"
        )
        logger.info(f"Total rows: {chart.shape[0]}")

        chart = drop_wrong_uom(chart, 0.95)
        """Log statistics about the chart events."""
        logger.info(
            f"# Unique Events: {chart[ChartEventsFeatureHeader.ITEM_ID].nunique()}"
        )
        logger.info(
            f"# Admissions: {chart[ChartEventsFeatureHeader.STAY_ID].nunique()}"
        )
        logger.info(f"Total rows: {chart.shape[0]}")
        chart = chart[[h.value for h in ChartEventsFeatureHeader]]
        self.df = chart
        return chart

    def process_chunk_chart_events(
        self, chunk: pd.DataFrame, cohort: pd.DataFrame
    ) -> pd.DataFrame:
        """Process a single chunk of chart events."""
        chunk = chunk.dropna(subset=[ChartEventsHeader.VALUENUM])
        chunk = chunk.merge(
            cohort[[CohortWithIcuHeader.STAY_ID, CohortWithIcuHeader.IN_TIME]],
            on=ChartEventsHeader.STAY_ID,
        )
        chunk[ChartEventsFeatureHeader.EVENT_TIME_FROM_ADMIT] = (
            chunk[ChartEventsHeader.CHARTTIME] - chunk[CohortWithIcuHeader.IN_TIME]
        )
        chunk = chunk.drop(["charttime", "intime"], axis=1)
        chunk = chunk.dropna()
        chunk = chunk.drop_duplicates()
        return chunk
