from enum import StrEnum
from pipeline.file_info.preproc.feature.path_prefix import FEATURE_EXTRACT_PATH

FEATURE_CHART_EVENTS_PATH = FEATURE_EXTRACT_PATH / "chart_events.csv.gz"


class ChartEventsFeatureHeader(StrEnum):
    STAY_ID = "stay_id"
    ITEM_ID = "itemid"
    VALUE_NUM = "valuenum"
    EVENT_TIME_FROM_ADMIT = "event_time_from_admit"
