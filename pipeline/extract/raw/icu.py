import pandas as pd
from pipeline.file_info.raw.icu import (
    ICUSTAY_PATH,
    OUTPUT_EVENT_PATH,
    CHART_EVENTS_PATH,
    INPUT_EVENT_PATH,
    PROCEDURE_EVENTS_PATH,
    IcuStaysHeader,
    ChartEventsHeader,
    OutputEventsHeader,
    InputEventsHeader,
    ProceduresEventsHeader,
)


def load_icustays() -> pd.DataFrame:
    return pd.read_csv(
        ICUSTAY_PATH,
        compression="gzip",
        parse_dates=[IcuStaysHeader.INTIME, IcuStaysHeader.OUTTIME],
    )


def load_output_events() -> pd.DataFrame:
    return pd.read_csv(
        OUTPUT_EVENT_PATH,
        compression="gzip",
        parse_dates=[OutputEventsHeader.CHART_TIME],
    ).drop_duplicates()


def load_chart_events(chunksize: int) -> pd.DataFrame:
    return pd.read_csv(
        CHART_EVENTS_PATH,
        compression="gzip",
        usecols=[c for c in ChartEventsHeader],
        parse_dates=[ChartEventsHeader.CHARTTIME],
        chunksize=chunksize,
    )


def load_input_events() -> pd.DataFrame:
    return pd.read_csv(
        INPUT_EVENT_PATH,
        compression="gzip",
        usecols=[f for f in InputEventsHeader],
        parse_dates=[InputEventsHeader.STARTTIME, InputEventsHeader.ENDTIME],
    )


def load_procedure_events() -> pd.DataFrame:
    return pd.read_csv(
        PROCEDURE_EVENTS_PATH,
        compression="gzip",
        usecols=[h for h in ProceduresEventsHeader],
        parse_dates=[ProceduresEventsHeader.START_TIME],
    ).drop_duplicates()
