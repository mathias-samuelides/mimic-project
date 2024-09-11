import pandas as pd
from pipeline.file_info.raw.icu import (
    ICUSTAY_PATH,
    OUTPUT_EVENT_PATH,
    CHART_EVENTS_PATH,
    INPUT_EVENT_PATH,
    PROCEDURE_EVENTS_PATH,
    IcuStays,
    ChartEvents,
    OuputputEvents,
    InputEvents,
    ProceduresEvents,
)


def load_icustays() -> pd.DataFrame:
    return pd.read_csv(
        ICUSTAY_PATH,
        compression="gzip",
        parse_dates=[IcuStays.INTIME, IcuStays.OUTTIME],
    )


def load_output_events() -> pd.DataFrame:
    return pd.read_csv(
        OUTPUT_EVENT_PATH,
        compression="gzip",
        parse_dates=[OuputputEvents.CHART_TIME],
    ).drop_duplicates()


def load_chart_events(chunksize: int) -> pd.DataFrame:
    return pd.read_csv(
        CHART_EVENTS_PATH,
        compression="gzip",
        usecols=[c for c in ChartEvents],
        parse_dates=[ChartEvents.CHARTTIME],
        chunksize=chunksize,
    )


def load_input_events() -> pd.DataFrame:
    return pd.read_csv(
        INPUT_EVENT_PATH,
        compression="gzip",
        usecols=[f for f in InputEvents],
        parse_dates=[InputEvents.STARTTIME, InputEvents.ENDTIME],
    )


def load_procedure_events() -> pd.DataFrame:
    return pd.read_csv(
        PROCEDURE_EVENTS_PATH,
        compression="gzip",
        usecols=[h for h in ProceduresEvents],
        parse_dates=[ProceduresEvents.START_TIME],
    ).drop_duplicates()
