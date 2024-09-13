import pandas as pd
from collections import defaultdict
from typing import Union, List, Tuple
from functools import partial
from multiprocessing import Pool

from pipeline.file_info.raw.hosp import AdmissionsHeader
from pipeline.file_info.preproc.feature.lab_events import LabEventsFeatureHeader


INPUTED_HOSPITAL_ADMISSION_ID_HEADER = "hadm_id_new"


def hadm_imputer(
    charttime: pd.Timestamp,
    hadm_old: Union[str, float],
    hadm_ids_w_timestamps: List[Tuple[str, pd.Timestamp, pd.Timestamp]],
) -> Tuple[str, pd.Timestamp, pd.Timestamp]:
    """
    Impute hospital admission ID based on the chart time and a list of admission IDs with timestamps.
    """

    # If old HADM ID exists and is valid, use that
    if not pd.isna(hadm_old):
        hadm_old = str(int(hadm_old))
        for h_id, adm_time, disch_time in hadm_ids_w_timestamps:
            if h_id == hadm_old:
                return hadm_old, adm_time, disch_time

    # Filter and sort HADM IDs based on their proximity to the lab event charttime
    valid_hadm_ids = [
        (hadm_id, admittime, dischtime)
        for hadm_id, admittime, dischtime in hadm_ids_w_timestamps
        if admittime <= charttime <= dischtime
    ]
    valid_hadm_ids.sort(key=lambda x: abs(charttime - x[1]))

    # Return the most relevant HADM ID or None if no valid ID is found

    return valid_hadm_ids[0] if valid_hadm_ids else (None, None, None)


def impute_row(row, subject_hadm_admittime_tracker):
    """Helper function to impute data for a single row."""
    new_hadm_id, new_admittime, new_dischtime = hadm_imputer(
        row[LabEventsFeatureHeader.CHART_TIME],
        row[LabEventsFeatureHeader.HOSPITAL_ADMISSION_ID],
        subject_hadm_admittime_tracker.get(row[LabEventsFeatureHeader.PATIENT_ID], []),
    )
    return pd.Series(
        [new_hadm_id, new_admittime, new_dischtime],
        index=[
            INPUTED_HOSPITAL_ADMISSION_ID_HEADER,
            AdmissionsHeader.ADMITTIME,
            AdmissionsHeader.DISCHTIME,
        ],
    )


def process_chunk(
    chunk: pd.DataFrame, subject_hadm_admittime_tracker: dict
) -> pd.DataFrame:
    """Process a single chunk for imputing HADM IDs."""
    imputed_data = chunk.apply(
        lambda row: impute_row(row, subject_hadm_admittime_tracker), axis=1
    )
    return pd.concat([chunk, imputed_data], axis=1)


def impute_hadm_ids(lab_table: pd.DataFrame, admissions: pd.DataFrame) -> pd.DataFrame:
    """Impute missing HADM IDs in the lab table."""
    # Create tracker from admission table
    subject_hadm_admittime_tracker = defaultdict(list)
    for row in admissions.itertuples():
        subject_hadm_admittime_tracker[row.subject_id].append(
            (row.hadm_id, row.admittime, row.dischtime)
        )

    # Prepare chunks and function for parallel processing
    chunk_size = 100
    chunks = [
        lab_table[i : i + chunk_size] for i in range(0, len(lab_table), chunk_size)
    ]
    process_func = partial(
        process_chunk, subject_hadm_admittime_tracker=subject_hadm_admittime_tracker
    )

    # Parallel processing
    with Pool(8) as pool:
        processed_chunks = pool.map(process_func, chunks)
    non_empty_chunks = [chunk.dropna(how="all", axis=1) for chunk in processed_chunks]
    # Consolidate processed chunks
    return pd.concat(non_empty_chunks, ignore_index=True)
