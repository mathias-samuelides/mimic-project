import pandas as pd
import numpy as np
from pipeline.file_info.code_map import NdcMapHeader
from pipeline.extract.static.code_map import load_ndc_mapping


# Read and preprocess NDC mapping table
def prepare_ndc_mapping() -> pd.DataFrame:
    """Prepares the NDC mapping table by formatting the NDC codes and deduplicating the data."""
    ndc_map = read_ndc_mapping()

    # Select relevant columns
    ndc_map = ndc_map[
        [
            NdcMapHeader.PRODUCT_NDC,
            NdcMapHeader.NON_PROPRIETARY_NAME,
            NdcMapHeader.PHARM_CLASSES,
        ]
    ]

    # Clean and normalize the non-proprietary names (convert to lowercase and handle NaN)
    ndc_map[NdcMapHeader.NON_PROPRIETARY_NAME] = (
        ndc_map[NdcMapHeader.NON_PROPRIETARY_NAME].fillna("").str.lower()
    )

    # Format NDC codes and add to a new column
    ndc_map[NdcMapHeader.NEW_NDC] = ndc_map[NdcMapHeader.PRODUCT_NDC].apply(
        format_ndc_table
    )

    # Drop duplicates based on formatted NDC and non-proprietary name
    ndc_map = ndc_map.drop_duplicates(
        subset=[NdcMapHeader.NEW_NDC, NdcMapHeader.NON_PROPRIETARY_NAME]
    )

    return ndc_map


# Convert numeric NDC to string format
def convert_ndc_to_string(ndc: int) -> str:
    """Converts NDC code to a 9-digit string, handling invalid values."""
    if ndc < 0:
        return np.nan  # Return NaN for dummy or invalid NDC values
    return str(ndc).zfill(11)[
        :-2
    ]  # Zero-fill to 11 digits and keep only first 9 digits


# Format NDC code from the table to the 9-digit format
def format_ndc_table(ndc: str) -> str:
    """Formats NDC code from the table into a 9-digit string by combining the segments."""
    parts = ndc.split("-")
    formatted_ndc = "".join(
        part.zfill(length) for part, length in zip(parts, [5, 4, 2])
    )
    return formatted_ndc[:9]  # Take only the first 9 digits (manufacturer and product)


# Read the NDC mapping file from disk
def read_ndc_mapping() -> pd.DataFrame:
    """Reads the NDC mapping file from a specified path and processes it."""
    ndc_map = load_ndc_mapping()
    ndc_map.columns = ndc_map.columns.str.lower()  # Normalize column names
    return ndc_map


# Extract Established Pharmacologic Class (EPC) from string
def get_EPC(pharm_classes: str) -> list:
    """Extracts Established Pharmacologic Class (EPC) tags from a pharmacological class string."""
    if not isinstance(pharm_classes, str):
        return np.nan  # Return NaN if input is not a string

    return [phrase for phrase in pharm_classes.split(",") if "[EPC]" in phrase]
