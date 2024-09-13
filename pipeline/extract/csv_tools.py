import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def save_data(data: pd.DataFrame, path: Path, data_name: str) -> pd.DataFrame:
    """Save DataFrame to specified path."""
    data.to_csv(path, compression="gzip", index=False)
    logger.info(f"[SUCCESSFULLY SAVED {data_name} DATA]")
    return data
