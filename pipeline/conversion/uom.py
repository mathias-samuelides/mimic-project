import pandas as pd


def drop_wrong_uom(data: pd.DataFrame, cut_off: float) -> pd.DataFrame:
    """Drop rows with uncommon units of measurement (uom) for each itemid, based on a cut-off frequency.

    Args:
        data (pd.DataFrame): The input DataFrame containing the data with 'itemid' and 'valueuom' columns.
        cut_off (float): The cut-off frequency (0 < cut_off <= 1) used to filter out uncommon units of measurement.

    Returns:
        pd.DataFrame: The filtered DataFrame where rows with uncommon uom are dropped.
    """

    # Create a function to filter each group
    def filter_by_uom_frequency(group):
        value_counts = group["valueuom"].value_counts()
        most_frequent_uom = value_counts.idxmax()
        frequency = value_counts.max()

        # Check if the most frequent uom meets the cut-off criteria
        if frequency / len(group) > cut_off:
            return group[group["valueuom"] == most_frequent_uom]
        return group

    # Apply the filter function to each group and concatenate the results
    filtered_data = (
        data.groupby("itemid")  # Group by 'itemid'
        .apply(
            filter_by_uom_frequency, include_groups=False
        )  # Apply the filter function to each group
        .reset_index(drop=True)  # Reset index after filtering
    )

    # Merge back the 'itemid' column to ensure consistency
    filtered_data = filtered_data.merge(
        data[["itemid"]], left_index=True, right_index=True
    )
    breakpoint()
    return filtered_data
