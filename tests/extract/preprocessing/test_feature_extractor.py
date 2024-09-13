from pipeline.preprocessing.feature.feature_extractor import (
    FeatureExtractor,
)
from pipeline.preprocessing.feature.feature_abc import FeatureGroup


def test_feature_icu_all_true():
    feature_extractor = FeatureExtractor(
        cohort_output="cohort_mortality_with_icu_0",
        use_icu=True,
        for_diagnoses=True,
        for_output_events=True,
        for_chart_events=True,
        for_procedures=True,
        for_medications=True,
        for_labs=True,
    )
    result = feature_extractor.save_features()
    assert len(result) == 5

    assert len(result[FeatureGroup.DIAGNOSES]) == 2647
    assert result[FeatureGroup.DIAGNOSES].columns.tolist() == [
        "subject_id",
        "hadm_id",
        "icd_code",
        "root_icd10_convert",
        "root",
        "stay_id",
    ]
    assert len(result[FeatureGroup.PROCEDURES]) == 1435
    assert result[FeatureGroup.PROCEDURES].columns.tolist() == [
        "subject_id",
        "hadm_id",
        "stay_id",
        "itemid",
        "starttime",
        "intime",
        "event_time_from_admit",
    ]
    assert len(result[FeatureGroup.MEDICATIONS]) == 11038
    assert result[FeatureGroup.MEDICATIONS].columns.tolist() == [
        "subject_id",
        "hadm_id",
        "starttime",
        "start_hours_from_admit",
        "stop_hours_from_admit",
        "stay_id",
        "itemid",
        "endtime",
        "rate",
        "amount",
        "orderid",
    ]
    assert len(result[FeatureGroup.OUTPUT]) == 9362
    assert result[FeatureGroup.OUTPUT].columns.tolist() == [
        "subject_id",
        "hadm_id",
        "stay_id",
        "itemid",
        "charttime",
        "intime",
        "event_time_from_admit",
    ]
    assert len(result[FeatureGroup.CHART]) == 162571
    assert result[FeatureGroup.CHART].columns.tolist() == [
        "stay_id",
        "itemid",
        "valuenum",
        "event_time_from_admit",
    ]


def test_feature_non_icu_all_true():
    feature_extractor = FeatureExtractor(
        cohort_output="cohort_readmission_without_icu_30_I50",
        use_icu=False,
        for_diagnoses=True,
        for_output_events=True,
        for_chart_events=True,
        for_procedures=True,
        for_medications=True,
        for_labs=True,
    )
    result = feature_extractor.save_features()
    assert len(result) == 4
    assert len(result[FeatureGroup.DIAGNOSES]) == 1273
    assert result[FeatureGroup.DIAGNOSES].columns.tolist() == [
        "subject_id",
        "hadm_id",
        "icd_code",
        "root_icd10_convert",
        "root",
    ]
    assert len(result[FeatureGroup.PROCEDURES]) == 136
    assert result[FeatureGroup.PROCEDURES].columns.tolist() == [
        "subject_id",
        "hadm_id",
        "icd_code",
        "icd_version",
        "chartdate",
        "admittime",
        "proc_time_from_admit",
    ]
    assert len(result[FeatureGroup.MEDICATIONS]) == 4803
    assert result[FeatureGroup.MEDICATIONS].columns.tolist() == [
        "subject_id",
        "hadm_id",
        "starttime",
        "start_hours_from_admit",
        "stop_hours_from_admit",
        "stoptime",
        "drug",
        "nonproprietaryname",
        "dose_val_rx",
        "EPC",
    ]
    assert len(result[FeatureGroup.LAB]) == 22029
    assert result[FeatureGroup.LAB].columns.tolist() == [
        "subject_id",
        "hadm_id",
        "charttime",
        "itemid",
        "admittime",
        "lab_time_from_admit",
        "valuenum",
        "valueuom",
    ]
