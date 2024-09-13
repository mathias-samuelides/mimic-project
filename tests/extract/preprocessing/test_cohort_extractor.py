import pytest
from pipeline.preprocessing.cohort.cohort_extractor import CohortExtractor
from pipeline.prediction_task import PredictionTask, TargetType
from pipeline.file_info.preproc.cohort import (
    CohortHeader,
    CohortWithIcuHeader,
    CohortWithoutIcuHeader,
)


@pytest.mark.parametrize(
    "target_type, nb_days, disease_readmission, disease_selection, expected_admission_records_count, expected_patients_count, expected_positive_cases_count",
    [
        (TargetType.MORTALITY, 0, None, None, 140, 100, 10),
        (TargetType.LOS, 3, None, None, 140, 100, 55),
        (TargetType.LOS, 7, None, None, 140, 100, 20),
        (TargetType.READMISSION, 30, None, None, 128, 93, 18),
        (TargetType.READMISSION, 90, None, None, 128, 93, 22),
        (TargetType.READMISSION, 30, "I50", None, 27, 20, 2),
        (TargetType.READMISSION, 30, "I25", None, 32, 29, 2),
        (TargetType.READMISSION, 30, "N18", None, 25, 18, 2),
        (TargetType.READMISSION, 30, "J44", None, 17, 12, 3),
        (TargetType.MORTALITY, 0, None, "I50", 32, 22, 5),
    ],
)
def test_extract_cohort_with_icu(
    target_type,
    nb_days,
    disease_readmission,
    disease_selection,
    expected_admission_records_count,
    expected_patients_count,
    expected_positive_cases_count,
):
    prediction_task = PredictionTask(
        target_type, disease_readmission, disease_selection, nb_days, use_icu=True
    )
    cohort_extractor = CohortExtractor(
        prediction_task=prediction_task,
    )
    df = cohort_extractor.extract().df
    assert len(df) == expected_admission_records_count

    assert df[CohortHeader.PATIENT_ID].nunique() == expected_patients_count
    assert df[CohortWithIcuHeader.STAY_ID].nunique() == expected_admission_records_count
    assert df[CohortHeader.LABEL].sum() == expected_positive_cases_count


@pytest.mark.parametrize(
    "target_type, nb_days, disease_readmission, disease_selection, expected_admission_records_count, expected_patients_count, expected_positive_cases_count",
    [
        (TargetType.MORTALITY, 0, None, None, 275, 100, 15),
        (TargetType.LOS, 3, None, None, 275, 100, 163),
        (TargetType.LOS, 7, None, None, 275, 100, 76),
        (TargetType.READMISSION, 30, None, None, 260, 95, 52),
        (TargetType.READMISSION, 90, None, None, 260, 95, 86),
        (TargetType.READMISSION, 30, "I50", None, 55, 23, 13),
        (TargetType.READMISSION, 30, "I25", None, 68, 32, 13),
        (TargetType.READMISSION, 30, "N18", None, 63, 22, 10),
        (TargetType.READMISSION, 30, "J44", None, 26, 12, 7),
    ],
)
def test_extract_cohort_without_icu(
    target_type,
    nb_days,
    disease_readmission,
    disease_selection,
    expected_admission_records_count,
    expected_patients_count,
    expected_positive_cases_count,
):
    prediction_task = PredictionTask(
        target_type, disease_readmission, disease_selection, nb_days, use_icu=False
    )
    cohort_extractor = CohortExtractor(
        prediction_task=prediction_task,
    )
    df = cohort_extractor.extract().df
    assert len(df) == expected_admission_records_count

    assert df[CohortHeader.PATIENT_ID].nunique() == expected_patients_count
    assert (
        df[CohortHeader.HOSPITAL_ADMISSION_ID].nunique()
        == expected_admission_records_count
    )
    assert df[CohortHeader.LABEL].sum() == expected_positive_cases_count
