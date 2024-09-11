from pipeline.extract.raw.hosp import load_patients


def test_load_patients():
    patients = load_patients()

    assert len(patients) == 100
