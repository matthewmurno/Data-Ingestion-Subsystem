import pandas as pd
import pytest

from src.transform import transform


def _make_base_df():
    """Small cleaned dataframe with normalized column names."""
    return pd.DataFrame(
        {
            "name": ["John Doe", "Jane Smith", "John Doe"],
            "age": [30, 40, 30],
            "gender": ["M", "F", "M"],
            "blood_type": ["O+", "A-", "O+"],
            "medical_condition": ["Flu", "Cold", "Flu"],
            "date_of_admission": [
                "2024-01-01",
                "2024-01-02",
                "2024-01-03",
            ],
            "doctor": ["Dr. House", "Dr. Wilson", "Dr. House"],
            "hospital": ["General Hospital", "City Clinic", "General Hospital"],
            "insurance_provider": ["Acme Health", "Acme Health", "Acme Health"],
            "billing_amount": [1000.0, 2000.0, None],
            "room_number": [101, 202, None],
            "admission_type": ["Emergency", "Planned", "Emergency"],
            "discharge_date": [
                "2024-01-05",
                "2024-01-07",
                None,
            ],
            "medication": ["Med A", "Med B", None],
            "test_results": ["normal", "abnormal", "unknown"],
        }
    )


def test_transform_builds_dimensions_and_valid_admissions():
    """
    Happy-path: valid rows should produce dimension tables and
    a non-empty admissions table with no rejects for those rows.
    """
    df = _make_base_df().iloc[:2].copy()

    result = transform(df)

    expected_keys = {
        "people",
        "doctors",
        "hospitals",
        "conditions",
        "insurance",
        "admission_types",
        "test_results",
        "admissions",
        "rejects",
    }
    assert set(result.keys()) == expected_keys

    people_df = result["people"]
    doctors_df = result["doctors"]
    hospitals_df = result["hospitals"]
    conditions_df = result["conditions"]
    insurance_df = result["insurance"]
    admission_types_df = result["admission_types"]
    test_results_df = result["test_results"]
    admissions_df = result["admissions"]
    rejects_df = result["rejects"]

    assert len(people_df) == 2  
    assert len(doctors_df) == 2 
    assert len(hospitals_df) == 2
    assert len(conditions_df) == 2
    assert len(insurance_df) == 1 
    assert len(admission_types_df) == 2 
    assert len(test_results_df) == 2

    assert people_df["person_id"].tolist() == [1, 2]
    assert doctors_df["doctor_id"].tolist() == [1, 2]
    assert hospitals_df["hospital_id"].tolist() == [1, 2]
    assert conditions_df["condition_id"].tolist() == [1, 2]
    assert insurance_df["insurance_id"].tolist() == [1]
    assert admission_types_df["admission_type_id"].tolist() == [1, 2]
    assert test_results_df["test_result_id"].tolist() == [1, 2]

    assert len(admissions_df) == 2
    assert len(rejects_df) == 0

    required_cols = [
        "admission_id",
        "person_id",
        "doctor_id",
        "condition_id",
        "insurance_id",
        "admission_type_id",
        "test_result_id",
        "date_of_admission",
        "discharge_date",
        "billing_amount",
        "room_number",
        "medication",
    ]
    assert not admissions_df[required_cols].isna().any().any()


def test_transform_routes_invalid_rows_to_rejects_and_sets_missing_columns():
    """
    Rows with missing required fields or invalid test_results
    should go to rejects with a populated missing_columns string,
    and invalid test_results should not appear in test_results_df.
    """
    df = _make_base_df()

    result = transform(df)

    test_results_df = result["test_results"]
    admissions_df = result["admissions"]
    rejects_df = result["rejects"]

    assert set(test_results_df["result_label"]) == {"normal", "abnormal"}
    assert "unknown" not in test_results_df["result_label"].tolist()

    assert len(admissions_df) == 2

    assert len(rejects_df) == 1

    reject_row = rejects_df.iloc[0]

    assert reject_row["name"] == "John Doe"
    assert reject_row["test_results"] == "unknown"

    missing_cols = set(reject_row["missing_columns"].split(","))
    assert "test_result_id" in missing_cols
    assert "billing_amount" in missing_cols
    assert "room_number" in missing_cols
    assert "discharge_date" in missing_cols
    assert "medication" in missing_cols


def test_transform_raises_when_required_columns_missing():
    """
    If a required column is missing, transform should raise.
    This also exercises the except-block in transform().
    """
    bad_df = pd.DataFrame(
        {
            "age": [30],
            "gender": ["M"],
            "blood_type": ["O+"],
            "medical_condition": ["Flu"],
            "date_of_admission": ["2024-01-01"],
            "doctor": ["Dr. House"],
            "hospital": ["General Hospital"],
            "insurance_provider": ["Acme Health"],
            "billing_amount": [1000.0],
            "room_number": [101],
            "admission_type": ["Emergency"],
            "discharge_date": ["2024-01-05"],
            "medication": ["Med A"],
            "test_results": ["normal"],
        }
    )

    with pytest.raises(KeyError):
        transform(bad_df)
