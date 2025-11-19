import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.rename(columns=lambda col: col.lower().strip().replace(" ", "_"))

    str_cols = df.select_dtypes(include=["object", "string"]).columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    if "name" in df.columns:
        df["name"] = df["name"].str.title()

    if "gender" in df.columns:
        df["gender"] = df["gender"].str.upper()

    if "blood_type" in df.columns:
        df["blood_type"] = df["blood_type"].str.upper()

    if "doctor" in df.columns:
        df["doctor"] = df["doctor"].str.title()

    if "hospital" in df.columns:
        df["hospital"] = df["hospital"].str.title()

    if "insurance_provider" in df.columns:
        df["insurance_provider"] = df["insurance_provider"].str.title()

    if "medical_condition" in df.columns:
        df["medical_condition"] = df["medical_condition"].str.title()

    if "test_results" in df.columns:
        df["test_results"] = df["test_results"].str.lower()

    if "admission_type" in df.columns:
        df["admission_type"] = df["admission_type"].str.lower()

    for col in ["age", "billing_amount", "room_number"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
            )

    if "billing_amount" in df.columns:
        df["billing_amount"] = df["billing_amount"].round(2)

    for col in ["date_of_admission", "discharge_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    people_df = (
        df[["name", "age", "gender", "blood_type"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    people_df["person_id"] = people_df.index + 1

    doctors_df = (
        df[["doctor", "hospital"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    doctors_df["doctor_id"] = doctors_df.index + 1

    hospitals_df = (
        doctors_df[["hospital"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"hospital": "hospital_name"})
    )
    hospitals_df["hospital_id"] = hospitals_df.index + 1

    doctors_df = doctors_df.merge(
        hospitals_df.rename(columns={"hospital_name": "hospital"}),
        on="hospital",
        how="left"
    )
    doctors_df = doctors_df.rename(
        columns={
            "doctor": "doctor_name",
            "hospital": "hospital_name"
        }
    )

    conditions_df = (
        df[["medical_condition"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"medical_condition": "condition_name"})
    )
    conditions_df["condition_id"] = conditions_df.index + 1

    insurance_df = (
        df[["insurance_provider"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"insurance_provider": "provider_name"})
    )
    insurance_df["insurance_id"] = insurance_df.index + 1

    test_results_df = (
        df[["test_results"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"test_results": "result_label"})
    )
    test_results_df["test_result_id"] = test_results_df.index + 1

    admission_types_df = (
        df[["admission_type"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"admission_type": "type_name"})
    )
    admission_types_df["admission_type_id"] = admission_types_df.index + 1

    df = df.merge(
        people_df,
        on=["name", "age", "gender", "blood_type"],
        how="left"
    )

    df = df.merge(
        doctors_df[["doctor_name", "hospital_name", "doctor_id"]],
        left_on=["doctor", "hospital"],
        right_on=["doctor_name", "hospital_name"],
        how="left"
    )

    df = df.merge(
        conditions_df,
        left_on="medical_condition",
        right_on="condition_name",
        how="left"
    )

    df = df.merge(
        insurance_df,
        left_on="insurance_provider",
        right_on="provider_name",
        how="left"
    )

    df = df.merge(
        test_results_df,
        left_on="test_results",
        right_on="result_label",
        how="left"
    )

    df = df.merge(
        admission_types_df,
        left_on="admission_type",
        right_on="type_name",
        how="left"
    )

    admissions_df = df[[
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
    ]].reset_index(drop=True)

    admissions_df["admission_id"] = admissions_df.index + 1

    admissions_df = admissions_df[[
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
    ]]

    return {"people": people_df, 
            "doctors": doctors_df, 
            "hospitals": hospitals_df, 
            "conditions": conditions_df, 
            "insurance": insurance_df, 
            "admission_types": admission_types_df,
            "test_results": test_results_df,
            "admissions": admissions_df
    }
    