import pandas as pd
from src.logger import get_logger

logger = get_logger(__name__)


def transform(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    logger.info(
        "Starting transform(): input df has %d rows x %d columns",
        df.shape[0],
        df.shape[1],
    )

    try:
        people_df = (
            df[["name", "age", "gender", "blood_type"]]
            .dropna(subset=["name", "age", "gender", "blood_type"])
            .drop_duplicates()
            .reset_index(drop=True)
        )
        people_df["person_id"] = people_df.index + 1
        logger.info(
            "Built people_df (no nulls): %d rows x %d columns",
            people_df.shape[0],
            people_df.shape[1],
        )

        doctors_df = (
            df[["doctor", "hospital"]]
            .dropna(subset=["doctor", "hospital"])
            .drop_duplicates()
            .reset_index(drop=True)
        )
        doctors_df["doctor_id"] = doctors_df.index + 1
        logger.info(
            "Built doctors_df (pre-hospital merge, no nulls): %d rows x %d columns",
            doctors_df.shape[0],
            doctors_df.shape[1],
        )

        hospitals_df = (
            doctors_df[["hospital"]]
            .dropna(subset=["hospital"])
            .drop_duplicates()
            .reset_index(drop=True)
            .rename(columns={"hospital": "hospital_name"})
        )
        hospitals_df["hospital_id"] = hospitals_df.index + 1
        logger.info(
            "Built hospitals_df (no nulls): %d rows x %d columns",
            hospitals_df.shape[0],
            hospitals_df.shape[1],
        )

        doctors_df = doctors_df.merge(
            hospitals_df.rename(columns={"hospital_name": "hospital"}),
            on="hospital",
            how="left",
        )
        doctors_df = doctors_df.rename(
            columns={
                "doctor": "doctor_name",
                "hospital": "hospital_name",
            }
        )
        logger.info(
            "Updated doctors_df after hospital merge: %d rows x %d columns",
            doctors_df.shape[0],
            doctors_df.shape[1],
        )

        conditions_df = (
            df[["medical_condition"]]
            .dropna(subset=["medical_condition"])
            .drop_duplicates()
            .reset_index(drop=True)
            .rename(columns={"medical_condition": "condition_name"})
        )
        conditions_df["condition_id"] = conditions_df.index + 1
        logger.info(
            "Built conditions_df (no nulls): %d rows x %d columns",
            conditions_df.shape[0],
            conditions_df.shape[1],
        )

        insurance_df = (
            df[["insurance_provider"]]
            .dropna(subset=["insurance_provider"])
            .drop_duplicates()
            .reset_index(drop=True)
            .rename(columns={"insurance_provider": "provider_name"})
        )
        insurance_df["insurance_id"] = insurance_df.index + 1
        logger.info(
            "Built insurance_df (no nulls): %d rows x %d columns",
            insurance_df.shape[0],
            insurance_df.shape[1],
        )

        valid_results = ["inconclusive", "normal", "abnormal"]

        test_results_df = (
            df[["test_results"]]
            .rename(columns={"test_results": "result_label"})
        )

        valid_mask = (
            test_results_df["result_label"].notna()
            & test_results_df["result_label"].isin(valid_results)
        )
        invalid_count = len(test_results_df) - valid_mask.sum()
        if invalid_count > 0:
            logger.warning(
                "test_results_df: %d invalid or null result_label value(s) "
                "dropped before dimension build",
                invalid_count,
            )

        test_results_df = (
            test_results_df[valid_mask]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        test_results_df["test_result_id"] = test_results_df.index + 1
        logger.info(
            "Built test_results_df (no nulls): %d rows x %d columns",
            test_results_df.shape[0],
            test_results_df.shape[1],
        )

        admission_types_df = (
            df[["admission_type"]]
            .dropna(subset=["admission_type"])
            .drop_duplicates()
            .reset_index(drop=True)
            .rename(columns={"admission_type": "type_name"})
        )
        admission_types_df["admission_type_id"] = admission_types_df.index + 1
        logger.info(
            "Built admission_types_df (no nulls): %d rows x %d columns",
            admission_types_df.shape[0],
            admission_types_df.shape[1],
        )

        logger.debug("Merging people into base df...")
        df = df.merge(
            people_df,
            on=["name", "age", "gender", "blood_type"],
            how="left",
        )

        logger.debug("Merging doctors into base df...")
        df = df.merge(
            doctors_df[["doctor_name", "hospital_name", "doctor_id"]],
            left_on=["doctor", "hospital"],
            right_on=["doctor_name", "hospital_name"],
            how="left",
        )

        logger.debug("Merging conditions into base df...")
        df = df.merge(
            conditions_df,
            left_on="medical_condition",
            right_on="condition_name",
            how="left",
        )

        logger.debug("Merging insurance into base df...")
        df = df.merge(
            insurance_df,
            left_on="insurance_provider",
            right_on="provider_name",
            how="left",
        )

        logger.debug("Merging test_results into base df...")
        df = df.merge(
            test_results_df,
            left_on="test_results",
            right_on="result_label",
            how="left",
        )

        logger.debug("Merging admission_types into base df...")
        df = df.merge(
            admission_types_df,
            left_on="admission_type",
            right_on="type_name",
            how="left",
        )

        logger.info(
            "Finished merges; df is now %d rows x %d columns",
            df.shape[0],
            df.shape[1],
        )

        admissions_required = df[
            [
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
        ].copy()

        required_cols = [
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

        missing_mask = admissions_required[required_cols].isna().any(axis=1)

        missing_columns_series = admissions_required[required_cols].isna().apply(
            lambda row: ",".join(row.index[row]), axis=1
        )

        rejects_df = df.loc[
            missing_mask,
            [
                "name",
                "age",
                "gender",
                "blood_type",
                "medical_condition",
                "date_of_admission",
                "doctor",
                "hospital",
                "insurance_provider",
                "billing_amount",
                "room_number",
                "admission_type",
                "discharge_date",
                "medication",
                "test_results",
            ],
        ].copy()

        rejects_df["missing_columns"] = missing_columns_series[missing_mask].values

        admissions_df = admissions_required[~missing_mask].reset_index(drop=True)
        admissions_df["admission_id"] = admissions_df.index + 1

        admissions_df = admissions_df[
            [
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
        ]

        logger.info(
            "Built admissions_df: %d rows x %d columns",
            admissions_df.shape[0],
            admissions_df.shape[1],
        )

        logger.info(
            "After reject split: %d valid admissions, %d rejected rows",
            admissions_df.shape[0],
            rejects_df.shape[0],
        )

        result = {
            "people": people_df,
            "doctors": doctors_df,
            "hospitals": hospitals_df,
            "conditions": conditions_df,
            "insurance": insurance_df,
            "admission_types": admission_types_df,
            "test_results": test_results_df,
            "admissions": admissions_df,
            "rejects": rejects_df,
        }

        logger.info(
            "transform() completed successfully. Output tables: %s",
            {k: v.shape for k, v in result.items()},
        )

        return result

    except Exception:
        logger.exception("transform() failed.")
        raise
