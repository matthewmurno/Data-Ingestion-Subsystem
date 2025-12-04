import pandas as pd
import psycopg2
from src.config import CONFIG, get_source_config

from src.logger import get_logger

logger = get_logger(__name__)

def load(loaded_data, db_url):
    people_df = loaded_data["people"]
    hospitals_df = loaded_data["hospitals"]
    doctors_df = loaded_data["doctors"]
    conditions_df = loaded_data["conditions"]
    insurance_df = loaded_data["insurance"]
    test_results_df = loaded_data["test_results"]
    admission_types_df = loaded_data["admission_types"]
    admissions_df = loaded_data["admissions"]
    rejects_df = loaded_data["rejects"]

    logger.info("Starting load()")
    logger.info(
        "Row counts - people=%d, hospitals=%d, doctors=%d, conditions=%d, "
        "insurance=%d, test_results=%d, admission_types=%d, admissions=%d, rejects=%d",
        len(people_df),
        len(hospitals_df),
        len(doctors_df),
        len(conditions_df),
        len(insurance_df),
        len(test_results_df),
        len(admission_types_df),
        len(admissions_df),
        len(rejects_df),
    )

    try:
        logger.info("Connecting to database...")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        logger.info("Database connection established.")

        logger.info("Creating tables if they do not exist...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS people (
                person_id   SERIAL PRIMARY KEY,
                name        TEXT NOT NULL,
                age         INT,
                gender      TEXT NOT NULL,
                blood_type  TEXT NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS hospitals (
                hospital_id   INT PRIMARY KEY,
                hospital_name TEXT NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS doctors (
                doctor_id     INT PRIMARY KEY,
                doctor_name   TEXT NOT NULL,
                hospital_id   INT NOT NULL REFERENCES hospitals(hospital_id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS conditions (
                condition_id   INT PRIMARY KEY,
                condition_name TEXT NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS insurance (
                insurance_id  INT PRIMARY KEY,
                provider_name TEXT NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_results (
                test_result_id INT PRIMARY KEY,
                result_label   TEXT NOT NULL CHECK (result_label IN ('inconclusive', 'normal', 'abnormal'))
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS admission_types (
                admission_type_id INT PRIMARY KEY,
                type_name         TEXT NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS admission_data (
                admission_id       INT PRIMARY KEY,
                person_id          INT NOT NULL REFERENCES people(person_id),
                doctor_id          INT NOT NULL REFERENCES doctors(doctor_id),
                condition_id       INT NOT NULL REFERENCES conditions(condition_id),
                insurance_id       INT NOT NULL REFERENCES insurance(insurance_id),
                admission_type_id  INT NOT NULL REFERENCES admission_types(admission_type_id),
                test_result_id     INT NOT NULL REFERENCES test_results(test_result_id),
                date_of_admission  TIMESTAMP,
                discharge_date     TIMESTAMP,
                billing_amount     NUMERIC(12, 2),
                room_number        INT,
                medication         TEXT NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS rejects (
                reject_id          SERIAL PRIMARY KEY,
                name               TEXT,
                age                TEXT,
                gender             TEXT,
                blood_type         TEXT,
                medical_condition  TEXT,
                date_of_admission  TEXT,
                doctor             TEXT,
                hospital           TEXT,
                insurance_provider TEXT,
                billing_amount     TEXT,
                room_number        TEXT,
                admission_type     TEXT,
                discharge_date     TEXT,
                medication         TEXT,
                test_results       TEXT,
                missing_columns    TEXT NOT NULL
            );
        """)


        conn.commit()
        logger.info("Tables created/verified successfully.")

        logger.info("Truncating tables and resetting identities...")
        cur.execute("""
            TRUNCATE rejects,
                     admission_data,
                     doctors,
                     hospitals,
                     conditions,
                     insurance,
                     admission_types,
                     test_results,
                     people
            RESTART IDENTITY;
        """)
        conn.commit()
        logger.info("Tables truncated.")

        logger.info("Starting insertion.")
        logger.debug("Inserting into people...")
        for row in people_df.itertuples(index=False):
            try:
                age = row.age

                if pd.isna(age):
                    age = None
                else:
                    age = int(age)
                    if age < 0 or age > 120:
                        logger.warning(
                            "Invalid age %s for person %s; setting age to NULL before insert",
                            age,
                            row.name,
                        )
                        age = None

                person_id = int(row.person_id)

                cur.execute(
                    """
                    INSERT INTO people (person_id, name, age, gender, blood_type)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (person_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        age = EXCLUDED.age,
                        gender = EXCLUDED.gender,
                        blood_type = EXCLUDED.blood_type;
                    """,
                    (person_id, row.name, age, row.gender, row.blood_type),
                )

            except psycopg2.Error:
                logger.exception("Failed inserting people row: %s", row)
                raise

        logger.debug("Inserting into hospitals...")
        for row in hospitals_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO hospitals (hospital_id, hospital_name)
                VALUES (%s, %s)
                ON CONFLICT (hospital_id) DO UPDATE
                SET hospital_name = EXCLUDED.hospital_name;
                """,
                (row.hospital_id, row.hospital_name)
            )

        logger.debug("Inserting into doctors...")
        for row in doctors_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO doctors (doctor_id, doctor_name, hospital_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (doctor_id) DO UPDATE
                SET doctor_name = EXCLUDED.doctor_name,
                    hospital_id = EXCLUDED.hospital_id;
                """,
                (row.doctor_id, row.doctor_name, row.hospital_id)
            )

        logger.debug("Inserting into conditions...")
        for row in conditions_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO conditions (condition_id, condition_name)
                VALUES (%s, %s)
                ON CONFLICT (condition_id) DO UPDATE
                SET condition_name = EXCLUDED.condition_name;
                """,
                (row.condition_id, row.condition_name)
            )

        logger.debug("Inserting into insurance...")
        for row in insurance_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO insurance (insurance_id, provider_name)
                VALUES (%s, %s)
                ON CONFLICT (insurance_id) DO UPDATE
                SET provider_name = EXCLUDED.provider_name;
                """,
                (row.insurance_id, row.provider_name)
            )

        logger.debug("Inserting into test_results...")
        for row in test_results_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO test_results (test_result_id, result_label)
                VALUES (%s, %s)
                ON CONFLICT (test_result_id) DO UPDATE
                SET result_label = EXCLUDED.result_label;
                """,
                (row.test_result_id, row.result_label)
            )

        logger.debug("Inserting into admission_types...")
        for row in admission_types_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO admission_types (admission_type_id, type_name)
                VALUES (%s, %s)
                ON CONFLICT (admission_type_id) DO UPDATE
                SET type_name = EXCLUDED.type_name;
                """,
                (row.admission_type_id, row.type_name)
            )

        logger.debug("Inserting into admission_data...")
        for row in admissions_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO admission_data (
                    admission_id,
                    person_id,
                    doctor_id,
                    condition_id,
                    insurance_id,
                    admission_type_id,
                    test_result_id,
                    date_of_admission,
                    discharge_date,
                    billing_amount,
                    room_number,
                    medication
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (admission_id) DO UPDATE
                SET person_id         = EXCLUDED.person_id,
                    doctor_id         = EXCLUDED.doctor_id,
                    condition_id      = EXCLUDED.condition_id,
                    insurance_id      = EXCLUDED.insurance_id,
                    admission_type_id = EXCLUDED.admission_type_id,
                    test_result_id    = EXCLUDED.test_result_id,
                    date_of_admission = EXCLUDED.date_of_admission,
                    discharge_date    = EXCLUDED.discharge_date,
                    billing_amount    = EXCLUDED.billing_amount,
                    room_number       = EXCLUDED.room_number,
                    medication        = EXCLUDED.medication;
                """,
                (
                    row.admission_id,
                    row.person_id,
                    row.doctor_id,
                    row.condition_id,
                    row.insurance_id,
                    row.admission_type_id,
                    row.test_result_id,
                    row.date_of_admission,
                    row.discharge_date,
                    row.billing_amount,
                    row.room_number,
                    row.medication,
                )
            )

        logger.debug("Inserting into rejects...")
        for row in rejects_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO rejects (
                    name,
                    age,
                    gender,
                    blood_type,
                    medical_condition,
                    date_of_admission,
                    doctor,
                    hospital,
                    insurance_provider,
                    billing_amount,
                    room_number,
                    admission_type,
                    discharge_date,
                    medication,
                    test_results,
                    missing_columns
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    row.name,
                    None if pd.isna(row.age) else str(row.age),
                    row.gender,
                    row.blood_type,
                    row.medical_condition,
                    None if pd.isna(row.date_of_admission) else str(row.date_of_admission),
                    row.doctor,
                    row.hospital,
                    row.insurance_provider,
                    None if pd.isna(row.billing_amount) else str(row.billing_amount),
                    None if pd.isna(row.room_number) else str(row.room_number),
                    row.admission_type,
                    None if pd.isna(row.discharge_date) else str(row.discharge_date),
                    row.medication,
                    row.test_results,
                    row.missing_columns,
                ),
            )

        conn.commit()
        logger.info("Load completed successfully.")

    except psycopg2.Error:
        logger.exception("Error in load() while working with the database.")
        if 'conn' in locals():
            conn.rollback()
            logger.info("Transaction rolled back due to error.")

    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        logger.info("Database connection closed. End of load().")