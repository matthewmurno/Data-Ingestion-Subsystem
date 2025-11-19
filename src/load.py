import pandas as pd
import psycopg2

from logger import get_logger

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

    logger.info("Starting load()")
    logger.info(
        "Row counts - people=%d, hospitals=%d, doctors=%d, conditions=%d, "
        "insurance=%d, test_results=%d, admission_types=%d, admissions=%d",
        len(people_df),
        len(hospitals_df),
        len(doctors_df),
        len(conditions_df),
        len(insurance_df),
        len(test_results_df),
        len(admission_types_df),
        len(admissions_df),
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
        conn.commit()
        logger.info("Tables created/verified successfully.")

        logger.info("Truncating tables and resetting identities...")
        cur.execute("""
            TRUNCATE admission_data,
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

        logger.info("Inserting into people...")
        for row in people_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO people (person_id, name, age, gender, blood_type)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (row.person_id, row.name, row.age, row.gender, row.blood_type)
            )

        logger.info("Inserting into hospitals...")
        for row in hospitals_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO hospitals (hospital_id, hospital_name)
                VALUES (%s, %s);
                """,
                (row.hospital_id, row.hospital_name)
            )

        logger.info("Inserting into doctors...")
        for row in doctors_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO doctors (doctor_id, doctor_name, hospital_id)
                VALUES (%s, %s, %s);
                """,
                (row.doctor_id, row.doctor_name, row.hospital_id)
            )

        logger.info("Inserting into conditions...")
        for row in conditions_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO conditions (condition_id, condition_name)
                VALUES (%s, %s);
                """,
                (row.condition_id, row.condition_name)
            )

        logger.info("Inserting into insurance...")
        for row in insurance_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO insurance (insurance_id, provider_name)
                VALUES (%s, %s);
                """,
                (row.insurance_id, row.provider_name)
            )

        logger.info("Inserting into test_results...")
        for row in test_results_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO test_results (test_result_id, result_label)
                VALUES (%s, %s);
                """,
                (row.test_result_id, row.result_label)
            )

        logger.info("Inserting into admission_types...")
        for row in admission_types_df.itertuples(index=False):
            cur.execute(
                """
                INSERT INTO admission_types (admission_type_id, type_name)
                VALUES (%s, %s);
                """,
                (row.admission_type_id, row.type_name)
            )

        logger.info("Inserting into admission_data...")
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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