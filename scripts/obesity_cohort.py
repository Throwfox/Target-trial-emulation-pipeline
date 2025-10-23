import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import duckdb


LOGGER = logging.getLogger("obesity_cohort")


def _resolve_path(base: Path, relative_or_absolute: str) -> str:
    """Return an absolute POSIX path for DuckDB."""
    candidate = Path(relative_or_absolute)
    if not candidate.is_absolute():
        candidate = base / candidate
    return candidate.as_posix()


def _create_view(con: duckdb.DuckDBPyConnection, name: str, path: str, sample_size: int) -> None:
    sql = (
        "CREATE OR REPLACE VIEW {name} AS\n"
        "SELECT * FROM read_csv_auto('{path}', SAMPLE_SIZE={sample_size}, IGNORE_ERRORS=TRUE)"
    ).format(name=name, path=path.replace("'", "''"), sample_size=sample_size)
    con.execute(sql)


def _create_concept_set(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    concept_cfg: Dict[str, Any],
) -> None:
    concept_ids: List[int] = [int(cid) for cid in concept_cfg.get("concept_ids", [])]
    include_descendants: bool = concept_cfg.get("include_descendants", True)

    if not concept_ids:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {table_name} AS SELECT CAST(NULL AS BIGINT) AS concept_id WHERE 1=0")
        return

    values_clause = ", ".join(f"({cid})" for cid in concept_ids)

    if include_descendants:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {table_name} AS
            SELECT DISTINCT descendant_concept_id AS concept_id
            FROM concept_ancestor
            WHERE ancestor_concept_id IN ({', '.join(str(cid) for cid in concept_ids)})
            UNION
            SELECT DISTINCT concept_id
            FROM (VALUES {values_clause}) AS v(concept_id)
            """
        )
    else:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {table_name} AS
            SELECT concept_id
            FROM (VALUES {values_clause}) AS v(concept_id)
            """
        )


def build_obesity_cohort(cfg: Dict[str, Any]) -> None:
    base_path = Path(cfg["cdm_path"])
    output_path = Path(cfg["output"]["cohort_csv"])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    log_level = cfg.get("log_level", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(asctime)s [%(levelname)s] %(message)s")

    sample_size = int(cfg.get("duckdb_sample_size", 200000))

    con = duckdb.connect()

    tables = cfg.get("tables", {})
    _create_view(con, "concept", _resolve_path(base_path, tables.get("concept", "r6287_concept.csv")), sample_size)
    _create_view(
        con,
        "concept_ancestor",
        _resolve_path(base_path, tables.get("concept_ancestor", "r6287_concept_ancestor.csv")),
        sample_size,
    )
    _create_view(con, "person", _resolve_path(base_path, tables.get("person", "r6287_person.csv")), sample_size)
    _create_view(
        con,
        "observation_period",
        _resolve_path(base_path, tables.get("observation_period", "r6287_observation_period.csv")),
        sample_size,
    )
    _create_view(
        con,
        "condition_occurrence",
        _resolve_path(base_path, tables.get("condition_occurrence", "r6287_condition_occurrence.csv")),
        sample_size,
    )
    _create_view(
        con,
        "measurement",
        _resolve_path(base_path, tables.get("measurement", "r6287_measurement_half*.csv")),
        sample_size,
    )
    _create_view(
        con,
        "visit_occurrence",
        _resolve_path(base_path, tables.get("visit_occurrence", "r6287_visit_occurrence.csv")),
        sample_size,
    )
    _create_view(con, "death", _resolve_path(base_path, tables.get("death", "r6287_death.csv")), sample_size)

    concept_sets = cfg.get("concept_sets", {})
    _create_concept_set(con, "obesity_condition_concepts", concept_sets["obesity_conditions"])
    _create_concept_set(con, "bmi_concepts", concept_sets["bmi_measurements"])
    _create_concept_set(con, "comorbidity_concepts", concept_sets["weight_related_comorbidities"])

    debug_cfg = cfg.get("debug", {})

    study = cfg.get("study", {})
    min_age = int(study.get("min_age", 18))
    study_start = study.get("start_date", "2012-01-01")
    study_end = study.get("end_date", "2024-01-31")
    obesity_min_date = study.get("obesity_min_index_date", "2014-01-01")

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE obesity_condition AS
        SELECT
            person_id,
            MIN(CAST(COALESCE(CONDITION_START_DATE, CONDITION_START_DATETIME) AS DATE)) AS first_obesity_dx
        FROM condition_occurrence
        WHERE condition_concept_id IN (SELECT concept_id FROM obesity_condition_concepts)
        GROUP BY person_id
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE bmi_events AS
        SELECT
            PERSON_ID AS person_id,
            CAST(COALESCE(MEASUREMENT_DATE, MEASUREMENT_DATETIME) AS DATE) AS measurement_date,
            TRY_CAST(VALUE_AS_NUMBER AS DOUBLE) AS bmi
        FROM measurement
        WHERE measurement_concept_id IN (SELECT concept_id FROM bmi_concepts)
          AND VALUE_AS_NUMBER IS NOT NULL
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE bmi_ge_30 AS
        SELECT person_id, MIN(measurement_date) AS first_bmi_ge_30
        FROM bmi_events
        WHERE bmi >= 30
        GROUP BY person_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE comorbidity_events AS
        SELECT
            PERSON_ID AS person_id,
            CAST(COALESCE(CONDITION_START_DATE, CONDITION_START_DATETIME) AS DATE) AS condition_date
        FROM condition_occurrence
        WHERE condition_concept_id IN (SELECT concept_id FROM comorbidity_concepts)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE bmi_27_29 AS
        SELECT b.person_id, b.measurement_date
        FROM bmi_events b
        WHERE b.bmi >= 27 AND b.bmi < 30
          AND EXISTS (
                SELECT 1
                FROM comorbidity_events c
                WHERE c.person_id = b.person_id
                  AND c.condition_date <= b.measurement_date
          )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE bmi_27_29_first AS
        SELECT person_id, MIN(measurement_date) AS first_bmi_27_29
        FROM bmi_27_29
        GROUP BY person_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE index_candidates AS
        SELECT person_id, first_obesity_dx AS index_date, 'condition_dx' AS index_source, 1 AS source_rank
        FROM obesity_condition
        WHERE first_obesity_dx IS NOT NULL
        UNION ALL
        SELECT person_id, first_bmi_ge_30, 'bmi_ge_30' AS index_source, 2 AS source_rank
        FROM bmi_ge_30
        WHERE first_bmi_ge_30 IS NOT NULL
        UNION ALL
        SELECT person_id, first_bmi_27_29, 'bmi_ge_27_plus_comorb' AS index_source, 3 AS source_rank
        FROM bmi_27_29_first
        WHERE first_bmi_27_29 IS NOT NULL
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE index_choice AS
        SELECT person_id, index_date, index_source
        FROM (
            SELECT
                person_id,
                index_date,
                index_source,
                ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY index_date, source_rank) AS rn
            FROM index_candidates
        )
        WHERE rn = 1
        """
    )

    max_persons = debug_cfg.get("max_persons")
    if max_persons:
        limit_val = int(max_persons)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE index_choice AS
            SELECT person_id, index_date, index_source
            FROM (
                SELECT ic.*, ROW_NUMBER() OVER (ORDER BY person_id, index_date, index_source) AS rn
                FROM index_choice ic
            )
            WHERE rn <= {limit_val}
            """
        )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE person_core AS
        SELECT
            PERSON_ID AS person_id,
            COALESCE(
                CAST(BIRTH_DATETIME AS DATE),
                make_date(
                    YEAR_OF_BIRTH,
                    COALESCE(NULLIF(MONTH_OF_BIRTH, 0), 1),
                    COALESCE(NULLIF(DAY_OF_BIRTH, 0), 1)
                )
            ) AS birth_date,
            GENDER_CONCEPT_ID AS gender_concept_id,
            RACE_CONCEPT_ID AS race_concept_id,
            ETHNICITY_CONCEPT_ID AS ethnicity_concept_id
        FROM person
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE visits_before_index AS
        SELECT DISTINCT ic.person_id
        FROM index_choice ic
        JOIN visit_occurrence vo
          ON ic.person_id = vo.PERSON_ID
        WHERE CAST(COALESCE(vo.VISIT_START_DATE, vo.VISIT_START_DATETIME) AS DATE) < ic.index_date
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE observation_coverage AS
        SELECT DISTINCT ic.person_id
        FROM index_choice ic
        JOIN observation_period op
          ON ic.person_id = op.PERSON_ID
        WHERE ic.index_date BETWEEN CAST(op.OBSERVATION_PERIOD_START_DATE AS DATE)
                                AND CAST(op.OBSERVATION_PERIOD_END_DATE AS DATE)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE death_dates AS
        SELECT PERSON_ID AS person_id,
               MIN(CAST(DEATH_DATE AS DATE)) AS death_date
        FROM death
        WHERE DEATH_DATE IS NOT NULL
        GROUP BY PERSON_ID
        """
    )

    cohort_sql = f"""
        SELECT
            ic.person_id,
            ic.index_date,
            ic.index_source,
            CASE WHEN oc.person_id IS NOT NULL THEN 1 ELSE 0 END AS obesity_dx_flag,
            CASE WHEN bg.person_id IS NOT NULL THEN 1 ELSE 0 END AS bmi_ge_30_flag,
            CASE WHEN b27.person_id IS NOT NULL THEN 1 ELSE 0 END AS bmi_ge_27_plus_comorb_flag,
            pc.birth_date,
            CAST(date_diff('day', pc.birth_date, ic.index_date) / 365.25 AS INTEGER) AS age_at_index,
            pc.gender_concept_id,
            pc.race_concept_id,
            pc.ethnicity_concept_id,
            dd.death_date
        FROM index_choice ic
        JOIN person_core pc ON pc.person_id = ic.person_id
        LEFT JOIN obesity_condition oc ON oc.person_id = ic.person_id AND oc.first_obesity_dx = ic.index_date
        LEFT JOIN bmi_ge_30 bg ON bg.person_id = ic.person_id AND bg.first_bmi_ge_30 = ic.index_date
        LEFT JOIN bmi_27_29_first b27 ON b27.person_id = ic.person_id AND b27.first_bmi_27_29 = ic.index_date
        LEFT JOIN death_dates dd ON dd.person_id = ic.person_id
        WHERE ic.index_date BETWEEN DATE '{study_start}' AND DATE '{study_end}'
          AND ic.index_date >= DATE '{obesity_min_date}'
          AND CAST(date_diff('day', pc.birth_date, ic.index_date) / 365.25 AS INTEGER) >= {min_age}
          AND pc.birth_date IS NOT NULL
          AND ic.index_date >= pc.birth_date
          AND (dd.death_date IS NULL OR ic.index_date <= dd.death_date)
          AND ic.person_id IN (SELECT person_id FROM visits_before_index)
          AND ic.person_id IN (SELECT person_id FROM observation_coverage)
    """

    LOGGER.info("Writing cohort to %s", output_path)
    con.execute(f"COPY ({cohort_sql}) TO '{output_path.as_posix()}' WITH (HEADER, DELIMITER ',')")

    stats = con.execute("SELECT COUNT(DISTINCT person_id) FROM index_choice").fetchone()[0]
    LOGGER.info("Total patients with obesity evidence prior to filtering: %s", stats)
    final_stats = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{output_path.as_posix()}')").fetchone()[0]
    LOGGER.info("Patients retained after study filters: %s", final_stats)

    con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract adult obesity cohort from OMOP CDM")
    parser.add_argument("--config", "-c", required=True, help="Path to obesity cohort configuration JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    build_obesity_cohort(cfg)


if __name__ == "__main__":
    main()
