import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import duckdb

LOGGER = logging.getLogger("glp1_user_split")


def _resolve_path(base: Path, relative_or_absolute: str) -> str:
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
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table_name} AS SELECT CAST(NULL AS BIGINT) AS concept_id WHERE 1=0"
        )
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


def label_glp1_exposure(cfg: Dict[str, Any]) -> None:
    base_path = Path(cfg["cdm_path"])
    obesity_cohort_path = Path(cfg["input"]["obesity_cohort"])
    output_cfg = cfg["output"]

    for key in ("user_csv", "nonuser_csv"):
        Path(output_cfg[key]).parent.mkdir(parents=True, exist_ok=True)

    log_level = cfg.get("log_level", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(asctime)s [%(levelname)s] %(message)s")

    sample_size = int(cfg.get("duckdb_sample_size", 200000))
    debug_cfg = cfg.get("debug", {})

    con = duckdb.connect()

    tables = cfg.get("tables", {})
    _create_view(con, "concept", _resolve_path(base_path, tables.get("concept", "r6287_concept.csv")), sample_size)
    _create_view(
        con,
        "concept_ancestor",
        _resolve_path(base_path, tables.get("concept_ancestor", "r6287_concept_ancestor.csv")),
        sample_size,
    )
    _create_view(
        con,
        "drug_exposure",
        _resolve_path(base_path, tables.get("drug_exposure", "r6287_drug_exposure.csv")),
        sample_size,
    )
    _create_view(
        con,
        "observation_period",
        _resolve_path(base_path, tables.get("observation_period", "r6287_observation_period.csv")),
        sample_size,
    )

    _create_view(
        con,
        "obesity_cohort",
        _resolve_path(obesity_cohort_path.parent, obesity_cohort_path.name),
        sample_size,
    )

    concept_sets = cfg.get("concept_sets", {})
    _create_concept_set(con, "glp1_concepts", concept_sets["glp1_ingredients"])

    exposure_cfg = cfg.get("exposure", {})
    min_exposure_date = exposure_cfg.get("min_date", cfg.get("study", {}).get("start_date", "2012-01-01"))
    max_exposure_date = exposure_cfg.get("max_date", cfg.get("study", {}).get("end_date", "2024-01-31"))
    min_days_from_index = int(exposure_cfg.get("min_days_from_obesity_index", 0))
    max_days_from_index = int(exposure_cfg.get("max_days_from_obesity_index", 730))

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE cohort_base AS
        SELECT
            person_id,
            CAST(index_date AS DATE) AS obesity_index_date,
            index_source,
            TRY_CAST(birth_date AS DATE) AS birth_date,
            age_at_index,
            gender_concept_id,
            race_concept_id,
            ethnicity_concept_id,
            TRY_CAST(death_date AS DATE) AS death_date
        FROM obesity_cohort
        """
    )

    max_persons = debug_cfg.get("max_persons")
    if max_persons:
        limit_val = int(max_persons)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE cohort_base AS
            SELECT *
            FROM (
                SELECT cb.*, ROW_NUMBER() OVER (ORDER BY person_id) AS rn
                FROM cohort_base cb
            )
            WHERE rn <= {limit_val}
            """
        )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE glp1_exposure_events AS
        SELECT
            PERSON_ID AS person_id,
            CAST(COALESCE(DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_START_DATETIME) AS DATE) AS exposure_date,
            DRUG_EXPOSURE_ID AS exposure_id,
            DRUG_CONCEPT_ID AS concept_id
        FROM drug_exposure
        WHERE DRUG_CONCEPT_ID IN (SELECT concept_id FROM glp1_concepts)
          AND CAST(COALESCE(DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_START_DATETIME) AS DATE) BETWEEN DATE '{min_exposure_date}' AND DATE '{max_exposure_date}'
          AND PERSON_ID IN (SELECT person_id FROM cohort_base)
          AND CAST(COALESCE(DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_START_DATETIME) AS DATE) IS NOT NULL
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE first_glp1 AS
        SELECT person_id, exposure_date, concept_id
        FROM (
            SELECT
                person_id,
                exposure_date,
                concept_id,
                ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY exposure_date, exposure_id) AS rn
            FROM glp1_exposure_events
        )
        WHERE rn = 1
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE observation_spans AS
        SELECT
            PERSON_ID AS person_id,
            CAST(OBSERVATION_PERIOD_START_DATE AS DATE) AS obs_start,
            CAST(OBSERVATION_PERIOD_END_DATE AS DATE) AS obs_end
        FROM observation_period
        WHERE PERSON_ID IN (SELECT person_id FROM cohort_base)
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE user_candidates AS
        SELECT
            cb.person_id,
            fg.exposure_date AS glp1_index_date,
            fg.concept_id AS glp1_concept_id,
            date_diff('day', cb.obesity_index_date, fg.exposure_date) AS days_from_obesity_index
        FROM cohort_base cb
        JOIN first_glp1 fg ON fg.person_id = cb.person_id
        WHERE date_diff('day', cb.obesity_index_date, fg.exposure_date) BETWEEN {min_days_from_index} AND {max_days_from_index}
          AND EXISTS (
                SELECT 1
                FROM observation_spans os
                WHERE os.person_id = cb.person_id
                  AND fg.exposure_date BETWEEN os.obs_start AND os.obs_end
            )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE glp1_users AS
        SELECT
            cb.person_id,
            uc.glp1_index_date,
            uc.glp1_concept_id,
            uc.days_from_obesity_index,
            cb.obesity_index_date,
            cb.index_source,
            cb.birth_date,
            CAST(date_diff('day', cb.birth_date, uc.glp1_index_date) / 365.25 AS INTEGER) AS age_at_glp1_index,
            cb.gender_concept_id,
            cb.race_concept_id,
            cb.ethnicity_concept_id,
            cb.death_date
        FROM cohort_base cb
        JOIN user_candidates uc ON uc.person_id = cb.person_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE glp1_non_users AS
        SELECT
            cb.*
        FROM cohort_base cb
        LEFT JOIN user_candidates uc ON uc.person_id = cb.person_id
        WHERE uc.person_id IS NULL
        """
    )

    user_sql = "SELECT * FROM glp1_users"
    nonuser_sql = "SELECT * FROM glp1_non_users"

    LOGGER.info("Writing GLP-1 users to %s", output_cfg["user_csv"])
    con.execute(
        f"COPY ({user_sql}) TO '{Path(output_cfg['user_csv']).as_posix()}' WITH (HEADER, DELIMITER ',')"
    )

    LOGGER.info("Writing GLP-1 non-users to %s", output_cfg["nonuser_csv"])
    con.execute(
        f"COPY ({nonuser_sql}) TO '{Path(output_cfg['nonuser_csv']).as_posix()}' WITH (HEADER, DELIMITER ',')"
    )

    user_count = con.execute("SELECT COUNT(*) FROM glp1_users").fetchone()[0]
    nonuser_count = con.execute("SELECT COUNT(*) FROM glp1_non_users").fetchone()[0]
    LOGGER.info("Identified %s GLP-1 users and %s non-users", user_count, nonuser_count)

    con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Identify GLP-1 users vs non-users within the obesity cohort")
    parser.add_argument("--config", "-c", required=True, help="Path to GLP-1 exposure configuration JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    label_glp1_exposure(cfg)


if __name__ == "__main__":
    main()
