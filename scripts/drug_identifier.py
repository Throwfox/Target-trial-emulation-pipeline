"""
Drug Identifier Script - 识别多种药物的使用情况

功能：
1. 根据 ingredient concept IDs 查找所有相关的药物概念（包括 descendants）
2. 保存每种药物的完整 concept ID sets 到单独的 CSV 文件
3. 在 drug_exposure 表中搜索用药记录
4. 生成患者用药汇总表，每种药物包含：
   - {药物名}_used: 是否使用过该药物 (0/1)
   - {药物名}_first_date: 首次使用日期

使用方法：
    python drug_identifier.py

输出文件：
1. 患者用药汇总表 (OUTPUT_CSV):
    person_id,semaglutide_used,semaglutide_first_date,liraglutide_used,liraglutide_first_date,...
    12345,1,2020-03-15,0,,
    67890,0,,1,2019-06-20,
    11111,1,2021-01-10,1,2020-05-05,

2. 每种药物的 concept sets (OUTPUT_CONCEPTS_DIR):
    - {药物名}_concepts.csv: 包含 concept_id, concept_name, concept_code 等详细信息
    - concept_sets_summary.csv: 所有药物的 concept ID 汇总统计

注意：所有配置都在下面的"配置区域"中，直接修改即可。
"""

import logging
from pathlib import Path
from typing import Any, Dict, List

import duckdb

LOGGER = logging.getLogger("drug_identifier")

# ============================================================
# 配置区域 - 在这里修改你的设置
# ============================================================

# OMOP CDM 数据路径
CDM_PATH = "/media/volume/GLP/RDRP_6287_GLP_1/"

# 数据表文件名
TABLES = {
    "concept": "r6287_concept.csv",
    "concept_ancestor": "r6287_concept_ancestor.csv",
    "drug_exposure": "r6287_drug_exposure.csv"
}

# 输入/输出
COHORT_FILTER = None  # 如果需要限制在特定队列，填写路径，如: "data/obesity_cohort.csv"
OUTPUT_CSV = "./output/drug_users_summary.csv"
OUTPUT_CONCEPTS_DIR = "./output/concept_sets"  # 保存每种药物的 concept ID sets

# 研究时间范围
STUDY_START_DATE = "2015-01-01"
STUDY_END_DATE = "2025-09-30"

# 药物成分配置
# 每种药物会生成两列: {药物名}_used 和 {药物名}_first_date
INGREDIENTS = {
    "semaglutide": {
        "concept_ids": [793143],
        "include_descendants": True
    },
    "liraglutide": {
        "concept_ids": [40170911],
        "include_descendants": True
    },
    "dulaglutide": {
        "concept_ids": [45774435],
        "include_descendants": True
    },
    "exenatide": {
        "concept_ids": [1583722],
        "include_descendants": True
    },
    "lixisenatide": {
        "concept_ids": [44506754],
        "include_descendants": True
    },
    "albiglutide": {
        "concept_ids": [44816332],
        "include_descendants": True
    },
    "tirzepatide": {
        "concept_ids": [779705],
        "include_descendants": True
    }
}

# 其他设置
DUCKDB_SAMPLE_SIZE = 200000
LOG_LEVEL = "INFO"

# ============================================================
# 以下是代码实现，一般不需要修改
# ============================================================


def _resolve_path(base: Path, relative_or_absolute: str) -> str:
    """Resolve a path that may be relative or absolute."""
    candidate = Path(relative_or_absolute)
    if not candidate.is_absolute():
        candidate = base / candidate
    return candidate.as_posix()


def _create_view(con: duckdb.DuckDBPyConnection, name: str, path: str, sample_size: int) -> None:
    """Create a view from a CSV file."""
    sql = (
        "CREATE OR REPLACE VIEW {name} AS\n"
        "SELECT * FROM read_csv_auto('{path}', SAMPLE_SIZE={sample_size}, IGNORE_ERRORS=TRUE)"
    ).format(name=name, path=path.replace("'", "''"), sample_size=sample_size)
    con.execute(sql)
    LOGGER.info(f"Created view: {name}")


def _get_descendant_concepts(
    con: duckdb.DuckDBPyConnection,
    ingredient_name: str,
    concept_ids: List[int],
    include_descendants: bool = True
) -> List[int]:
    """Create a temporary table with all descendant concepts for an ingredient.
    
    Returns:
        List of all concept IDs (including descendants if specified)
    """
    table_name = f"{ingredient_name}_concepts"
    
    if not concept_ids:
        LOGGER.warning(f"No concept IDs provided for {ingredient_name}")
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table_name} AS SELECT CAST(NULL AS BIGINT) AS concept_id WHERE 1=0"
        )
        return []
    
    values_clause = ", ".join(f"({cid})" for cid in concept_ids)
    
    if include_descendants:
        sql = f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
SELECT DISTINCT descendant_concept_id AS concept_id
FROM concept_ancestor
        WHERE ancestor_concept_id IN ({', '.join(str(cid) for cid in concept_ids)})
        UNION
        SELECT DISTINCT concept_id
        FROM (VALUES {values_clause}) AS v(concept_id)
        """
    else:
        sql = f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT concept_id
        FROM (VALUES {values_clause}) AS v(concept_id)
        """
    
    con.execute(sql)
    
    # Get all concept IDs
    result = con.execute(f"SELECT concept_id FROM {table_name} ORDER BY concept_id").fetchall()
    all_concept_ids = [row[0] for row in result]
    
    LOGGER.info(f"Created concept set for {ingredient_name}: {len(all_concept_ids)} concepts")
    return all_concept_ids


def identify_drug_users() -> None:
    """
    Identify drug users for multiple ingredients and create a summary table.
    
    For each ingredient, creates two columns:
    - {ingredient}_used: boolean flag (0/1)
    - {ingredient}_first_date: date of first use
    """
    base_path = Path(CDM_PATH)
    output_path = Path(OUTPUT_CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create output directory for concept sets
    concepts_dir = Path(OUTPUT_CONCEPTS_DIR)
    concepts_dir.mkdir(parents=True, exist_ok=True)
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    sample_size = DUCKDB_SAMPLE_SIZE
    
    # Initialize DuckDB connection
    con = duckdb.connect()
    
    # Load OMOP CDM tables
    _create_view(
        con,
        "concept",
        _resolve_path(base_path, TABLES.get("concept", "r6287_concept.csv")),
        sample_size
    )
    _create_view(
        con,
        "concept_ancestor",
        _resolve_path(base_path, TABLES.get("concept_ancestor", "r6287_concept_ancestor.csv")),
        sample_size
    )
    _create_view(
        con,
        "drug_exposure",
        _resolve_path(base_path, TABLES.get("drug_exposure", "r6287_drug_exposure.csv")),
        sample_size
    )
    
    # Optional: load cohort filter if provided
    if COHORT_FILTER:
        cohort_path = Path(COHORT_FILTER)
        _create_view(
            con,
            "cohort_filter",
            _resolve_path(cohort_path.parent, cohort_path.name),
            sample_size
        )
        LOGGER.info("Loaded cohort filter")
    
    # Get study date range
    min_date = STUDY_START_DATE
    max_date = STUDY_END_DATE
    
    # Process each ingredient
    if not INGREDIENTS:
        raise ValueError("No ingredients specified in configuration")
    
    LOGGER.info(f"Processing {len(INGREDIENTS)} ingredients: {', '.join(INGREDIENTS.keys())}")
    
    # Create concept sets for each ingredient and save them
    all_concept_sets = {}
    for ingredient_name, ingredient_cfg in INGREDIENTS.items():
        concept_ids = [int(cid) for cid in ingredient_cfg.get("concept_ids", [])]
        include_descendants = ingredient_cfg.get("include_descendants", True)
        
        # Get all concept IDs (including descendants)
        all_concepts = _get_descendant_concepts(con, ingredient_name, concept_ids, include_descendants)
        all_concept_sets[ingredient_name] = {
            "original_concept_ids": concept_ids,
            "all_concept_ids": all_concepts
        }
        
        # Save concept set with details to CSV
        if all_concepts:
            # Get concept details from concept table
            concept_list_str = ', '.join(str(cid) for cid in all_concepts)
            concept_details = con.execute(f"""
                SELECT 
                    concept_id,
                    concept_name,
                    concept_code,
                    concept_class_id,
                    vocabulary_id,
                    domain_id
                FROM concept
                WHERE concept_id IN ({concept_list_str})
                ORDER BY concept_id
            """).fetchall()
            
            # Save to CSV
            concept_csv_path = concepts_dir / f"{ingredient_name}_concepts.csv"
            LOGGER.info(f"Saving concept set for {ingredient_name} to {concept_csv_path}")
            
            # Create a dataframe-like structure and export
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE {ingredient_name}_concept_details AS
                SELECT 
                    c.concept_id,
                    c.concept_name,
                    c.concept_code,
                    c.concept_class_id,
                    c.vocabulary_id,
                    c.domain_id,
                    CASE WHEN c.concept_id IN ({', '.join(str(cid) for cid in concept_ids)}) 
                         THEN 1 ELSE 0 END AS is_original_ingredient
                FROM concept c
                WHERE c.concept_id IN (SELECT concept_id FROM {ingredient_name}_concepts)
                ORDER BY c.concept_id
            """)
            
            con.execute(
                f"COPY (SELECT * FROM {ingredient_name}_concept_details) TO '{concept_csv_path.as_posix()}' WITH (HEADER, DELIMITER ',')"
            )
    
    # Save a summary file with all concept sets
    summary_path = concepts_dir / "concept_sets_summary.csv"
    LOGGER.info(f"Saving concept sets summary to {summary_path}")
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("ingredient,original_concept_ids,num_original,num_total_concepts\n")
        for ingredient_name, concept_data in all_concept_sets.items():
            original_ids = ';'.join(str(x) for x in concept_data['original_concept_ids'])
            num_original = len(concept_data['original_concept_ids'])
            num_total = len(concept_data['all_concept_ids'])
            f.write(f"{ingredient_name},{original_ids},{num_original},{num_total}\n")
    
    # Get all unique person_ids from drug_exposure or cohort
    if COHORT_FILTER:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE all_persons AS
            SELECT DISTINCT person_id
            FROM cohort_filter
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE all_persons AS
            SELECT DISTINCT PERSON_ID AS person_id
            FROM drug_exposure
            WHERE CAST(COALESCE(DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_START_DATETIME) AS DATE) 
                BETWEEN DATE '{min_date}' AND DATE '{max_date}'
        """)
    
    person_count = con.execute("SELECT COUNT(*) FROM all_persons").fetchone()[0]
    LOGGER.info(f"Found {person_count} persons to analyze")
    
    # For each ingredient, find first use date
    for ingredient_name in INGREDIENTS.keys():
        LOGGER.info(f"Processing drug exposure for {ingredient_name}")
        
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {ingredient_name}_first_use AS
            SELECT
                person_id,
                MIN(CAST(COALESCE(DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_START_DATETIME) AS DATE)) AS first_date
            FROM drug_exposure
            WHERE DRUG_CONCEPT_ID IN (SELECT concept_id FROM {ingredient_name}_concepts)
              AND CAST(COALESCE(DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_START_DATETIME) AS DATE) 
                  BETWEEN DATE '{min_date}' AND DATE '{max_date}'
              AND CAST(COALESCE(DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_START_DATETIME) AS DATE) IS NOT NULL
            GROUP BY person_id
        """)
        
        user_count = con.execute(f"SELECT COUNT(*) FROM {ingredient_name}_first_use").fetchone()[0]
        LOGGER.info(f"Found {user_count} users of {ingredient_name}")
    
    # Build the final query with all ingredients
    select_clauses = ["ap.person_id"]
    join_clauses = []
    
    for ingredient_name in INGREDIENTS.keys():
        # Add used flag column
        select_clauses.append(
            f"CASE WHEN {ingredient_name}_fu.person_id IS NOT NULL THEN 1 ELSE 0 END AS {ingredient_name}_used"
        )
        # Add first date column
        select_clauses.append(
            f"{ingredient_name}_fu.first_date AS {ingredient_name}_first_date"
        )
        # Add join
        join_clauses.append(
            f"LEFT JOIN {ingredient_name}_first_use AS {ingredient_name}_fu ON ap.person_id = {ingredient_name}_fu.person_id"
        )
    
    final_sql = f"""
        SELECT
            {', '.join(select_clauses)}
        FROM all_persons ap
        {' '.join(join_clauses)}
        ORDER BY ap.person_id
    """
    
    LOGGER.info("Creating final drug users table")
    con.execute(f"CREATE OR REPLACE TEMP TABLE drug_users_final AS {final_sql}")
    
    # Export to CSV
    LOGGER.info(f"Writing results to {output_path}")
    con.execute(
        f"COPY (SELECT * FROM drug_users_final) TO '{output_path.as_posix()}' WITH (HEADER, DELIMITER ',')"
    )
    
    # Print summary statistics
    LOGGER.info("\n" + "="*60)
    LOGGER.info("SUMMARY STATISTICS")
    LOGGER.info("="*60)
    LOGGER.info(f"\nConcept sets saved to: {concepts_dir}")
    for ingredient_name, concept_data in all_concept_sets.items():
        LOGGER.info(f"  {ingredient_name}: {len(concept_data['all_concept_ids'])} concepts (from {len(concept_data['original_concept_ids'])} original)")
    LOGGER.info("")
    
    for ingredient_name in INGREDIENTS.keys():
        user_count = con.execute(
            f"SELECT SUM({ingredient_name}_used) FROM drug_users_final"
        ).fetchone()[0]
        percentage = (user_count / person_count * 100) if person_count > 0 else 0
        LOGGER.info(f"{ingredient_name}: {user_count} users ({percentage:.2f}%)")
    
    # Check for multi-drug users
    if len(INGREDIENTS) > 1:
        used_cols = [f"{name}_used" for name in INGREDIENTS.keys()]
        multi_drug_sql = f"SELECT SUM({' + '.join(used_cols)}) AS drug_count FROM drug_users_final"
        result = con.execute(f"""
            SELECT drug_count, COUNT(*) AS person_count
            FROM ({multi_drug_sql})
            GROUP BY drug_count
            ORDER BY drug_count
        """).fetchall()
        
        LOGGER.info("\nMulti-drug usage:")
        for drug_count, person_cnt in result:
            if drug_count > 0:
                LOGGER.info(f"  {int(drug_count)} drugs: {person_cnt} persons")
    
    LOGGER.info("="*60)
    
    con.close()
    LOGGER.info("Done!")


def main() -> None:
    """Main entry point."""
    identify_drug_users()


if __name__ == "__main__":
    main()
