from pyspark import pipelines as dp
from pyspark.sql import functions as F

UNIFORM_TABLE_PROPERTIES = {
    # Required for Unity Catalog to serve external Iceberg metadata for this table's
    # queryable name (not just its internal pipeline bookkeeping) — without it,
    # Snowflake's Iceberg REST client rejects the table even with UniForm otherwise
    # correctly enabled, same gap that blocked the gold layer before this property
    # was added there. Needed now that the sync notebook covers every schema, not
    # just analytics/gold (src/utility/snowflake_sync_config.json).
    "pipelines.externalMetadata.enabled": "true",
    "delta.columnMapping.mode": "name",
    "delta.enableRowTracking": "true",
    # Streaming tables and materialized views require Iceberg v3, not v2 — v3 flips
    # the deletion-vectors requirement from v2 (disabled) to enabled by default on
    # new v3 tables, and v2 must be explicitly turned off, not just left unset.
    "delta.enableDeletionVectors": "true",
    "delta.enableIcebergCompatV2": "false",
    "delta.enableIcebergCompatV3": "true",
    "delta.universalFormat.enabledFormats": "iceberg",
}

catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("raw_schema_name")

source_drivers_table = f"{catalog_name}.{schema_name}.race_drivers"
source_teams_table = f"{catalog_name}.{schema_name}.race_teams"
source_circuits_table = f"{catalog_name}.{schema_name}.race_circuits"
source_race_results_table = f"{catalog_name}.{schema_name}.race_results_unnested"

target_drivers_table = "dim_drivers"
target_teams_table = "dim_teams"
target_circuits_table = "dim_circuits"
target_race_results_table = "fact_race_results"

dp.create_streaming_table(
    name = target_drivers_table,
    comment = "Drivers Dimension Table stored in SCD Type-2",
    cluster_by_auto = True,
    table_properties = UNIFORM_TABLE_PROPERTIES
)

dp.create_auto_cdc_flow(
  target = target_drivers_table,
  source = source_drivers_table,
  keys = ["driver_Id"],
  sequence_by = F.col("race_Date"),
  stored_as_scd_type = 2
)

dp.create_streaming_table(
    name = target_teams_table,
    comment = "Teams Dimension Table stored in SCD Type-2",
    cluster_by_auto = True,
    table_properties = UNIFORM_TABLE_PROPERTIES
)

dp.create_auto_cdc_flow(
  target = target_teams_table,
  source = source_teams_table,
  keys = ["team_id"],
  sequence_by = F.col("race_Date"),
  stored_as_scd_type = 2
)

dp.create_streaming_table(
    target_circuits_table,
    comment = "Circuits Dimension Table stored in SCD Type-2",
    cluster_by_auto = True,
    table_properties = UNIFORM_TABLE_PROPERTIES
)

dp.create_auto_cdc_flow(
  target = target_circuits_table,
  source = source_circuits_table,
  keys = ["circuit_Id"],
  sequence_by = F.col("race_Date"),
  stored_as_scd_type = 2
)

dp.create_streaming_table(
    target_race_results_table,
    comment = "Race results stored in denormalized manner",
    cluster_by_auto = True,
    table_properties = UNIFORM_TABLE_PROPERTIES
)

@dp.view
def race_results_source():
    return dp.read_stream(source_race_results_table).withColumnRenamed("load_date_time", "refreshed_at")

dp.create_auto_cdc_flow(
  target = target_race_results_table,
  source = "race_results_source",
  keys = ["race_Id", "driver_Id"],
  sequence_by = F.col("refreshed_at"),
  stored_as_scd_type = 1
)