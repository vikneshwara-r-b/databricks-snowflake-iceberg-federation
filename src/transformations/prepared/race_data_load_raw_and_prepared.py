import os
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

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


@dp.table(
    name="raw_race",
    comment="Stored raw race details data sourced from F1 API",
    cluster_by_auto=True,
    table_properties=UNIFORM_TABLE_PROPERTIES
)
def load_raw_race_details():
    volume_root_path = spark.conf.get("volume_root_path")
    volume_subdir_path = spark.conf.get("volume_subdir_path")
    volume_path = os.path.join(volume_root_path, volume_subdir_path)
    
    ddl_schema_str = "STRUCT<api: STRING, limit: BIGINT, offset: BIGINT, races: STRUCT<circuit: ARRAY<STRUCT<circuitId: STRING, circuitLength: STRING, circuitName: STRING, city: STRING, corners: BIGINT, country: STRING, fastestLapDriverId: STRING, fastestLapTeamId: STRING, fastestLapYear: BIGINT, firstParticipationYear: BIGINT, lapRecord: STRING, url: STRING>>, date: STRING, raceId: STRING, raceName: STRING, results: ARRAY<STRUCT<driver: STRUCT<birthday: STRING, driverId: STRING, name: STRING, nationality: STRING, number: BIGINT, shortName: STRING, surname: STRING, url: STRING>, fastLap: STRING, grid: STRING, points: BIGINT, position: STRING, retired: STRING, team: STRUCT<constructorsChampionships: BIGINT, driversChampionships: BIGINT, firstAppareance: BIGINT, nationality: STRING, teamId: STRING, teamName: STRING, url: STRING>, time: STRING>>, round: STRING, time: STRING, url: STRING>, season: BIGINT, total: BIGINT, url: STRING>"
    native_schema = StructType.fromDDL(ddl_schema_str)

    # 3. Read Auto Loader stream (removed conflicting options)
    final_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(native_schema)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "*.json")
        .option("recursiveFileLookup", "true")
        .load(volume_path)
        .withColumn("source_file_name", F.col("_metadata.file_name"))
        .withColumn("source_file_load_date", F.col("_metadata.file_modification_time"))
        .withColumn("load_date_time", F.current_timestamp())
    )

    return final_df

@dp.table(
    name="race_raw_quarantined",
    comment="Stores invalid or corrupt JSON records caught by Auto Loader rescued data column.",
    cluster_by_auto=True,
    table_properties=UNIFORM_TABLE_PROPERTIES
)
def load_quarantine_race_details():
    return (
        dp.read_stream("raw_race")
        .filter(F.col("_rescued_data").isNotNull())
        .withColumn("load_date_time", F.current_timestamp())
    )

@dp.table(
    name="race_results_valid",
    comment="race results data ready for flattening and downstream transformations.",
    cluster_by_auto=True,
    table_properties=UNIFORM_TABLE_PROPERTIES
)
def load_valid_race_results():
   race_data_nested_df = (
        dp.read_stream("raw_race")
        .filter(F.col("_rescued_data").isNull())
        .select(
        F.col("url").alias("api_url"),
        F.col("season").alias("season_year"),
        F.col("races.round").cast("integer").alias("race_Round"),
        F.col("races.date").cast("date").alias("race_Date"),
        F.col("races.time").alias("race_Time"),
        F.col("races.raceId").alias("race_Id"),
        F.col("races.raceName").alias("race_Name"),
        F.col("races.circuit").alias("circuit_nested"),
        F.col("races.results").alias("results_nested"),
        "source_file_name",
        "source_file_load_date",
        "load_date_time"
        )
    )
   return race_data_nested_df

@dp.table(
    name="race_results_unnested",
    comment="race results information in unnested way",
    schema="""
    season_year integer,race_Id string,driver_Id string,team_Id string,circuit_Id string,race_Name string,race_Round integer
    ,race_Date date,race_Time string,driver_Race_Grid_Position string,driver_Race_Final_Position string
    ,driver_Race_Points float,driver_Race_Fast_Lap string,driver_Race_Gap_With_Win_Time string
    ,source_file_load_date timestamp,load_date_time timestamp
    """,
    cluster_by_auto=True,
    table_properties=UNIFORM_TABLE_PROPERTIES
)
def load_unnested_race_results():
   nested_df = dp.readStream("race_results_valid").withColumn("value",  F.explode(F.col("results_nested")))
   race_results_unnested_df = (
        nested_df.select(
        F.col("season_year").cast("integer"),
        "race_Id",
        F.col("value.driver.driverId").alias("driver_Id"),
        F.col("value.team.teamId").alias("team_Id"),
        F.expr("circuit_nested[0].circuitId").alias("circuit_Id"),
        "race_Name",
        "race_Round",
        "race_Date",
        "race_Time",
        F.col("value.grid").alias("driver_Race_Grid_Position"),
        F.col("value.position").alias("driver_Race_Final_Position"),
        F.col("value.points").cast("float").alias("driver_Race_Points"),
        F.col("value.fastLap").alias("driver_Race_Fast_Lap"),
        F.col("value.time").alias("driver_Race_Gap_With_Win_Time"),
        "source_file_load_date",
        "load_date_time"
        )
   )
   return race_results_unnested_df

@dp.table(
    name="race_drivers",
    comment="race drivers information",
    schema = """
    driver_Id string,name string,surname string,shortName string
    ,nationality string,birthday date,number integer,race_Date date,load_date_time timestamp
    """,
    cluster_by_auto=True,
    table_properties=UNIFORM_TABLE_PROPERTIES
)
def load_race_driver_info():
   nested_df = dp.readStream("race_results_valid").withColumn("value",  F.explode(F.col("results_nested")))
   drivers_df = (
        nested_df.select(
        F.col("value.driver.driverId").alias("driver_Id"),
        F.col("value.driver.name").alias("name"),
        F.col("value.driver.surname").alias("surname"),
        F.col("value.driver.shortName").alias("shortName"),
        F.col("value.driver.nationality").alias("nationality"),
        F.col("value.driver.birthday").cast("date").alias("birthday"),
        F.col("value.driver.number").cast("integer").alias("number"),
        "race_Date",
        "load_date_time"
        )
   )
   return drivers_df


@dp.table(
    name="race_teams",
    comment="race teams information",
    schema = """
    team_Id string,name string,nationality string,first_Appearance integer
    ,constructors_Championships integer,drivers_Championships integer,race_Date date,load_date_time timestamp
    """,
    cluster_by_auto=True,
    table_properties=UNIFORM_TABLE_PROPERTIES
)
def load_race_teams_info():
   nested_df = dp.readStream("race_results_valid").withColumn("value",  F.explode(F.col("results_nested")))
   race_teams_df = (
        nested_df.select(
            F.col("value.team.teamId").alias("team_id"),
            F.col("value.team.teamName").alias("name"),
            F.col("value.team.nationality").alias("nationality"),
            F.col("value.team.firstAppareance").cast("integer").alias("first_Appearance"),
            F.col("value.team.constructorsChampionships").cast("integer").alias("constructors_championships"),
            F.col("value.team.driversChampionships").cast("integer").alias("drivers_championships"),
            "race_Date",
            "load_date_time"
        ).distinct()
   )
   return race_teams_df

@dp.table(
    name="race_circuits",
    comment="race circuits information",
    schema = """
    circuit_Id string,name string,city string,country string,length string
    ,corners integer,first_Participation_Year integer,lap_Record string
    ,fastest_Lap_Driver_Id string,fastest_Lap_Team_Id string,fastest_Lap_Year integer,race_Date date,load_date_time timestamp
    """,
    cluster_by_auto=True,
    table_properties=UNIFORM_TABLE_PROPERTIES
)
def load_race_circuits_info():
   race_circuits_df = (
        dp.readStream("race_results_valid").select(
            F.expr("circuit_nested[0].circuitId").alias("circuit_Id"),
            F.expr("circuit_nested[0].circuitName").alias("name"),
            F.expr("circuit_nested[0].city").alias("city"),
            F.expr("circuit_nested[0].country").alias("country"),
            F.expr("circuit_nested[0].circuitLength").alias("length"),
            F.expr("circuit_nested[0].corners").cast("integer").alias("corners"),
            F.expr("circuit_nested[0].firstParticipationYear").cast("integer").alias("first_Participation_Year"),
            F.expr("circuit_nested[0].lapRecord").alias("lap_Record"),
            F.expr("circuit_nested[0].fastestLapDriverId").alias("fastest_Lap_Driver_Id"),
            F.expr("circuit_nested[0].fastestLapTeamId").alias("fastest_Lap_Team_Id"),
            F.expr("circuit_nested[0].fastestLapYear").cast("integer").alias("fastest_Lap_Year"),
            "race_Date",
            "load_date_time"
        )
   )
   return race_circuits_df