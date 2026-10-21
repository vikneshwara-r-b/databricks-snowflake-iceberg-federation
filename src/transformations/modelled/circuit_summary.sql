CREATE OR REFRESH MATERIALIZED VIEW circuit_summary
CLUSTER BY AUTO
TBLPROPERTIES (
    'pipelines.externalMetadata.enabled' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.enableRowTracking' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'delta.enableIcebergCompatV2' = 'false',
    'delta.enableIcebergCompatV3' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
)
WITH circuits_current AS (
    SELECT * FROM ${catalog_name}.${curated_schema_name}.dim_circuits
    WHERE `__END_AT` IS NULL
)
SELECT c.circuit_Id, c.name as circuit_name, c.city, c.country
,COUNT(DISTINCT r.race_Id) AS total_races_held
,MIN(r.race_date) AS first_race_date
,MAX(r.race_date) AS last_race_date
,c.lap_Record AS lap_record_time
,c.fastest_Lap_Driver_Id AS fastest_lap_driver_id
,MAX(r.refreshed_at) AS refreshed_at
FROM circuits_current c
INNER JOIN ${catalog_name}.${curated_schema_name}.fact_race_results r
ON c.circuit_Id = r.circuit_Id
GROUP BY c.circuit_Id, c.name, c.city, c.country, c.lap_Record, c.fastest_Lap_Driver_Id;