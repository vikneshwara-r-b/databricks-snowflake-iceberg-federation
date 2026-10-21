CREATE OR REFRESH MATERIALIZED VIEW team_performance_summary
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
WITH teams_current AS (
    SELECT * FROM  ${catalog_name}.${curated_schema_name}.dim_teams
    WHERE `__END_AT` IS NULL
)
SELECT t.team_Id,t.name as team_name,COUNT(DISTINCT race_result.race_Id) AS races_entered
,SUM(CASE WHEN race_result.driver_Race_Final_Position = 1 THEN 1 ELSE 0 END) AS wins
,SUM(CASE WHEN race_result.driver_Race_Final_Position <= 3 THEN 1 ELSE 0 END) AS podiums
,SUM(race_result.driver_Race_Points) AS total_points
,COUNT(DISTINCT race_result.driver_Id) AS drivers_used
,MAX(race_result.refreshed_at) AS refreshed_at
FROM ${catalog_name}.${curated_schema_name}.fact_race_results race_result
INNER JOIN teams_current t ON race_result.team_Id = t.team_Id
GROUP BY t.team_Id,t.name