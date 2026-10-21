CREATE OR REFRESH MATERIALIZED VIEW driver_performance_summary
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
WITH drivers_current AS (
    SELECT * FROM ${catalog_name}.${curated_schema_name}.dim_drivers
    WHERE `__END_AT` IS NULL
),
teams_current AS (
    SELECT * FROM ${catalog_name}.${curated_schema_name}.dim_teams
    WHERE `__END_AT` IS NULL
)
SELECT race_result.season_year, d.driver_Id, d.name as driver_name, t.team_Id, t.name as team_name
,COUNT(DISTINCT race_result.race_Id) as races_entered
,COUNT(CASE WHEN race_result.driver_Race_Final_Position = 1 THEN d.driver_Id END) AS wins
,COUNT(CASE WHEN race_result.driver_Race_Final_Position <= 3 THEN d.driver_Id END) AS podiums
,SUM(race_result.driver_Race_Points) AS total_points
,AVG(race_result.driver_Race_Final_Position) AS avg_finish_position
,MIN(race_result.driver_Race_Final_Position) AS best_finish_position
,MAX(race_result.refreshed_at) AS refreshed_at
FROM ${catalog_name}.${curated_schema_name}.fact_race_results race_result
INNER JOIN drivers_current d ON race_result.driver_Id = d.driver_Id
INNER JOIN teams_current t ON race_result.team_Id = t.team_Id
GROUP BY race_result.season_year, d.driver_Id, d.name, t.team_Id, t.name