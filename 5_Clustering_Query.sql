CREATE OR REPLACE MODEL 'vlba-2024-mpd-group-5.MPD_DATA_5.Cust_Markt_clustering'
OPTIONS(
  model_type='kmeans',
  num_clusters=4,
  standardize_features=true
) AS
SELECT
  SAFE_CAST(Latitude AS FLOAT64) AS Latitude,
  SAFE_CAST(Longitude AS FLOAT64) AS Longitude
FROM
  'vlba-2024-mpd-group-5.MPD_DATA_5.MPD_CustAtt_extended';
