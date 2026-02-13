UPDATE 'vlba-2024-mpd-group-5.MPD_DATA_5.MPD-ProductMat'
SET Amount_Required = CAST(
  ROUND((
    SELECT AVG(SAFE_CAST(Amount_Required AS FLOAT64))
    FROM 'vlba-2024-mpd-group-5.MPD_DATA_5.MPD-ProductMat'
    WHERE SAFE_CAST(Amount_Required AS FLOAT64) IS NOT NULL
  ), 2) AS STRING
)
WHERE REGEXP_CONTAINS(Amount_Required, r'\d') AND REGEXP_CONTAINS(Amount_Required, r'[a-zA-Z]');
