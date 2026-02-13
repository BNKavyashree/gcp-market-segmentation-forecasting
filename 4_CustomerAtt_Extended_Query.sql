CREATE OR REPLACE TABLE ⁠ vlba-2024-mpd-group-5.MPD_DATA_5.MPD_CustAtt_ex ⁠ AS
SELECT
    cust.CustomerId,
    cust.City,
    cust.SalesOrg,
    cust.Country,
    cust.Currency,
    loc.Latitude,
    loc.Longitude,
    ctxt.CustDescr
FROM
    'vlba-2024-mpd-group-5.MPD_DATA_5.MPD-CustomerAttr' AS cust
LEFT JOIN
    'vlba-2024-mpd-group-5.MPD_DATA_5.MPD_CustomersAttr_location_Coordinates' AS loc
ON
    cust.City = loc.City
LEFT JOIN
    'vlba-2024-mpd-group-5.MPD_DATA_5.MPD-CustomerText' AS ctxt
ON
    cust.CustomerId = ctxt.CustomerID;
