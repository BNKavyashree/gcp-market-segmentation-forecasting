SELECT
    Region,
    SUM(CustomerCount) AS TotalCustomers
FROM
    (
        SELECT
            A.Cluster_name AS Region,
            COUNT(DISTINCT B.CustomerID) AS CustomerCount
        FROM
            'vlba-2024-mpd-group-5.MPD_DATA_5.MPD_vis_cluster' AS A
        JOIN
            'vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Orders'AS B ON A.CustomerID = B.CustomerID
        GROUP BY
            A.Cluster_name
    ) AS CustomerSummary
GROUP BY
    Region;
