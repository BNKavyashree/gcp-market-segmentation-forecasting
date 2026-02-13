SELECT
    Region,
    SUM(TotalRevenue) AS TotalRevenue
FROM
    (
        SELECT
            A.Cluster_name AS Region,
            SUM(C.RevenueUSD) AS TotalRevenue
        FROM
            'vlba-2024-mpd-group-5.MPD_DATA_5.MPD_vis_cluster' AS A
        JOIN
            'vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Orders' AS B ON A.CustomerID = B.CustomerID
        JOIN
            'vlba-2024-mpd-group-5.MPD_DATA_5.MPD-OrderItems' AS C ON B.OrderNumber = C.OrderNumber
        GROUP BY
            A.Cluster_name
    ) AS RevenueSummary
GROUP BY
Region;
