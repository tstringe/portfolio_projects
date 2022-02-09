/*

Cleaning 2020 Minneapolis Scooter Data with SQL Queries in BigQuery

*/

-- Joining Scooter Ride Data with Geographic Centerline Data for Starting/Ending Locations

WITH start_trips AS -- temporary table to hold starting location geographic data
    (SELECT 
        trips.ObjectId,
        trips.StartCenterlineID,
        centerlines.STREETALL AS StartStreet,
        centerlines.ZIP5_L AS StartZip,
        centerlines.CITYLEFT AS StartCity
    FROM 
        `erudite-host-336919.minneapolis_scooter_data_2020.scooters_2020` AS trips
    LEFT JOIN
        `erudite-host-336919.minneapolis_scooter_data_2020.centerline_data` AS centerlines
    ON 
        trips.StartCenterlineID = FORMAT("%.*f",2,CAST(centerlines.GBSID AS FLOAT64) + .0001)), -- Converting integer Id's to decimals and casting as strings for future joining

end_trips AS -- temporary table to hold ending location geographic data
    (SELECT 
        trips.ObjectId,
        trips.EndCenterlineID,
        centerlines.STREETALL AS EndStreet,
        centerlines.ZIP5_L AS EndZip,
        centerlines.CITYLEFT AS EndCity
    FROM 
        `erudite-host-336919.minneapolis_scooter_data_2020.scooters_2020` AS trips
    LEFT JOIN
        `erudite-host-336919.minneapolis_scooter_data_2020.centerline_data` AS centerlines
    ON 
        trips.EndCenterlineID = FORMAT("%.*f",2,CAST(centerlines.GBSID AS FLOAT64) + .0001)) -- Converting integer Id's to decimals and casting as strings for future joining

SELECT 
    trips.ObjectId,
    trips.TripID,
    trips.TripDuration,
    trips.TripDistance,
    Date(trips.StartTime) AS StartDate, --Extract date from start timestamp
    Extract(time FROM DATETIME_TRUNC(DATE_ADD(StartTime, INTERVAL 5 Minute), Hour)) AS StartTime, -- Puts HH:59:59 into next hour and truncates to keep other times within the same hour
    trips.StartCenterlineID,
    trips.StartCenterlineType,
    start_trips.StartStreet,
    start_trips.StartZip,
    start_trips.StartCity,
    DATE(trips.EndTime) AS EndDate, --Extract date from end timestamp
    Extract(time FROM DATETIME_TRUNC(DATE_ADD(StartTime, INTERVAL 5 Minute), Hour)) AS EndTime, -- Puts HH:59:59 into next hour and truncates to keep other times within the same hour
    trips.EndCenterlineID,
    trips.EndCenterlineType,
    end_trips.EndStreet,
    end_trips.EndZip,
    end_trips.EndCity
FROM 
    `erudite-host-336919.minneapolis_scooter_data_2020.scooters_2020` AS trips
JOIN
    start_trips
ON 
    trips.ObjectId = start_trips.ObjectId
JOIN
    end_trips 
ON 
    trips.ObjectID = end_trips.ObjectID;

-- Checking for Duplicate Trips

SELECT 
    *
FROM 
    `erudite-host-336919.minneapolis_scooter_data_2020.scooters_merged`
WHERE 
    TripID IN (
        SELECT 
            TripID 
        FROM 
            `erudite-host-336919.minneapolis_scooter_data_2020.scooters_merged`
        GROUP BY 
            TripID 
        HAVING 
            COUNT(1) > 1
       )
       
-- Removing Duplicates

DELETE FROM 
    `erudite-host-336919.minneapolis_scooter_data_2020.scooters_merged`
WHERE TripID IN (
    SELECT 
        TripID 
    FROM (
        SELECT
            TripId,
            ROW_NUMBER() OVER (PARTITION BY TripID) AS rownum 
            FROM 
                `erudite-host-336919.minneapolis_scooter_data_2020.scooters_merged`
    ) AS sub
    WHERE 
        rownum > 1
);
