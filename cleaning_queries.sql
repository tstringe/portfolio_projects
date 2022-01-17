/*

Cleaning 2020 Minneapolis Scooter Data with SQL Queries in BigQuery

*/

-- Joining Scooter Ride Data with Geographic Centerline Data for Starting/Ending Locations

WITH start_trips AS -- temporary table to hold starting location geographic data
    (SELECT 
        trips.ObjectId,
        trips.StartCenterlineID,
        centerlines.STREETALL,
        centerlines.ZIP5_L,
        centerlines.CITYLEFT
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
        centerlines.STREETALL,
        centerlines.ZIP5_L,
        centerlines.CITYLEFT
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
    trips.StartTime,
    trips.EndTime,
    trips.StartCenterlineID,
    trips.StartCenterlineType,
    start_trips.STREETALL,
    start_trips.ZIP5_L,
    start_trips.CITYLEFT,
    trips.EndCenterlineID,
    trips.EndCenterlineType,
    end_trips.STREETALL,
    end_trips.ZIP5_L,
    end_trips.CITYLEFT
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
