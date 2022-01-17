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
    trips.StartTime,
    trips.EndTime,
    trips.StartCenterlineID,
    trips.StartCenterlineType,
    start_trips.StartStreet,
    start_trips.StartZip,
    start_trips.StartCity,
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
    

-- Extracting Date, Minute, Hour, Second from DateTime data
    
    SELECT 
        *,
        DATE(StartTime) AS StartDate,
        EXTRACT(hour FROM StartTime) AS StartHour,
        EXTRACT(minute FROM StartTime) AS StartMinute,
        EXTRACT(second FROM StartTime) AS StartSecond,
        EXTRACT(hour FROM EndTime) AS EndHour,
        EXTRACT(minute FROM EndTime) AS EndMinute,
        EXTRACT(second FROM EndTime) AS EndSecond
FROM `erudite-host-336919.minneapolis_scooter_data_2020.scooters_merged`;
