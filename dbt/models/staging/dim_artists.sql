{{ config(materialized = 'table') }}

SELECT
    {{ dbt_utils.surrogate_key(['artistId', 'name']) }} AS artistKey,
    *
FROM
(
    SELECT
        MAX(artist_id) as artistId,
        MAX(artist_latitude) AS latitude,
        MAX(artist_longitude) AS longitude,
        MAX(artist_location) AS location,
        REPLACE(REPLACE(artist_name, '"', ''), '\\', '') AS name
    FROM {{ source('staging', 'songs') }}
    GROUP BY artist_name
    UNION ALL       
    SELECT 'AAAAAAAAAAAAAAAAAA', 0, 0, 'NA', 'NA'
)