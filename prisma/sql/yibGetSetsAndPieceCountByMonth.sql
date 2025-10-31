WITH
    tz AS (
        SELECT
            COALESCE(u."timeZone", 'UTC') AS tz
        FROM
            brickd."brickd_User" u
        WHERE
            u.id = $1
    ),
    built_local AS (
        SELECT
            date_trunc(
                'month',
                /* convert stored-UTC timestamp to user's local time */
                (
                    (uci."builtDate" AT TIME ZONE 'UTC') AT TIME ZONE tz.tz
                )
            ) AS month_local,
            (
                (uci."builtDate" AT TIME ZONE 'UTC') AT TIME ZONE tz.tz
            ) AS built_local_ts,
            s."numberOfParts" AS parts
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN brickd."brickd_UserCollection" uc on uci."collectionId" = uc.id
            JOIN brickd."brickd_Set" s ON s.id = uci."setId"
            CROSS JOIN tz
        WHERE
            uci."userId" = $1
            AND uc."isTestingCollection" = 0
            AND uci."buildStatus" = 'BUILT'
            AND uci."builtDate" IS NOT NULL
    )
SELECT
    to_char(month_local, 'MM-YY') AS "month",
    COUNT(*) AS "set_count",
    COALESCE(SUM(parts), 0) AS "parts_sum"
FROM
    built_local
WHERE
    built_local_ts >= ($2::timestamp) -- inclusive start (in user's local time)
    AND built_local_ts < ($3::timestamp) -- exclusive end (in user's local time)
GROUP BY
    month_local
ORDER BY
    month_local;