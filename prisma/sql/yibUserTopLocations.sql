WITH
    tz AS (
        SELECT
            COALESCE(u."timeZone", 'UTC') AS tz
        FROM
            brickd."brickd_User" u
        WHERE
            u.id = $1
    ),
    bounds AS (
        SELECT
            -- Interpret $2/$3 as *local* timestamps in the user's TZ, convert to timestamptz (UTC)
            ($2::timestamp AT TIME ZONE tz.tz) AS start_utc,
            ($3::timestamp AT TIME ZONE tz.tz) AS end_utc
        FROM
            tz
    ),
    filtered AS (
        SELECT
            uci."locationId"
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN bounds b ON TRUE
        WHERE
            uci."userId" = $1
            AND uci."locationId" IS NOT NULL
            AND uci."updatedAt" >= b.start_utc
            AND uci."updatedAt" <= b.end_utc
    ),
    unique_locations AS (
        SELECT
            COUNT(DISTINCT "locationId")::bigint AS total_unique_locations
        FROM
            filtered
    ),
    top_locs_aggs AS (
        SELECT
            f."locationId",
            COUNT(*)::bigint AS "count"
        FROM
            filtered f
        GROUP BY
            f."locationId"
        ORDER BY
            "count" DESC
        LIMIT
            $4
    ),
    top_locs AS (
        SELECT
            l."name",
            tla."count",
            CASE
                WHEN l."isOnline" = 1 THEN COALESCE(NULLIF(l."region", ''), 'Online')
                ELSE NULLIF(
                    CONCAT_WS(
                        ', ',
                        NULLIF(l."city", ''),
                        NULLIF(l."region", '')
                    ),
                    ''
                )
            END AS "fullAddress",
            l."url",
            l."isOnline"
        FROM
            top_locs_aggs tla
            JOIN brickd."brickd_Location" l ON l.id = tla."locationId"
    )
SELECT
    ul.total_unique_locations,
    COALESCE(
        (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'name',
                        t."name",
                        'count',
                        t."count",
                        'fullAddress',
                        COALESCE(t."fullAddress", 'Online'),
                        'url',
                        t."url",
                        'isOnline',
                        t."isOnline"
                    )
                    ORDER BY
                        t."count" DESC
                )
            FROM
                top_locs t
        ),
        '[]'::jsonb
    ) AS locations
FROM
    unique_locations ul;