WITH
    user_tz AS (
        SELECT
            COALESCE(u."timeZone", 'UTC') AS tz
        FROM
            brickd."brickd_User" u
        WHERE
            u.id = $1
    ),
    /* Combine NOTES + UCI (createdAt). Swap to builtDate if you prefer builds. */
    all_events AS (
        SELECT
            (
                n."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            ) AS local_ts
        FROM
            brickd."brickd_UserCollectionItemNote" n
            CROSS JOIN user_tz ut
        WHERE
            n."userId" = $1
        UNION ALL
        SELECT
            (
                uci."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            brickd."brickd_UserCollectionItem" uci
            CROSS JOIN user_tz ut
        WHERE
            uci."userId" = $1
            /* --- OR (for builds):
            SELECT (uci."builtDate" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz)
            FROM brickd."brickd_UserCollectionItem" uci
            CROSS JOIN user_tz ut
            WHERE uci."userId" = $1 AND uci."builtDate" IS NOT NULL
             */
    ),
    /* Restrict to local window if provided */
    filtered AS (
        SELECT
            e.local_ts
        FROM
            all_events e
            CROSS JOIN user_tz ut
        WHERE
            (
                $2::timestamptz IS NULL
                OR e.local_ts >= ($2::timestamptz AT TIME ZONE ut.tz)
            )
            AND (
                $3::timestamptz IS NULL
                OR e.local_ts < ($3::timestamptz AT TIME ZONE ut.tz)
            )
    ),
    /* Hour buckets (0–23) */
    hour_groups AS (
        SELECT
            EXTRACT(
                HOUR
                FROM
                    local_ts
            )::int AS hour_24,
            COUNT(*) AS cnt
        FROM
            filtered
        GROUP BY
            1
    ),
    top_hour AS (
        SELECT
            hour_24,
            cnt
        FROM
            hour_groups
        ORDER BY
            cnt DESC,
            hour_24 ASC
        LIMIT
            1
    ),
    /* Day-of-week counts (0=Sun..6=Sat) */
    dow_counts AS (
        SELECT
            EXTRACT(
                DOW
                FROM
                    local_ts
            )::int AS dow,
            COUNT(*) AS cnt
        FROM
            filtered
        GROUP BY
            1
    ),
    top_dow AS (
        SELECT
            d.dow,
            CASE d.dow
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END AS day_name,
            d.cnt
        FROM
            dow_counts d
        ORDER BY
            d.cnt DESC,
            d.dow ASC
        LIMIT
            1
    )
SELECT
    -- Top hour (24h + 12h)
    (
        SELECT
            th.hour_24
        FROM
            top_hour th
    ) AS "hour_24",
    CASE
        WHEN (
            SELECT
                th.hour_24
            FROM
                top_hour th
        ) IS NULL THEN NULL
        ELSE LPAD(
            (
                SELECT
                    th.hour_24::text
                FROM
                    top_hour th
            ),
            2,
            '0'
        ) || ':00–' || LPAD(
            (
                (
                    (
                        SELECT
                            th.hour_24
                        FROM
                            top_hour th
                    ) + 1
                ) % 24
            )::text,
            2,
            '0'
        ) || ':00'
    END AS "hour_range_24",
    CASE
        WHEN (
            SELECT
                th.hour_24
            FROM
                top_hour th
        ) IS NULL THEN NULL
        ELSE TO_CHAR(
            TO_TIMESTAMP(
                (
                    SELECT
                        th.hour_24::text
                    FROM
                        top_hour th
                ),
                'HH24'
            ),
            'FMHH12 AM'
        ) || ' – ' || TO_CHAR(
            TO_TIMESTAMP(
                (
                    (
                        (
                            SELECT
                                th.hour_24
                            FROM
                                top_hour th
                        ) + 1
                    ) % 24
                )::text,
                'HH24'
            ),
            'FMHH12 AM'
        )
    END AS "hour_range_12",
    COALESCE(
        (
            SELECT
                th.cnt
            FROM
                top_hour th
        ),
        0
    ) AS "hour_activity_count",
    -- Top day of week (name + count)
    (
        SELECT
            td.day_name
        FROM
            top_dow td
    ) AS "top_day_of_week",
    COALESCE(
        (
            SELECT
                td.cnt
            FROM
                top_dow td
        ),
        0
    ) AS "top_day_count";