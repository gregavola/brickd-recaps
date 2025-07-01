WITH
    -- 1) Grab the user’s timezone
    user_tz AS (
        SELECT
            COALESCE("timeZone", 'UTC') AS tz
        FROM
            "brickd"."brickd_User"
        WHERE
            id = $1
    ),
    -- 2) Union all activity, normalized to the user’s local day boundary
    combined_activity AS (
        -- discussions
        SELECT
            ci."userId" AS user_id,
            DATE_TRUNC(
                'day',
                ci."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            ) AS activity_date
        FROM
            "brickd"."brickd_Discussion" ci
            CROSS JOIN user_tz ut
        WHERE
            ci."userId" = $1
            AND ci."isDeleted" = 0
        UNION ALL
        -- user collection items
        SELECT
            ci."userId",
            DATE_TRUNC(
                'day',
                ci."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            "brickd"."brickd_UserCollectionItem" ci
            CROSS JOIN user_tz ut
        WHERE
            ci."userId" = $1
        UNION ALL
        -- user collection item notes
        SELECT
            cin."userId",
            DATE_TRUNC(
                'day',
                cin."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            "brickd"."brickd_UserCollectionItemNote" cin
            CROSS JOIN user_tz ut
        WHERE
            cin."userId" = $1
        UNION ALL
        -- generic user activity
        SELECT
            ua."userId",
            DATE_TRUNC(
                'day',
                ua."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            "brickd"."brickd_UserActivity" ua
            CROSS JOIN user_tz ut
        WHERE
            ua."userId" = $1
        UNION ALL
        -- discussion messages
        SELECT
            dm."userId",
            DATE_TRUNC(
                'day',
                dm."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            "brickd"."brickd_DiscussionMessage" dm
            CROSS JOIN user_tz ut
        WHERE
            dm."userId" = $1
        UNION ALL
        -- discussion likes
        SELECT
            dl."userId",
            DATE_TRUNC(
                'day',
                dl."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            "brickd"."brickd_DiscussionLike" dl
            CROSS JOIN user_tz ut
        WHERE
            dl."userId" = $1
        UNION ALL
        -- comment likes
        SELECT
            cl."userId",
            DATE_TRUNC(
                'day',
                cl."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            "brickd"."brickd_UserCommentLikes" cl
            CROSS JOIN user_tz ut
        WHERE
            cl."userId" = $1
        UNION ALL
        -- user comments
        SELECT
            cm."userId",
            DATE_TRUNC(
                'day',
                cm."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            "brickd"."brickd_UserComment" cm
            CROSS JOIN user_tz ut
        WHERE
            cm."userId" = $1
        UNION ALL
        -- user likes
        SELECT
            ul."userId",
            DATE_TRUNC(
                'day',
                ul."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE ut.tz
            )
        FROM
            "brickd"."brickd_UserLike" ul
            CROSS JOIN user_tz ut
        WHERE
            ul."userId" = $1
    ),
    -- 3) Deduplicate days and restrict to the [$2, $3] window
    user_activity AS (
        SELECT DISTINCT
            ca.user_id,
            ca.activity_date
        FROM
            combined_activity ca
            CROSS JOIN user_tz ut
        WHERE
            ca.activity_date > DATE_TRUNC('day', $2::timestamptz AT TIME ZONE ut.tz)
            AND ca.activity_date <= DATE_TRUNC('day', $3::timestamptz AT TIME ZONE ut.tz)
    ),
    -- 4) Identify streak “groups” by looking for gaps > 1 day
    streaks AS (
        SELECT
            user_id,
            activity_date,
            SUM(
                CASE
                    WHEN prev_date IS NULL
                    OR activity_date - prev_date > INTERVAL '1 day' THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    user_id
                ORDER BY
                    activity_date
            ) AS streak_group
        FROM
            (
                SELECT
                    user_id,
                    activity_date,
                    LAG(activity_date) OVER (
                        PARTITION BY
                            user_id
                        ORDER BY
                            activity_date
                    ) AS prev_date
                FROM
                    user_activity
            ) t
    ),
    -- 5) Collapse each group into (start, end, length)
    streak_lengths AS (
        SELECT
            user_id,
            MIN(activity_date) AS streak_start,
            MAX(activity_date) AS streak_end,
            COUNT(*) AS streak_length
        FROM
            streaks
        GROUP BY
            user_id,
            streak_group
    )
    -- 6) Return the single longest streak (0 if none)
SELECT
    $1 AS user_id,
    -- if no rows, subquery yields NULL, so COALESCE to 0
    COALESCE(
        (
            SELECT
                sl.streak_length
            FROM
                streak_lengths sl
            ORDER BY
                sl.streak_length DESC
            LIMIT
                1
        ),
        0
    ) AS longest_streak_length,
    -- start date of that longest streak
    (
        SELECT
            sl.streak_start
        FROM
            streak_lengths sl
        ORDER BY
            sl.streak_length DESC
        LIMIT
            1
    ) AS longest_streak_start,
    -- end date of that longest streak
    (
        SELECT
            sl.streak_end
        FROM
            streak_lengths sl
        ORDER BY
            sl.streak_length DESC
        LIMIT
            1
    ) AS longest_streak_end;