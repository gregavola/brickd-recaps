WITH
    combined_activity AS (
        SELECT
            ua."userId" AS user_id,
            date_trunc(
                'day',
                ua."createdAt" AT TIME ZONE COALESCE(u."timeZone", 'UTC')
            ) AS activity_date_group
        FROM
            "brickd"."brickd_UserActivity" ua
            JOIN "brickd"."brickd_User" u ON u.id = ua."userId"
        WHERE
            ua."userId" = $1 -- user ID
            AND ua."createdAt" BETWEEN $2 AND $3 -- date range
        UNION ALL
        SELECT
            dm."userId" AS user_id,
            date_trunc(
                'day',
                dm."createdAt" AT TIME ZONE COALESCE(u."timeZone", 'UTC')
            ) AS activity_date_group
        FROM
            "brickd"."brickd_DiscussionMessage" dm
            JOIN "brickd"."brickd_User" u ON u.id = dm."userId"
        WHERE
            dm."userId" = $1
            AND dm."createdAt" BETWEEN $2 AND $3
        UNION ALL
        SELECT
            dl."userId" AS user_id,
            date_trunc(
                'day',
                dl."createdAt" AT TIME ZONE COALESCE(u."timeZone", 'UTC')
            ) AS activity_date_group
        FROM
            "brickd"."brickd_DiscussionLike" dl
            JOIN "brickd"."brickd_User" u ON u.id = dl."userId"
        WHERE
            dl."userId" = $1
            AND dl."createdAt" BETWEEN $2 AND $3
        UNION ALL
        SELECT
            cl."userId" AS user_id,
            date_trunc(
                'day',
                cl."createdAt" AT TIME ZONE COALESCE(u."timeZone", 'UTC')
            ) AS activity_date_group
        FROM
            "brickd"."brickd_UserCommentLikes" cl
            JOIN "brickd"."brickd_User" u ON u.id = cl."userId"
        WHERE
            cl."userId" = $1
            AND cl."createdAt" BETWEEN $2 AND $3
        UNION ALL
        SELECT
            c."userId" AS user_id,
            date_trunc(
                'day',
                c."createdAt" AT TIME ZONE COALESCE(u."timeZone", 'UTC')
            ) AS activity_date_group
        FROM
            "brickd"."brickd_UserComment" c
            JOIN "brickd"."brickd_User" u ON u.id = c."userId"
        WHERE
            c."userId" = $1
            AND c."createdAt" BETWEEN $2 AND $3
        UNION ALL
        SELECT
            l."userId" AS user_id,
            date_trunc(
                'day',
                l."createdAt" AT TIME ZONE COALESCE(u."timeZone", 'UTC')
            ) AS activity_date_group
        FROM
            "brickd"."brickd_UserLike" l
            JOIN "brickd"."brickd_User" u ON u.id = l."userId"
        WHERE
            l."userId" = $1
            AND l."createdAt" BETWEEN $2 AND $3
    ),
    user_activity AS (
        SELECT DISTINCT
            user_id,
            activity_date_group
        FROM
            combined_activity
    ),
    streaks AS (
        SELECT
            user_id,
            activity_date_group,
            SUM(
                CASE
                    WHEN previous_activity_date_group IS NULL
                    OR activity_date_group - previous_activity_date_group > INTERVAL '1 day' THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY
                    user_id
                ORDER BY
                    activity_date_group
            ) AS streak_group
        FROM
            (
                SELECT
                    user_id,
                    activity_date_group,
                    LAG(activity_date_group) OVER (
                        PARTITION BY
                            user_id
                        ORDER BY
                            activity_date_group
                    ) AS previous_activity_date_group
                FROM
                    user_activity
            ) sub
    ),
    streak_lengths AS (
        SELECT
            user_id,
            MIN(activity_date_group) AS streak_start_date,
            MAX(activity_date_group) AS streak_end_date,
            COUNT(*) AS streak_length
        FROM
            streaks
        GROUP BY
            user_id,
            streak_group
    )
SELECT
    user_id,
    streak_length AS longest_streak,
    streak_start_date AS longest_streak_start_date,
    streak_end_date AS longest_streak_end_date
FROM
    streak_lengths
ORDER BY
    streak_length DESC
LIMIT
    1;