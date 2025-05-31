WITH
    combined_activity AS (
        SELECT
            "userId" AS user_id,
            DATE_TRUNC('day', "createdAt" - INTERVAL '12 hours') AS activity_date_group
        FROM
            "brickd"."brickd_UserActivity"
        WHERE
            "userId" = $1 -- Replace with the specific user ID
            AND "createdAt" BETWEEN $2 AND $3 -- Replace with the date range
        UNION ALL
        SELECT
            "userId" AS user_id,
            DATE_TRUNC('day', "createdAt" - INTERVAL '12 hours') AS activity_date_group
        FROM
            "brickd"."brickd_DiscussionMessage"
        WHERE
            "userId" = $1 -- Replace with the specific user ID
            AND "createdAt" BETWEEN $2 AND $3 -- Replace with the date range
        UNION ALL
        SELECT
            "userId" AS user_id,
            DATE_TRUNC('day', "createdAt" - INTERVAL '12 hours') AS activity_date_group
        FROM
            "brickd"."brickd_DiscussionLike"
        WHERE
            "userId" = $1 -- Replace with the specific user ID
            AND "createdAt" BETWEEN $2 AND $3 -- Replace with the date range
        UNION ALL
        SELECT
            "userId" AS user_id,
            DATE_TRUNC('day', "createdAt" - INTERVAL '12 hours') AS activity_date_group
        FROM
            "brickd"."brickd_UserCommentLikes"
        WHERE
            "userId" = $1 -- Replace with the specific user ID
            AND "createdAt" BETWEEN $2 AND $3 -- Replace with the date range
        UNION ALL
        SELECT
            "userId" AS user_id,
            DATE_TRUNC('day', "createdAt" - INTERVAL '12 hours') AS activity_date_group
        FROM
            "brickd"."brickd_UserComment"
        WHERE
            "userId" = $1 -- Replace with the specific user ID
            AND "createdAt" BETWEEN $2 AND $3 -- Replace with the date range
        UNION ALL
        SELECT
            "userId" AS user_id,
            DATE_TRUNC('day', "createdAt" - INTERVAL '12 hours') AS activity_date_group
        FROM
            "brickd"."brickd_UserLike"
        WHERE
            "userId" = $1 -- Replace with the specific user ID
            AND "createdAt" BETWEEN $2 AND $3 -- Replace with the date range
    ),
    user_activity AS (
        -- Group by unique day per user
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
            -- Generate streak grouping by checking for gaps > 24 hours
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
                    -- Get the previous activity date to check for gaps
                    LAG(activity_date_group) OVER (
                        PARTITION BY
                            user_id
                        ORDER BY
                            activity_date_group
                    ) AS previous_activity_date_group
                FROM
                    user_activity
            ) subquery
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

-- Only return the longest streak