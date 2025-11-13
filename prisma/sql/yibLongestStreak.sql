WITH
    "user_tz" AS (
        SELECT
            "timeZone"
        FROM
            "brickd"."brickd_User"
        WHERE
            "id" = $1
    ),
    "activity_days" AS (
        SELECT
            "userId",
            "localDate"::date AS "activity_date"
        FROM
            "brickd"."brickd_UserDayActivity"
        WHERE
            "userId" = $1
            AND "localDate" BETWEEN $2::date AND $3::date
    ),
    "streak_groups" AS (
        SELECT
            "userId",
            "activity_date",
            "activity_date" - INTERVAL '1 day' * ROW_NUMBER() OVER (
                ORDER BY
                    "activity_date"
            ) AS "grp"
        FROM
            "activity_days"
    ),
    "grouped_streaks" AS (
        SELECT
            "userId",
            MIN("activity_date") AS "streak_start",
            MAX("activity_date") AS "streak_end",
            COUNT(*) AS "streak_length"
        FROM
            "streak_groups"
        GROUP BY
            "userId",
            "grp"
    ),
    "longest_streak" AS (
        SELECT
            "userId",
            "streak_start",
            "streak_end",
            "streak_length"
        FROM
            "grouped_streaks"
        ORDER BY
            "streak_length" DESC,
            "streak_end" DESC
        LIMIT
            1
    )
SELECT
    ls."userId",
    ls."streak_length" AS "longestStreak",
    ls."streak_start" AS "longestStreakStart",
    ls."streak_end" AS "longestStreakEnd"
FROM
    "longest_streak" ls;