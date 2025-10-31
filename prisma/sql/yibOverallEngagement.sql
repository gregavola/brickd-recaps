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
            $2::timestamp AS start_local,
            $3::timestamp AS end_local,
            tz.tz
        FROM
            tz
    ),
    -- Activities authored by the target user
    my_activity AS (
        SELECT
            ua.id
        FROM
            brickd."brickd_UserActivity" ua
        WHERE
            ua."userId" = $1
    ),
    /* ------------ Totals (likes/comments/views) ------------ */
    likes_cte AS (
        SELECT
            COUNT(*)::bigint AS likes_received
        FROM
            brickd."brickd_UserLike" l
            JOIN my_activity a ON a.id = l."activityId"
            JOIN bounds b ON TRUE
        WHERE
            (
                (l."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (l."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    comments_cte AS (
        SELECT
            COUNT(*)::bigint AS comments_received
        FROM
            brickd."brickd_UserComment" c
            JOIN my_activity a ON a.id = c."activityId"
            JOIN bounds b ON TRUE
        WHERE
            c."isActive" = 1
            AND (
                (c."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (c."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    views_cte AS (
        SELECT
            COUNT(*)::bigint AS views_received
        FROM
            brickd."brickd_Views" v
            JOIN my_activity a ON a.id = v."activityId"
            JOIN bounds b ON TRUE
        WHERE
            COALESCE(v."isBot", 0) = 0
            AND (
                (v."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (v."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    /* ------------ Followers gained (as requested: actorId = $1) ------------ */
    follows_cte AS (
        SELECT
            COUNT(*)::bigint AS followers_gain
        FROM
            brickd."brickd_UserFollows" f
            JOIN bounds b ON TRUE
        WHERE
            f."actorId" = $1
            AND (
                (f."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (f."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    /* ------------ Top activity by score within the window ------------ */
    engaged_activity AS (
        -- only activities that had engagement in-window
        SELECT DISTINCT
            l."activityId" AS id
        FROM
            brickd."brickd_UserLike" l
            JOIN my_activity a ON a.id = l."activityId"
            JOIN bounds b ON TRUE
        WHERE
            (
                (l."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (l."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        UNION
        SELECT DISTINCT
            c."activityId"
        FROM
            brickd."brickd_UserComment" c
            JOIN my_activity a ON a.id = c."activityId"
            JOIN bounds b ON TRUE
        WHERE
            c."isActive" = 1
            AND (
                (c."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (c."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        UNION
        SELECT DISTINCT
            v."activityId"
        FROM
            brickd."brickd_Views" v
            JOIN my_activity a ON a.id = v."activityId"
            JOIN bounds b ON TRUE
        WHERE
            COALESCE(v."isBot", 0) = 0
            AND (
                (v."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (v."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    likes_by_act AS (
        SELECT
            l."activityId",
            COUNT(*)::bigint AS likes
        FROM
            brickd."brickd_UserLike" l
            JOIN engaged_activity ea ON ea.id = l."activityId"
            JOIN bounds b ON TRUE
        WHERE
            (
                (l."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (l."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        GROUP BY
            l."activityId"
    ),
    comments_by_act AS (
        SELECT
            c."activityId",
            COUNT(*)::bigint AS comments
        FROM
            brickd."brickd_UserComment" c
            JOIN engaged_activity ea ON ea.id = c."activityId"
            JOIN bounds b ON TRUE
        WHERE
            c."isActive" = 1
            AND (
                (c."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (c."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        GROUP BY
            c."activityId"
    ),
    views_by_act AS (
        SELECT
            v."activityId",
            COUNT(*)::bigint AS views
        FROM
            brickd."brickd_Views" v
            JOIN engaged_activity ea ON ea.id = v."activityId"
            JOIN bounds b ON TRUE
        WHERE
            COALESCE(v."isBot", 0) = 0
            AND (
                (v."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (v."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        GROUP BY
            v."activityId"
    ),
    scored AS (
        SELECT
            ea.id AS "activityId",
            COALESCE(v.views, 0) AS views,
            COALESCE(l.likes, 0) AS likes,
            COALESCE(c.comments, 0) AS comments,
            (
                COALESCE(v.views, 0) + 2 * COALESCE(l.likes, 0) + 5 * COALESCE(c.comments, 0)
            )::bigint AS score
        FROM
            engaged_activity ea
            LEFT JOIN views_by_act v ON v."activityId" = ea.id
            LEFT JOIN likes_by_act l ON l."activityId" = ea.id
            LEFT JOIN comments_by_act c ON c."activityId" = ea.id
    ),
    top_activity AS (
        SELECT
            s."activityId" AS top_activity_id,
            s.score AS top_activity_score
        FROM
            scored s
        ORDER BY
            s.score DESC,
            s."activityId" DESC
        LIMIT
            1
    )
SELECT
    l.likes_received,
    c.comments_received,
    v.views_received,
    f.followers_gain,
    ta.top_activity_id,
    ta.top_activity_score
FROM
    likes_cte l
    CROSS JOIN comments_cte c
    CROSS JOIN views_cte v
    CROSS JOIN follows_cte f
    LEFT JOIN top_activity ta ON TRUE;