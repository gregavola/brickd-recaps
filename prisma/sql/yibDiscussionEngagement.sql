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
    -- Your discussions (exclude deleted)
    my_discussions AS (
        SELECT
            d.id
        FROM
            brickd."brickd_Discussion" d
        WHERE
            d."userId" = $1
            AND COALESCE(d."isDeleted", 0) = 0
    ),
    -- How many discussions you created within the window
    discussions_created_cte AS (
        SELECT
            COUNT(*)::bigint AS discussions_created
        FROM
            brickd."brickd_Discussion" d
            JOIN bounds b ON TRUE
        WHERE
            d."userId" = $1
            AND COALESCE(d."isDeleted", 0) = 0
            AND (
                (d."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (d."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    -- Likes on your discussions in the window
    discussion_likes_cte AS (
        SELECT
            COUNT(*)::bigint AS discussion_likes_received
        FROM
            brickd."brickd_DiscussionLike" dl
            JOIN my_discussions md ON md.id = dl."discussionId"
            JOIN bounds b ON TRUE
        WHERE
            (
                (dl."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (dl."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    -- Likes on messages YOU created in the window
    message_likes_cte AS (
        SELECT
            COUNT(*)::bigint AS message_likes_received
        FROM
            brickd."brickd_DiscussionMessageLike" dml
            JOIN brickd."brickd_DiscussionMessage" dm ON dm.id = dml."messageId"
            JOIN bounds b ON TRUE
        WHERE
            dm."userId" = $1
            AND (
                (dml."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (dml."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    -- Comments (messages) posted on your discussions in the window (exclude deleted)
    comments_on_my_discussions_cte AS (
        SELECT
            COUNT(*)::bigint AS comments_on_your_discussions
        FROM
            brickd."brickd_DiscussionMessage" dm
            JOIN my_discussions md ON md.id = dm."discussionId"
            JOIN bounds b ON TRUE
        WHERE
            COALESCE(dm."isDeleted", 0) = 0
            AND (
                (dm."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (dm."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    -- Non-bot views on your discussions in the window
    discussion_views_cte AS (
        SELECT
            COUNT(*)::bigint AS discussion_views_received
        FROM
            brickd."brickd_DiscussionView" dv
            JOIN my_discussions md ON md.id = dv."discussionId"
            JOIN bounds b ON TRUE
        WHERE
            COALESCE(dv."isBot", 0) = 0
            AND (
                (dv."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (dv."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    /* --------- Per-discussion engagement (for top discussion) --------- */
    likes_by_discussion AS (
        SELECT
            dl."discussionId",
            COUNT(*)::bigint AS likes
        FROM
            brickd."brickd_DiscussionLike" dl
            JOIN my_discussions md ON md.id = dl."discussionId"
            JOIN bounds b ON TRUE
        WHERE
            (
                (dl."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (dl."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        GROUP BY
            dl."discussionId"
    ),
    comments_by_discussion AS (
        SELECT
            dm."discussionId",
            COUNT(*)::bigint AS comments
        FROM
            brickd."brickd_DiscussionMessage" dm
            JOIN my_discussions md ON md.id = dm."discussionId"
            JOIN bounds b ON TRUE
        WHERE
            COALESCE(dm."isDeleted", 0) = 0
            AND (
                (dm."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (dm."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        GROUP BY
            dm."discussionId"
    ),
    views_by_discussion AS (
        SELECT
            dv."discussionId",
            COUNT(*)::bigint AS views
        FROM
            brickd."brickd_DiscussionView" dv
            JOIN my_discussions md ON md.id = dv."discussionId"
            JOIN bounds b ON TRUE
        WHERE
            COALESCE(dv."isBot", 0) = 0
            AND (
                (dv."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (dv."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        GROUP BY
            dv."discussionId"
    ),
    scored_discussions AS (
        SELECT
            md.id AS "discussionId",
            COALESCE(v.views, 0) AS views,
            COALESCE(l.likes, 0) AS likes,
            COALESCE(c.comments, 0) AS comments,
            (
                COALESCE(v.views, 0) + 2 * COALESCE(l.likes, 0) + 5 * COALESCE(c.comments, 0)
            )::bigint AS score
        FROM
            my_discussions md
            LEFT JOIN views_by_discussion v ON v."discussionId" = md.id
            LEFT JOIN likes_by_discussion l ON l."discussionId" = md.id
            LEFT JOIN comments_by_discussion c ON c."discussionId" = md.id
    ),
    top_discussion AS (
        SELECT
            "discussionId" AS top_discussion_id,
            score AS top_discussion_score
        FROM
            scored_discussions
        ORDER BY
            score DESC,
            "discussionId" DESC
        LIMIT
            1
    )
SELECT
    dc.discussions_created,
    (
        dl.discussion_likes_received + ml.message_likes_received
    )::bigint AS likes_received_total,
    cmd.comments_on_your_discussions,
    dv.discussion_views_received,
    td.top_discussion_id,
    td.top_discussion_score
FROM
    discussions_created_cte dc
    CROSS JOIN discussion_likes_cte dl
    CROSS JOIN message_likes_cte ml
    CROSS JOIN comments_on_my_discussions_cte cmd
    CROSS JOIN discussion_views_cte dv
    LEFT JOIN top_discussion td ON TRUE;