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
            $2::timestamp AS start_local, -- local wall-clock start
            $3::timestamp AS end_local, -- local wall-clock end (exclusive)
            tz.tz
        FROM
            tz
    ),
    -- Your media (owned by user) that were TAKEN/CREATED within the same bounds
    my_media AS (
        SELECT
            m.id,
            m."mediaKey",
            m."mediaAltKey"
        FROM
            brickd."brickd_UserCollectionItemMedia" m
            JOIN brickd."brickd_UserCollectionItem" uci ON uci.id = m."collectionItemId"
            JOIN bounds b ON TRUE
        WHERE
            uci."userId" = $1
            -- ensure media was created within the same local window
            AND timezone (b.tz, m."createdAt") >= b.start_local
            AND timezone (b.tz, m."createdAt") < b.end_local
    ),
    -- Views on your (in-window) media within the window (exclude bots)
    views_by_media AS (
        SELECT
            mv."mediaId",
            COUNT(*)::bigint AS views
        FROM
            brickd."brickd_MediaViewed" mv
            JOIN my_media mm ON mm.id = mv."mediaId"
            JOIN bounds b ON TRUE
        WHERE
            COALESCE(mv."isBot", 0) = 0
            AND timezone (b.tz, mv."createdAt") >= b.start_local
            AND timezone (b.tz, mv."createdAt") < b.end_local
        GROUP BY
            mv."mediaId"
    ),
    total_views_cte AS (
        SELECT
            COALESCE(SUM(vbm.views), 0)::bigint AS total_media_views
        FROM
            views_by_media vbm
    ),
    total_media_cte AS (
        SELECT
            COALESCE(COUNT(*), 0)::bigint AS total_media
        FROM
            views_by_media
    ),
    top3_rows AS (
        SELECT
            vbm."mediaId",
            vbm.views
        FROM
            views_by_media vbm
        ORDER BY
            vbm.views DESC,
            vbm."mediaId" DESC
        LIMIT
            3
    ),
    top3_cte AS (
        SELECT
            COALESCE(
                json_agg(
                    jsonb_build_object(
                        'mediaId',
                        mm.id,
                        'views',
                        t.views,
                        'mediaKey',
                        mm."mediaKey",
                        'mediaAltKey',
                        mm."mediaAltKey"
                    )
                    ORDER BY
                        t.views DESC,
                        mm.id DESC
                ),
                '[]'::json
            ) AS top_media
        FROM
            top3_rows t
            JOIN my_media mm ON mm.id = t."mediaId"
    )
SELECT
    tv.total_media_views,
    tm.total_media,
    top.top_media
FROM
    total_views_cte tv
    CROSS JOIN total_media_cte tm
    CROSS JOIN top3_cte top;