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
    -- Your media (via your collection items)
    my_media AS (
        SELECT
            m.id,
            m."mediaKey",
            m."mediaAltKey"
        FROM
            brickd."brickd_UserCollectionItemMedia" m
            JOIN brickd."brickd_UserCollectionItem" uci ON uci.id = m."collectionItemId"
        WHERE
            uci."userId" = $1
    ),
    -- Views on your media within the window (exclude bots)
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
            AND (
                (mv."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (mv."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        GROUP BY
            mv."mediaId"
    ),
    total_views_cte AS (
        SELECT
            COALESCE(SUM(vbm.views), 0)::bigint AS total_media_views
        FROM
            views_by_media vbm
    ),
    -- NEW: how many distinct media had at least one view in the window
    total_media_cte AS (
        SELECT
            COUNT(*)::bigint AS total_media
        FROM
            views_by_media
    ),
    top3_cte AS (
        SELECT
            json_agg(
                jsonb_build_object(
                    'mediaId',
                    mm.id,
                    'views',
                    vbm.views,
                    'mediaKey',
                    mm."mediaKey",
                    'mediaAltKey',
                    mm."mediaAltKey"
                )
                ORDER BY
                    vbm.views DESC,
                    mm.id DESC
            ) AS top_media
        FROM
            (
                SELECT
                    vbm.*
                FROM
                    views_by_media vbm
                ORDER BY
                    vbm.views DESC,
                    vbm."mediaId" DESC
                LIMIT
                    3
            ) vbm
            JOIN my_media mm ON mm.id = vbm."mediaId"
    )
SELECT
    COALESCE(tv.total_media_views, 0)::bigint AS total_media_views,
    COALESCE(tm.total_media, 0)::bigint AS total_media,
    COALESCE(top.top_media, '[]'::json) AS top_media
FROM
    total_views_cte tv
    CROSS JOIN total_media_cte tm
    CROSS JOIN top3_cte top;