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
    -- All levels earned by this user in the local window
    earned AS (
        SELECT
            ubl."userId",
            ubl."levelId",
            bcl."challengeId",
            bcl."imageUrl" AS level_image_url,
            (
                (ubl."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) AS local_created_at
        FROM
            brickd."brickd_UserBuildChallengeLevels" ubl
            JOIN brickd."brickd_BuildChallengeLevels" bcl ON bcl.id = ubl."levelId"
            JOIN bounds b ON TRUE
        WHERE
            ubl."userId" = $1
            AND (
                (ubl."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (ubl."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    counts AS (
        SELECT
            COUNT(DISTINCT e."levelId") AS users_earned, -- for single user: 0 or 1
            COUNT(DISTINCT e."challengeId") AS unique_challenges
        FROM
            earned e
    ),
    -- Top 5 challenges by number of levels earned in-window (include high-level artwork)
    top6 AS (
        SELECT
            e."challengeId",
            bc."name",
            bc."imageUrl" AS challenge_image_url, -- high-level artwork
            COUNT(*)::bigint AS levels_earned
        FROM
            earned e
            JOIN brickd."brickd_BuildChallenge" bc ON bc.id = e."challengeId"
        GROUP BY
            e."challengeId",
            bc."name",
            bc."imageUrl"
        ORDER BY
            levels_earned DESC,
            e."challengeId" DESC
        LIMIT
            6
    ),
    top6_json AS (
        SELECT
            json_agg(
                jsonb_build_object(
                    'challengeId',
                    t."challengeId",
                    'name',
                    t."name",
                    'levels_earned',
                    t.levels_earned,
                    'imageUrl',
                    t.challenge_image_url
                )
                ORDER BY
                    t.levels_earned DESC,
                    t."challengeId" DESC
            ) AS top_challenges
        FROM
            top6 t
    ),
    -- Most recent level earned (by local time) -> return its level image
    most_recent_level AS (
        SELECT
            e.level_image_url AS recent_level_image_url
        FROM
            earned e
        ORDER BY
            e.local_created_at DESC,
            e."levelId" DESC
        LIMIT
            1
    ),
    last25_badges AS (
        SELECT
            e.level_image_url,
            e.local_created_at,
            e."levelId"
        FROM
            earned e
        ORDER BY
            e.local_created_at DESC,
            e."levelId" DESC
        LIMIT
            25
    ),
    last25_badge_images_json AS (
        SELECT
            COALESCE(
                json_agg(
                    l.level_image_url
                    ORDER BY
                        l.local_created_at DESC,
                        l."levelId" DESC
                ),
                '[]'::json
            ) AS last_25_badge_image_urls
        FROM
            last25_badges l
    )
SELECT
    c.users_earned,
    c.unique_challenges,
    COALESCE(tj.top_challenges, '[]'::json) AS top_challenges,
    (
        SELECT
            mrl.recent_level_image_url
        FROM
            most_recent_level mrl
    ) AS recent_level_image_url,
    (
        SELECT
            j.last_25_badge_image_urls
        FROM
            last25_badge_images_json j
    ) AS last_25_badge_image_urls
FROM
    counts c
    CROSS JOIN top6_json tj;