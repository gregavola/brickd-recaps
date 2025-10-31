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
    /* items added by user in window (use addedAt when present, else createdAt) */
    items AS (
        SELECT
            ucmii."minifigId",
            mf."figureNumber",
            mf."figureImageUrl",
            (
                (
                    COALESCE(ucmii."addedAt", ucmii."createdAt") AT TIME ZONE 'UTC'
                )
            ) AT TIME ZONE b.tz AS ts_local
        FROM
            brickd."brickd_UserCollectionMinifigItem" ucmii
            JOIN brickd."brickd_UserCollection" uc ON uc.id = ucmii."collectionId"
            JOIN brickd."brickd_Minifig" mf ON ucmii."minifigId" = mf.id
            JOIN bounds b ON TRUE
        WHERE
            ucmii."userId" = $1
            AND uc."isTestingCollection" = 0
            AND uc."isWishList" = 0
            AND (
                (
                    COALESCE(ucmii."addedAt", ucmii."createdAt") AT TIME ZONE 'UTC'
                ) AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (
                    COALESCE(ucmii."addedAt", ucmii."createdAt") AT TIME ZONE 'UTC'
                ) AT TIME ZONE b.tz
            ) < b.end_local
    ),
    /* minifigs that are exclusive to exactly one set in the whole catalog */
    exclusive_minifigs AS (
        SELECT
            sm."figureNumber"
        FROM
            brickd."brickd_SetMinifigs" sm
        GROUP BY
            sm."figureNumber"
        HAVING
            COUNT(DISTINCT sm."setNumber") = 1
    )
SELECT
    /* same metrics as your original query */
    (
        SELECT
            COUNT(DISTINCT i."minifigId")
        FROM
            items i
    ) AS unique_minifigs_added,
    (
        SELECT
            COUNT(DISTINCT i."figureNumber")
        FROM
            items i
            LEFT JOIN exclusive_minifigs e ON e."figureNumber" = i."figureNumber"
        WHERE
            e."figureNumber" IS NOT NULL
    ) AS exclusive_minifigs_added,
    /* last 15 minifigure image URLs (non-null), deduped by minifig, newest-first by user-local time */
    COALESCE(
        (
            SELECT
                ARRAY (
                    SELECT
                        x."figureImageUrl"
                    FROM
                        (
                            SELECT DISTINCT
                                ON (i."minifigId") i."minifigId",
                                i."figureImageUrl",
                                i.ts_local
                            FROM
                                items i
                            WHERE
                                i."figureImageUrl" IS NOT NULL
                            ORDER BY
                                i."minifigId",
                                i.ts_local DESC
                        ) x
                    ORDER BY
                        x.ts_local DESC
                    LIMIT
                        15
                )
        ),
        ARRAY[]::text[]
    ) AS image_urls;