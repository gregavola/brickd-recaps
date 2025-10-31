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
    wishlist_added AS (
        SELECT
            COALESCE(SUM(uci."quantity"), 0)::int AS total_wishlist_added
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN brickd."brickd_UserCollection" uc ON uc.id = uci."collectionId"
            JOIN bounds b ON TRUE
        WHERE
            uci."userId" = $1
            AND uc."isTestingCollection" = 0 -- drop if you want to include testing collections
            AND COALESCE(uc."isWishList", 0) = 1 -- wishlist only
            AND (
                (
                    COALESCE(uci."addedAt", uci."createdAt") AT TIME ZONE 'UTC'
                ) AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (
                    COALESCE(uci."addedAt", uci."createdAt") AT TIME ZONE 'UTC'
                ) AT TIME ZONE b.tz
            ) < b.end_local
    ),
    deleted_wishlist AS (
        SELECT
            COUNT(*)::int AS total_deleted_count
        FROM
            brickd."brickd_DeleteCollectionItemLog" d
            JOIN bounds b ON TRUE
        WHERE
            d."userId" = $1
            AND d."reason" = 'WISHLIST'
            AND (
                (d."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (d."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    )
SELECT
    wa.total_wishlist_added,
    dw.total_deleted_count
FROM
    wishlist_added wa
    CROSS JOIN deleted_wishlist dw;