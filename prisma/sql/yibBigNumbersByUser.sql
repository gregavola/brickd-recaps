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
    /* ---------- BUILT TOTALS (children preferred; else parent) ---------- */
    child_built AS (
        SELECT
            uci.id AS uci_id,
            COUNT(*)::int AS child_built_count
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN brickd."brickd_UserCollection" uc ON uc.id = uci."collectionId"
            JOIN brickd."brickd_UserCollectionItemChildren" ucic ON ucic."collectionItemId" = uci.id
            JOIN bounds b ON TRUE
        WHERE
            uci."userId" = $1
            AND uc."isTestingCollection" = 0
            AND ucic."buildStatus" = 'BUILT'
            and uc."collectionType" = 'SETS'
            AND ucic."builtDate" IS NOT NULL
            AND (
                (ucic."builtDate" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (ucic."builtDate" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
        GROUP BY
            uci.id
    ),
    parent_built AS (
        SELECT
            uci.id AS uci_id,
            COALESCE(uci."quantity", 0)::int AS qty_if_parent_built
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN brickd."brickd_UserCollection" uc ON uc.id = uci."collectionId"
            JOIN bounds b ON TRUE
        WHERE
            uci."userId" = $1
            AND uc."isTestingCollection" = 0
            AND uci."buildStatus" = 'BUILT'
            and uc."collectionType" = 'SETS'
            AND uci."builtDate" IS NOT NULL
            AND (
                (uci."builtDate" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (uci."builtDate" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    built_rollup AS (
        SELECT
            uci.id AS uci_id,
            s."numberOfParts" AS parts_per_set,
            COALESCE(
                cb.child_built_count,
                CASE
                    WHEN pb.uci_id IS NOT NULL THEN pb.qty_if_parent_built
                    ELSE 0
                END
            )::int AS built_units
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN brickd."brickd_UserCollection" uc ON uc.id = uci."collectionId"
            JOIN brickd."brickd_Set" s ON s.id = uci."setId"
            LEFT JOIN child_built cb ON cb.uci_id = uci.id
            LEFT JOIN parent_built pb ON pb.uci_id = uci.id
        WHERE
            uci."userId" = $1
            AND uc."isTestingCollection" = 0
            and uc."collectionType" = 'SETS'
            AND (
                cb.child_built_count IS NOT NULL
                OR pb.uci_id IS NOT NULL
            )
    ),
    built_totals AS (
        SELECT
            COALESCE(SUM(built_units), 0)::int AS total_sets_built,
            COALESCE(SUM(built_units * parts_per_set), 0)::bigint AS total_parts_built
        FROM
            built_rollup
    ),
    /* ---------- ADDED-TO-COLLECTION (exclude wishlists) ---------- */
    added_to_collection AS (
        SELECT
            COALESCE(SUM(uci."quantity"), 0)::int AS total_sets_added
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN brickd."brickd_UserCollection" uc ON uc.id = uci."collectionId"
            JOIN bounds b ON TRUE
        WHERE
            uci."userId" = $1
            AND uc."isTestingCollection" = 0
            and uc."collectionType" = 'SETS'
            AND COALESCE(uc."isWishList", 0) = 0 -- adjust column name/casing if needed
            AND (
                (uci."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (uci."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    ),
    /* ---------- COLLECTIONS CREATED ---------- */
    collections_created AS (
        SELECT
            COUNT(*)::int AS total_collections_created
        FROM
            brickd."brickd_UserCollection" c
            JOIN bounds b ON TRUE
        WHERE
            c."userId" = $1
            AND c."isTestingCollection" = 0
            AND (
                (c."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) >= b.start_local
            AND (
                (c."createdAt" AT TIME ZONE 'UTC') AT TIME ZONE b.tz
            ) < b.end_local
    )
SELECT
    bt.total_sets_built,
    bt.total_parts_built,
    atc.total_sets_added,
    cc.total_collections_created
FROM
    built_totals bt
    CROSS JOIN added_to_collection atc
    CROSS JOIN collections_created cc;