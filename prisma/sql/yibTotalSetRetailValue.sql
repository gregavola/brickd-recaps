WITH
    u AS (
        SELECT
            id,
            "countryId",
            COALESCE("isBuilder", 0) AS is_builder
        FROM
            brickd."brickd_User"
        WHERE
            id = $1
    ),
    -- Candidate vendors (not manual) whose country-group includes the user's country
    candidate_vendors AS (
        SELECT
            v.id,
            v."name",
            v."currencyCode" AS currency,
            v."countryCode",
            v."groupId"
        FROM
            brickd."brickd_SetMarketplaceVendor" v
            JOIN u ON TRUE
            JOIN brickd."brickd_SetMarketplaceVendorCountryGroupItems" cgi ON cgi."groupId" = v."groupId"
            AND cgi."countryId" = u."countryId"
        WHERE
            v."isManual" = 0
            AND v.id IN (1, 6, 9, 15, 21, 30)
    ),
    -- Pick exactly ONE vendor (tie-breaker = smallest id; replace with your priority if you have it)
    chosen_vendor AS (
        SELECT
            *
        FROM
            candidate_vendors
        ORDER BY
            id
        LIMIT
            1
    ),
    -- User's owned sets (non-wishlist) within the time bounds
    user_sets AS (
        SELECT DISTINCT
            uci."setId"
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN brickd."brickd_UserCollection" uc ON uc.id = uci."collectionId"
        WHERE
            uci."userId" = $1
            AND uc."isWishList" = 0
            and uc."isTestingCollection" = 0
            and uc."collectionType" = 'SETS'
            AND (uci."createdAt" BETWEEN $2 AND $3)
    ),
    -- Total sets in that same bounded window
    total_sets_have AS (
        SELECT
            COUNT(DISTINCT uci."setId")::bigint AS total_sets
        FROM
            brickd."brickd_UserCollectionItem" uci
            JOIN brickd."brickd_UserCollection" uc ON uc.id = uci."collectionId"
        WHERE
            uci."userId" = $1
            AND uc."isWishList" = 0
            and uc."isTestingCollection" = 0
            and uc."collectionType" = 'SETS'
            AND (uci."createdAt" BETWEEN $2 AND $3)
    ),
    -- Price aggregates for the chosen vendor over the bounded set list
    vendor_prices AS (
        SELECT
            m."vendorId",
            SUM(m.price)::numeric(18, 2) AS sum_price,
            MIN(m.price)::numeric(18, 2) AS min_price,
            MAX(m.price)::numeric(18, 2) AS max_price,
            AVG(m.price)::numeric(18, 2) AS avg_price,
            COUNT(DISTINCT m."setId") AS total_sets_for_vendor
        FROM
            brickd."brickd_SetMarketplace" m
            JOIN chosen_vendor cv ON cv.id = m."vendorId"
        WHERE
            m.price <> 0
            AND m."setId" IN (
                SELECT
                    "setId"
                FROM
                    user_sets
            )
        GROUP BY
            m."vendorId"
    )
SELECT
    ts.total_sets AS "totalSets", -- total sets in bounds (user-owned)
    cv.id AS "vendorId",
    cv."name" AS "vendorName",
    cv.currency AS "currency",
    cv."countryCode",
    CASE
        WHEN cv.id IN (1, 6, 9) THEN 0
        ELSE 1
    END AS "isBuilder",
    COALESCE(vp.total_sets_for_vendor, 0) AS "vendorSets",
    COALESCE(vp.sum_price, 0) AS "totalPrice",
    COALESCE(vp.min_price, 0) AS "minPrice",
    COALESCE(vp.max_price, 0) AS "maxPrice",
    COALESCE(vp.avg_price, 0) AS "avgPrice"
FROM
    total_sets_have ts
    LEFT JOIN chosen_vendor cv ON TRUE
    LEFT JOIN vendor_prices vp ON vp."vendorId" = cv.id;