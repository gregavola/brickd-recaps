WITH
    filtered AS (
        SELECT
            pt.id AS "parentId",
            pt.uuid AS "uuid",
            pt.name AS "name",
            uci.id AS "uci_id",
            s.id AS "set_id"
        FROM
            brickd."brickd_ParentTheme" pt
            JOIN brickd."brickd_ParentThemeItems" pti ON pti."parentId" = pt.id
            JOIN brickd."brickd_Set" s ON s."themeId" = pti."themeId"
            JOIN brickd."brickd_UserCollectionItem" uci ON uci."setId" = s.id
            JOIN brickd."brickd_UserCollection" uc ON uc.id = uci."collectionId"
        WHERE
            uci."userId" = $1
            AND (
                (
                    uci."createdAt" IS NOT NULL
                    AND uci."createdAt" >= $2
                    AND uci."createdAt" <= $3
                )
                OR (
                    uci."builtDate" IS NOT NULL
                    AND uci."builtDate" >= $2
                    AND uci."builtDate" <= $3
                )
            )
            AND uc."isTestingCollection" = 0
            AND uc."isWishList" = 0
    ),
    grouped AS (
        SELECT
            "parentId",
            "uuid",
            "name",
            COUNT(DISTINCT "set_id") AS "totalCount"
        FROM
            filtered
        GROUP BY
            1,
            2,
            3
    )
SELECT
    g."parentId",
    g."uuid",
    g."name",
    g."totalCount",
    COALESCE(imgs.images, '[]'::json) AS images
FROM
    grouped g
    LEFT JOIN LATERAL (
        /* Up to 10 sets (distinct by set id), most recent first in this window */
        SELECT
            json_agg(obj) AS images
        FROM
            (
                SELECT DISTINCT
                    ON (s.id) jsonb_build_object(
                        'setId',
                        s.id,
                        'setNumber',
                        s."setNumber",
                        'name',
                        s."name",
                        'setImageUrl',
                        s."setImageUrl",
                        'customImageUrl',
                        s."customSetImageUrl"
                    ) AS obj
                FROM
                    brickd."brickd_Set" s
                    JOIN brickd."brickd_UserCollectionItem" uci2 ON uci2."setId" = s.id
                    JOIN brickd."brickd_UserCollection" uc2 ON uc2.id = uci2."collectionId"
                    JOIN brickd."brickd_ParentThemeItems" pti2 ON pti2."themeId" = s."themeId"
                WHERE
                    uci2."userId" = $1
                    AND (
                        (
                            uci2."createdAt" IS NOT NULL
                            AND uci2."createdAt" >= $2
                            AND uci2."createdAt" <= $3
                        )
                        OR (
                            uci2."builtDate" IS NOT NULL
                            AND uci2."builtDate" >= $2
                            AND uci2."builtDate" <= $3
                        )
                    )
                    AND uc2."isTestingCollection" = 0
                    AND uc2."isWishList" = 0
                    AND pti2."parentId" = g."parentId"
                ORDER BY
                    s.id,
                    COALESCE(uci2."builtDate", uci2."createdAt") DESC
                LIMIT
                    10
            ) t
    ) AS imgs ON TRUE
ORDER BY
    g."totalCount" DESC
LIMIT
    $4;