WITH
    audience AS (
        SELECT
            "brickd"."brickd_User"."id",
            "brickd"."brickd_User"."uuid",
            "brickd"."brickd_User"."userName",
            (
                SELECT
                    count("brickd"."brickd_UserCollectionItem"."id")
                FROM
                    "brickd"."brickd_UserCollectionItem"
                WHERE
                    "brickd"."brickd_UserCollectionItem"."createdAt" >= $1
                    AND "brickd"."brickd_UserCollectionItem"."createdAt" <= $2
                    AND "brickd"."brickd_UserCollectionItem"."userId" = "brickd"."brickd_User".id
            ) AS "totalSets",
            (
                SELECT
                    count("brickd"."brickd_UserCollectionMinifigItem"."id")
                FROM
                    "brickd"."brickd_UserCollectionMinifigItem"
                WHERE
                    "brickd"."brickd_UserCollectionMinifigItem"."createdAt" >= $1
                    AND "brickd"."brickd_UserCollectionMinifigItem"."createdAt" <= $2
                    AND "brickd"."brickd_UserCollectionMinifigItem"."userId" = "brickd"."brickd_User".id
            ) AS "totalMinifigs"
        FROM
            "brickd"."brickd_User"
        WHERE
            "brickd"."brickd_User".id = 1
    )
SELECT
    a.*
FROM
    audience a
    LEFT JOIN "brickd"."brickd_UserRecap" ur ON ur."userId" = a.id
    AND ur."reportDate" = $3
WHERE
    (
        a."totalMinifigs" >= 2
        OR a."totalSets" >= 5
    )
    AND ur.id IS NULL
ORDER BY
    a.id ASC;