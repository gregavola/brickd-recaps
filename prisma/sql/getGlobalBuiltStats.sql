SELECT
    sum("brickd"."brickd_Set"."numberOfParts") as "totalPieces",
    count("brickd"."brickd_Set"."id") as "totalSets",
    count(distinct "brickd"."brickd_Set"."id") as "totalDistinctSets",
    (
        SELECT
            count(*)
        from
            "brickd"."brickd_User"
        where
            "createdAt" >= $1
            and "createdAt" <= $2
            and "isActive" = 1
            and "isTestUser" = 0
    ) as "totalUsers"
from
    "brickd"."brickd_Set"
    INNER JOIN "brickd"."brickd_UserCollectionItem" on "brickd"."brickd_UserCollectionItem"."setId" = "brickd"."brickd_Set"."id"
WHERE
    "brickd"."brickd_UserCollectionItem"."buildStatus" = 'BUILT'
    and "brickd"."brickd_UserCollectionItem"."createdAt" >= $1
    and "brickd"."brickd_UserCollectionItem"."createdAt" <= $2