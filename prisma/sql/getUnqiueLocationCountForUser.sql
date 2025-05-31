SELECT
    count(
        distinct "brickd"."brickd_UserCollectionItem"."locationId"
    ) as "totalDistinctCount",
    count("brickd"."brickd_UserCollectionItem"."locationId") as "totalCount"
from
    "brickd"."brickd_UserCollection"
    inner join "brickd"."brickd_UserCollectionItem" on "brickd"."brickd_UserCollection".id = "brickd"."brickd_UserCollectionItem"."collectionId"
WHERE
    "brickd"."brickd_UserCollection"."userId" = $1
    and "brickd"."brickd_UserCollection"."isTestingCollection" = 0
    AND "brickd"."brickd_UserCollectionItem"."createdAt" >= $2
    and "brickd"."brickd_UserCollectionItem"."createdAt" <= $3