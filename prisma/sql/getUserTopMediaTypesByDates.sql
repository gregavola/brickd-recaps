SELECT
    "brickd"."brickd_UserCollectionItemMedia"."mediaType",
    count("brickd"."brickd_UserCollectionItemMedia"."id") as "totalCount"
from
    "brickd"."brickd_UserCollection"
    inner join "brickd"."brickd_UserCollectionItem" on "brickd"."brickd_UserCollection".id = "brickd"."brickd_UserCollectionItem"."collectionId"
    inner join "brickd"."brickd_UserCollectionItemMedia" on "brickd"."brickd_UserCollectionItemMedia"."collectionItemId" = "brickd"."brickd_UserCollectionItem".id
WHERE
    "brickd"."brickd_UserCollection"."userId" = $1
    and "brickd"."brickd_UserCollection"."isTestingCollection" = 0
    AND "brickd"."brickd_UserCollectionItem"."createdAt" >= $2
    and "brickd"."brickd_UserCollectionItem"."createdAt" <= $3
group by
    "brickd"."brickd_UserCollectionItemMedia"."mediaType"