SELECT
    sum(
        "quantity" * "brickd"."brickd_Set"."numberOfParts"
    ) as "totalPieceCount"
from
    "brickd"."brickd_UserCollection"
    inner join "brickd"."brickd_UserCollectionItem" on "brickd"."brickd_UserCollection".id = "brickd"."brickd_UserCollectionItem"."collectionId"
    inner join "brickd"."brickd_Set" on "brickd"."brickd_Set".id = "brickd"."brickd_UserCollectionItem"."setId"
WHERE
    "brickd"."brickd_UserCollection"."userId" = $3
    and "brickd"."brickd_UserCollection"."isTestingCollection" = 0
    and "brickd"."brickd_UserCollectionItem"."buildStatus" = 'BUILT'
    and "brickd"."brickd_UserCollectionItem"."builtDate" >= $1
    AND "brickd"."brickd_UserCollectionItem"."builtDate" <= $2