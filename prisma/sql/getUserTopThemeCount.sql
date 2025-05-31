SELECT
    count(
        distinct "brickd"."brickd_ParentThemeItems"."uuid"
    ) as "totalCount"
from
    "brickd"."brickd_ParentTheme"
    inner join "brickd"."brickd_ParentThemeItems" on "brickd"."brickd_ParentThemeItems"."parentId" = "brickd"."brickd_ParentTheme".id
    inner join "brickd"."brickd_Set" on "brickd"."brickd_Set"."themeId" = "brickd"."brickd_ParentThemeItems"."themeId"
    inner join "brickd"."brickd_UserCollectionItem" on "brickd"."brickd_UserCollectionItem"."setId" = "brickd"."brickd_Set".id
    inner join "brickd"."brickd_UserCollection" on "brickd"."brickd_UserCollection"."id" = "brickd"."brickd_UserCollectionItem"."collectionId"
where
    "brickd"."brickd_UserCollectionItem"."userId" = $1
    and "brickd"."brickd_UserCollectionItem"."createdAt" >= $2
    and "brickd"."brickd_UserCollectionItem"."createdAt" <= $3
    and "brickd_UserCollection"."isWishList" = 0
    and "brickd"."brickd_UserCollectionItem"."buildStatus" = 'BUILT'