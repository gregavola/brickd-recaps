WITH
    top_locs_aggs as (
        SELECT
            "brickd"."brickd_UserCollectionItem"."locationId",
            count("brickd"."brickd_UserCollectionItem".id) as "totalCount"
        from
            "brickd"."brickd_UserCollectionItem"
        where
            "brickd"."brickd_UserCollectionItem"."userId" = $1
            AND "brickd"."brickd_UserCollectionItem"."updatedAt" >= $2
            AND "brickd"."brickd_UserCollectionItem"."updatedAt" <= $3
            AND "brickd"."brickd_UserCollectionItem"."locationId" is NOT NULL
        group by
            1
        order by
            "totalCount" desc
        LIMIT
            $4
    )
SELECT
    "brickd"."brickd_Location".uuid,
    "brickd"."brickd_Location".name,
    "brickd"."brickd_Location"."isOnline",
    "brickd"."brickd_Location"."url",
    "brickd"."brickd_Location"."region",
    "brickd"."brickd_Location"."city",
    "brickd"."brickd_Location"."imageUrl",
    "brickd"."brickd_Location"."googlePlaceId",
    "brickd"."brickd_Location"."fullAddress",
    "brickd"."brickd_Country"."codeShort",
    "brickd"."brickd_Country"."name" as "countryName",
    top_locs_aggs."totalCount"
from
    top_locs_aggs
    INNER JOIN "brickd"."brickd_Location" on "brickd"."brickd_Location".id = top_locs_aggs."locationId"
    INNER JOIN "brickd"."brickd_Country" on "brickd"."brickd_Country".id = "brickd"."brickd_Location"."countryId"
ORDER by
    top_locs_aggs."totalCount" desc