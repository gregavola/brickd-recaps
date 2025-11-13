SELECT
    MAX(uci."totalDuration") AS "maxDuration",
    MIN(uci."totalDuration") AS "minDuration",
    SUM(uci."totalDuration") AS "totalDurationSum",
    AVG(uci."totalDuration") AS "avgDuration",
    COUNT(uci."totalDuration") AS "itemsWithDuration"
FROM
    "brickd"."brickd_UserCollectionItem" uci
WHERE
    uci."userId" = $1 -- specific userId
    AND uci."totalDuration" IS NOT NULL -- ignore nulls
    AND uci."builtDate" >= $2 -- startDate
    AND uci."builtDate" <= $3;

-- endDate