WITH
    agg AS (
        SELECT
            r."userId",
            COUNT(*) AS "totalCount",
            MAX(r."createdAt") AS "lastSeen"
        FROM
            brickd."brickd_Requests" r
        WHERE
            r."createdAt" >= $1
            AND r."userId" NOT IN (0, 1)
            -- (optional, if nullable) AND r."userId" IS NOT NULL
        GROUP BY
            r."userId"
    )
SELECT
    u."id",
    u."uuid",
    u."name",
    u."userName",
    u."name", -- (You have "name" twice; keep only once if not intentional)
    u."avatar",
    u."isBuilder",
    u."createdAt",
    c."codeShort",
    agg."lastSeen",
    agg."totalCount"
FROM
    agg
    JOIN brickd."brickd_User" u ON u.id = agg."userId"
    LEFT JOIN brickd."brickd_Country" c ON c."id" = u."countryId"
ORDER BY
    agg."totalCount" DESC
LIMIT
    $2;