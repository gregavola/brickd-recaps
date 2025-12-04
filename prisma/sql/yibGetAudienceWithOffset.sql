SELECT
    a.*,
    "brickd"."brickd_User"."userName",
    "brickd"."brickd_User"."id" as "userId"
from
    "brickd"."brickd_UserRecapReportAudience" as a
    INNER JOIN "brickd"."brickd_User" on "brickd"."brickd_User".id = a."userId"
WHERE
    "reportId" = $1
ORDER By
    a."userId"
OFFSET
    $2
LIMIT
    $3