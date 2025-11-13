SELECT
    count(u.id) as "totalCount"
from
    "brickd"."brickd_UserRecapReportAudience"
    INNER JOIN "brickd"."brickd_User" as u on u.id = "brickd"."brickd_UserRecapReportAudience"."userId"
    INNER JOIN "brickd"."brickd_UserRecap" on (
        "brickd"."brickd_UserRecap"."userId" = "brickd"."brickd_UserRecapReportAudience"."userId"
        and "brickd"."brickd_UserRecap"."reportId" = $1
    )
WHERE
    "brickd"."brickd_UserRecapReportAudience"."reportId" = $1
    and u."enableCommunicationEmails" = 1
    and "brickd"."brickd_UserRecap"."status" IN ('CREATED', 'QUEUED')
    and "brickd"."brickd_UserRecap"."emailResponse" IS NULL