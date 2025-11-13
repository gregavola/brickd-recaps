SELECT
    count(u.id) as "totalCount"
from
    "brickd"."brickd_UserRecapReportAudience"
    INNER JOIN "brickd"."brickd_User" as u on u.id = "brickd"."brickd_UserRecapReportAudience"."userId"
    INNER JOIN "brickd"."brickd_YearInBrickUser" on (
        "brickd"."brickd_YearInBrickUser"."userId" = "brickd"."brickd_UserRecapReportAudience"."userId"
        and "brickd"."brickd_YearInBrickUser"."reportId" = $1
    )
WHERE
    "brickd"."brickd_UserRecapReportAudience"."reportId" = $1
    and u."enableCommunicationEmails" = 1
    and "brickd"."brickd_YearInBrickUser"."status" IN ('CREATED', 'QUEUED')
    and "brickd"."brickd_YearInBrickUser"."emailResponse" IS NULL