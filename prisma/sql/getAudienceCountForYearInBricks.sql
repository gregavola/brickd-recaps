SELECT
    count(u.id) as "totalCount"
from
    "brickd"."brickd_YearInBrickUser"
    INNER JOIN "brickd"."brickd_User" as u on u.id = "brickd"."brickd_YearInBrickUser"."userId"
WHERE
    "reportId" = $1
    and u."enableCommunicationEmails" = 1
    and "brickd"."brickd_YearInBrickUser"."status" IN ('CREATED', 'QUEUED')
    and "brickd"."brickd_YearInBrickUser"."emailResponse" IS NULL