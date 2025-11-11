SELECT
    count(u.id) as "totalCount"
from
    "brickd"."brickd_UserRecap"
    INNER JOIN "brickd"."brickd_User" as u on u.id = "brickd"."brickd_UserRecap"."userId"
WHERE
    "reportId" = $1
    and u."enableCommunicationEmails" = 1
    and "brickd"."brickd_UserRecap"."status" IN ('CREATED', 'QUEUED')
    and "brickd"."brickd_UserRecap"."emailResponse" IS NULL