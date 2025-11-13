SELECT
    u.id,
    u."uuid",
    ura."createdAt",
    u."userName",
    u."enableCommunicationEmails"
FROM
    "brickd"."brickd_UserRecapReportAudience" AS ura
    JOIN "brickd"."brickd_User" AS u ON u."id" = ura."userId"
    JOIN "brickd"."brickd_UserRecap" AS ucr ON ucr."userId" = ura."userId"
    AND ucr."reportId" = ura."reportId"
WHERE
    ura."reportId" = $1
    AND ucr."status" <> 'COMPLETE'
ORDER by
    u.id
OFFSET
    $2
LIMIT
    $3