SELECT
    u.id,
    u."uuid",
    ura."createdAt",
    u."userName",
    u."enableCommunicationEmails"
FROM
    "brickd"."brickd_UserRecapReportAudience" AS ura
    JOIN "brickd"."brickd_User" AS u ON u."id" = ura."userId"
    JOIN "brickd"."brickd_YearInBrickUser" AS yibu ON yibu."userId" = ura."userId"
    AND yibu."reportId" = ura."reportId"
WHERE
    ura."reportId" = $1
    AND yibu."status" <> 'COMPLETE'
ORDER by
    u.id
OFFSET
    $2
LIMIT
    $3