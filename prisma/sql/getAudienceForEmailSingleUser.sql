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
    and u.id = $2