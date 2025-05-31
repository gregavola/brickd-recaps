SELECT
    "brickd"."brickd_UserActivity"."activityType",
    count("brickd"."brickd_UserActivity"."id") as "totalCount"
from
    "brickd"."brickd_UserActivity"
WHERE
    "brickd"."brickd_UserActivity"."userId" = $1
    AND "brickd"."brickd_UserActivity"."createdAt" >= $2
    and "brickd"."brickd_UserActivity"."createdAt" <= $3
group by
    "brickd"."brickd_UserActivity"."activityType"