SELECT
    a."id",
    (
        SELECT
            count(distinct "ipAddress")
        from
            "brickd"."brickd_Views"
        where
            "activityId" = a."id"
            and "isBot" = 0
    ) as "totalViews",
    (
        SELECT
            count(*)
        from
            "brickd"."brickd_UserLike"
        where
            "activityId" = a."id"
    ) as "totalLikes",
    (
        SELECT
            count(*)
        from
            "brickd"."brickd_UserComment"
        where
            "activityId" = a."id"
    ) as "totalComments",
    (
        SELECT
            count(*)
        from
            "brickd"."brickd_UserCommentLikes"
            inner join "brickd"."brickd_UserComment" on "brickd"."brickd_UserComment"."id" = "brickd"."brickd_UserCommentLikes"."commentId"
        where
            "activityId" = a."id"
    ) as "totalCommentLikes"
FROM
    "brickd"."brickd_UserActivity" AS a
WHERE
    a."createdAt" >= $2
    and a."createdAt" <= $3
    and a."userId" = $1
GROUP BY
    1
ORDER BY
    "totalLikes" DESC,
    "totalComments" DESC,
    "totalCommentLikes" DESC,
    "totalViews" DESC
LIMIT
    $4