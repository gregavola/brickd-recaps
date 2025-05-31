SELECT
  (
    SELECT COUNT(*)
    FROM brickd."brickd_UserActivity" AS ua
    WHERE ua."userId" = $1
      AND ua."createdAt" BETWEEN $2 AND $3
  ) AS user_activity_count,

  (
    SELECT COUNT(*)
    FROM brickd."brickd_UserLike" AS ul
    WHERE ul."userId" = $1
      AND ul."createdAt" BETWEEN $2 AND $3
  ) AS user_like_count,

  (
    SELECT COUNT(*)
    FROM brickd."brickd_UserCommentLikes" AS ucl
    WHERE ucl."userId" = $1
      AND ucl."createdAt" BETWEEN $2 AND $3
  ) AS user_comment_like_count,

  (
    SELECT COUNT(*)
    FROM brickd."brickd_UserComment" AS uc
    WHERE uc."userId" = $1
      AND uc."createdAt" BETWEEN $2 AND $3
  ) AS user_comment_count,

  (
    SELECT COUNT(*)
    FROM brickd."brickd_UserCollectionItemNote" AS ucin
    JOIN brickd."brickd_UserCollectionItem" AS uci
      ON uci."id" = ucin."collectionItemId"
    JOIN brickd."brickd_UserCollection" AS uc
      ON uc."id" = uci."collectionId"
    WHERE ucin."userId" = $1
      AND ucin."createdAt" BETWEEN $2 AND $3
      AND uc."isTestingCollection" = 0
  ) AS user_collection_item_note_count,

  (
    SELECT COUNT(*)
    FROM brickd."brickd_UserCollectionItemMedia" AS ucim
    JOIN brickd."brickd_UserCollectionItem" AS uci
      ON uci."id" = ucim."collectionItemId"
    JOIN brickd."brickd_UserCollection" AS uc
      ON uc."id" = uci."collectionId"
    WHERE uci."userId" = $1
      AND ucim."createdAt" BETWEEN $2 AND $3
      AND uc."isTestingCollection" = 0
  ) AS user_collection_item_media_count,

  (
    SELECT COUNT(*)
    FROM brickd."brickd_UserCollectionItem" AS uci
    JOIN brickd."brickd_UserCollection" AS uc
      ON uc."id" = uci."collectionId"
    WHERE uci."userId" = $1
      AND uci."createdAt" BETWEEN $2 AND $3
      AND uc."isWishList" = 0
      AND uc."isTestingCollection" = 0
  ) AS user_collection_item_count,

  (
    SELECT COUNT(*)
    FROM brickd."brickd_UserCollection" AS uc
    WHERE uc."userId" = $1
      AND uc."createdAt" BETWEEN $2 AND $3
      AND uc."isWishList" = 1
      AND uc."isTestingCollection" = 0
  ) AS user_collection_count;