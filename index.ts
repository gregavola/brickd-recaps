import { Context, SQSBatchResponse, SQSRecord } from "aws-lambda";
import { DateTime } from "luxon";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import {
  getAudienceCount,
  getAudienceForMonthlyRecapsWithOffset,
  getAudienceForMonthlyRecapTest,
  getGlobalBuiltStats,
  getTopUserActivity,
  getTotalPieceCountForUserWithRange,
  getUnqiueLocationCountForUser,
  getUserRecapStandardStats,
  getUserStreakThisMonth,
  getUserTopActivityTypesByDate,
  getUserTopLocations,
  getUserTopMediaTypesByDates,
  getUserTopThemeCount,
  getUserTopThemes,
  yibBigNumbersByUser,
  yibDiscussionEngagement,
  yibGetAudienceTest,
  yibGetAudienceWithOffset,
  yibGetSetsAndPieceCountByMonth,
  yibGetUserTopThemes,
  yibMinifiguresByUser,
  yibOverallEngagement,
  yibTopUserHourDay,
  yibTotalSetRetailValue,
  yibUserBadges,
  yibUserTopLocations,
  yibUserTotalMedia,
  yibWishListAdds,
} from "@prisma/client/sql";
import prisma from "./db";
import s3 from "./s3";
import sqs from "./sqs";
import {
  GetObjectCommand,
  ObjectCannedACL,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { LoopsClient } from "loops";
import {
  CollectionItemMediaType,
  DiscussionPostType,
  Prisma,
} from "@prisma/client";
import { JsonArray } from "@prisma/client/runtime/library";
import { SendMessageCommand } from "@aws-sdk/client-sqs";
import { Redis, SetCommandOptions } from "@upstash/redis";

// types

export interface Set {
  uuid: string;
  name: string;
  year: string;
  setNumber: string;
  setImageUrl: string;
  customSetImageUrl: string | null;
  numberOfParts: number;
  theme: { id: number; name: string } | null;
}

export interface CollectionItem {
  set: Set;
  notes: string | null;
}

export interface MinifigInfo {
  uuid: string;
  figureNumber: string;
  figureImageUrl: string;
  slug: string | null;
  name: string;
  shortName: string | null;
  description: string | null;
}

export interface CollectionMinfigItem {
  uuid: string;
  createdAt: Date | string;
  addedAt: string | Date | null;
  shortTag: string | null;
  quantity: number;
  minifig: MinifigInfo;
}

export interface CollectionMedia {
  uuid: string;
  mediaType: CollectionItemMediaType;
  mediaUrl: string;
  mediaSource: "CLOUDINARY" | "S3";
  mediaAltKey: string | null;
  mediaThumbnailKey?: string | null;
  mediaKey: string | null;
  createdAt: string | Date;
}

export interface YIBActivity {
  uuid: string;
  shortTag: string | null;
  createdAt: Date | string;
  likes: number;
  views: number;
  comments: number;
  collectionItem: CollectionItem | null;
  collectionMinifig?: CollectionMinfigItem | null;
  currentMedia: CollectionMedia | null;
  parentMedia: CollectionMedia[] | null;
}

export interface DiscussionCategory {
  uuid: string;
  name: string;
  bgColor: string | null;
}

export interface YIBDiscussion {
  uuid: string;
  title: string;
  message: string | null;
  shortTag: string;
  category: DiscussionCategory | null;
  comments: number;
  views: number;
  discussionType: DiscussionPostType;
  link: string | null;
  imageUri: string | null;
  mediaAltKey: string | null;
  createdAt: Date | string;
  updatedAt: Date | string;
  likes: number;
  discussionMedia: string[];
}

interface YearInBricks {
  updatedAt: Date | string;
  uuid: string;
  user: {
    userName: string;
    avatar: string;
    uuid: string;
    isBuilder: number;
  };
  numbers?: {
    addedToList?: number;
    collectionsCreated?: number;
    setsBuilt: number;
    piecesBuilt: number;
    global: GlobalStats;
    byMonth: {
      date: string;
      setCount: number;
      pieceCount: number;
    }[];
    setValue?: {
      currency: string;
      countryCode: string;
      vendorSets: number;
      totalPrice: number;
    };
    byDayHour?: {
      topHour12: string;
      topHour24: string;
      topDayOfWeek: string;
      count: number;
    };
  };
  places?: {
    count: number;
    locations: {
      name: string;
      address: string | null;
      isOnline: number;
      url: string | null;
      count: number;
    }[];
  };
  themes?: {
    totalThemeCount: number;
    topTheme: {
      name: string;
      count: number;
      sets: {
        name: string;
        image: string;
      }[];
    };
    topThemes: {
      name: string;
      count: number;
      sets: {
        name: string;
        image: string;
      }[];
    }[];
  };
  minifigures?: {
    count: number;
    exclusiveCount: number;
    images: string[];
  };
  wishlist?: {
    setsInWishList: number;
    setsAcquired: number;
  };
  engagement?: {
    topPost: YIBActivity | null;
    likes: number;
    comments: number;
    views: number;
    followers: number;
  };
  discussions?: {
    count: number;
    topPost: YIBDiscussion | null;
    likes: number;
    comments: number;
    views: number;
  };
  media?: {
    count: number;
    views: number;
    topPosts: {
      url: string;
      count: number;
    }[];
  };
  badges?: {
    count: number;
    badgeImageUrls: string[];
    topLevels: {
      name: string;
      level: string;
      imageUrl: string;
    }[];
  };
  summary?: {
    setsBuilt: number;
    piecesBuilt: number;
    topThemes: {
      name: string;
      count: number;
    }[];
    topLevels: {
      name: string;
      level: string;
      imageUrl: string;
    }[];
  };
}

export interface GlobalStats {
  totalPieces: number;
  totalUsers: number;
  totalSets: number;
  totalDistinctSets: number;
}

const uploadUserRecaps = async ({
  data,
  key,
  isYIB,
}: {
  data: string;
  key: string;
  isYIB: number;
}) => {
  // file name

  const uploadParams = {
    Bucket: isYIB === 1 ? "brickd-yib" : "brickd-user-recaps",
    Key: key,
    Body: data,
    ContentType: "application/json",
    CacheControl: "max-age=2628000",
    ACL: ObjectCannedACL.private,
  };

  const response = await s3.send(new PutObjectCommand(uploadParams));

  return response;
};

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || "",
  token: process.env.UPSTASH_REDIS_REST_TOKEN || "",
});

const upstash = {
  get: async (key: string) => {
    return await redis.get(key);
  },
  set: async (key: string, data: any, opts?: SetCommandOptions) => {
    return await redis.set(key, data, opts);
  },
  del: async (key: string) => {
    return await redis.del(key);
  },
  smismember: async (key: string, members: string[]) => {
    return await redis.smismember(key, members);
  },
};

const getCloudFrontSetImage = (
  setNumber: string,
  customImageUrl: string | null,
  imageUrl: string
) => {
  if (!customImageUrl) {
    return imageUrl;
  }

  return `https://d1vkkzx0dvroko.cloudfront.net/${setNumber}/main.jpg`;
};

export const chunk = (arr: Array<any>, chunkSize: number) => {
  const chunks = [];
  for (let i = 0; i < arr.length; i += chunkSize) {
    const chunk = arr.slice(i, i + chunkSize);
    chunks.push(chunk);
  }

  return chunks;
};

const getYearInBricksReview = async ({
  userId,
  start,
  end,
  stats,
}: {
  userId: number;
  start: string;
  end: string;
  stats: GlobalStats;
}): Promise<YearInBricks> => {
  dayjs.extend(utc);
  dayjs.extend(timezone);

  const timeZone = await prisma.brickd_User.findFirst({
    select: {
      timeZone: true,
    },
    where: {
      id: userId,
    },
  });

  if (!timeZone) {
    throw new Error("User Not Found");
  }

  const userStartTime = DateTime.fromISO(start, { zone: "utc" }).setZone(
    timeZone.timeZone || "Etc/GMT",
    { keepLocalTime: true }
  );
  const userEndTime = DateTime.fromISO(end, { zone: "utc" }).setZone(
    timeZone.timeZone || "Etc/GMT",
    { keepLocalTime: true }
  );

  console.log(
    `User Local Start Time: ${userStartTime.toFormat(
      "yyyy-MM-dd HH:mm:ss ZZZZ"
    )}`
  );
  console.log(
    `User Local End Time: ${userEndTime.toFormat("yyyy-MM-dd HH:mm:ss ZZZZ")}`
  );

  const startDate = userStartTime.toJSDate();
  const endDate = userEndTime.toJSDate();

  console.log("Prisma Start Date (UTC):", startDate);
  console.log("Prisma End Date (UTC):", endDate);

  const user = await prisma.brickd_User.findFirst({
    select: {
      uuid: true,
      name: true,
      userName: true,
      avatar: true,
      isBuilder: true,
      createdAt: true,
    },
    where: {
      id: userId,
    },
  });

  if (!user) {
    throw new Error("Invalid User");
  }

  let yearinBricks: YearInBricks = {
    uuid: "",
    updatedAt: new Date(),
    user: {
      userName: user.userName,
      uuid: user.uuid,
      avatar: user.avatar,
      isBuilder: user.isBuilder,
    },
  };

  const globalStats = stats;

  // Numbers
  const bigNumbers = await prisma.$queryRawTyped(
    yibBigNumbersByUser(userId, startDate, endDate)
  );

  if (bigNumbers.length !== 0) {
    const item = bigNumbers[0];

    if (item.total_sets_added !== 0) {
      const bigNumbersByMonth = await prisma.$queryRawTyped(
        yibGetSetsAndPieceCountByMonth(userId, startDate, endDate)
      );

      const setValue = await prisma.$queryRawTyped(
        yibTotalSetRetailValue(userId, startDate, endDate)
      );

      const topDayHour = await prisma.$queryRawTyped(
        yibTopUserHourDay(userId, startDate, endDate)
      );

      yearinBricks.numbers = {
        addedToList: Number(item.total_sets_added),

        byMonth: bigNumbersByMonth.map((subItem) => {
          return {
            date: subItem.month,
            setCount: Number(subItem.set_count),
            pieceCount: Number(subItem.parts_sum),
          };
        }),
        ...(item.total_collections_created !== 0 && {
          collectionsCreated: Number(item.total_collections_created),
        }),
        ...(item.total_sets_built !== 0 && {
          setsBuilt: Number(item.total_sets_built),
        }),
        piecesBuilt: Number(item.total_parts_built),
        global: globalStats,
        ...(setValue.length !== 0 && {
          setValue: {
            vendorSets: Number(setValue[0].vendorSets),
            currency: setValue[0].currency,
            countryCode: setValue[0].countryCode,
            totalPrice: Number(setValue[0].totalPrice),
          },
        }),
        ...(topDayHour.length !== 0 && {
          byDayHour: {
            topDayOfWeek: topDayHour[0].top_day_of_week,
            count: Number(topDayHour[0].hour_activity_count),
            topHour12: topDayHour[0].hour_range_12,
            topHour24: topDayHour[0].hour_range_24,
          },
        }),
      };
    }
  }

  // places
  const places = await prisma.$queryRawTyped(
    yibUserTopLocations(userId, startDate, endDate, 10)
  );

  if (places.length !== 0 && Number(places[0].total_unique_locations) >= 1) {
    yearinBricks.places = {
      count: Number(places[0].total_unique_locations),
      locations: (places[0].locations as Prisma.JsonArray).map((subItem) => {
        return {
          name: subItem["name"] as string,
          count: subItem["count"] as number,
          isOnline: subItem["isOnline"] as number,
          url: subItem["url"] ? (subItem["url"] as string) : null,
          address: subItem["fullAddress"]
            ? (subItem["fullAddress"] as string)
            : null,
        };
      }),
    };
  }
  // Themes
  const themes = await prisma.$queryRawTyped(
    yibGetUserTopThemes(userId, startDate, endDate, 10)
  );

  if (themes.length > 1) {
    yearinBricks.themes = {
      totalThemeCount: 0,
      topTheme: {
        name: themes[0].name,
        count: Number(themes[0].totalCount),
        sets: (themes[0].images as Prisma.JsonArray).map((image) => {
          return {
            image: getCloudFrontSetImage(
              image["setNumber"] as string,
              image["customImageUrl"],
              image["setImageUrl"]
            ),
            name: image["name"],
          };
        }),
      },
      topThemes: themes.map((subItem) => {
        return {
          name: subItem.name,
          count: Number(subItem.totalCount),
          sets: (subItem.images as Prisma.JsonArray).map((image) => {
            return {
              image: getCloudFrontSetImage(
                image["setNumber"] as string,
                image["customImageUrl"],
                image["setImageUrl"]
              ),
              name: image["name"],
            };
          }),
        };
      }),
    };
  }

  const minifigures = await prisma.$queryRawTyped(
    yibMinifiguresByUser(userId, startDate, endDate)
  );

  if (minifigures.length !== 0 && minifigures[0].unique_minifigs_added >= 1) {
    const medias = minifigures[0].image_urls as Prisma.JsonArray;

    yearinBricks.minifigures = {
      count: Number(minifigures[0].unique_minifigs_added),
      exclusiveCount: Number(minifigures[0].exclusive_minifigs_added),
      images: medias.map((subItem) => {
        return subItem as string;
      }),
    };
  }

  const wishlist = await prisma.$queryRawTyped(
    yibWishListAdds(userId, startDate, endDate)
  );

  if (wishlist.length !== 0 && wishlist[0].total_wishlist_added >= 1) {
    yearinBricks.wishlist = {
      setsAcquired: Number(wishlist[0].total_deleted_count),
      setsInWishList: Number(wishlist[0].total_wishlist_added),
    };
  }

  let yibActivity: YIBActivity | null = null;
  const engagement = await prisma.$queryRawTyped(
    yibOverallEngagement(userId, startDate, endDate)
  );

  if (engagement.length !== 0 && engagement[0].top_activity_id) {
    const data = await prisma.brickd_UserActivity.findFirst({
      select: {
        _count: {
          select: {
            likes: {
              where: {
                createdAt: {
                  gte: startDate,
                  lte: endDate,
                },
              },
            },
            activityViews: {
              where: {
                createdAt: {
                  gte: startDate,
                  lte: endDate,
                },
              },
            },
            comments: {
              where: {
                createdAt: {
                  gte: startDate,
                  lte: endDate,
                },
              },
            },
          },
        },
        uuid: true,
        shortTag: true,
        createdAt: true,
        currentMedia: {
          select: {
            uuid: true,
            mediaKey: true,
            mediaType: true,
            mediaUrl: true,
            mediaThumbnailKey: true,
            mediaSource: true,
            mediaAltKey: true,
            createdAt: true,
          },
        },
        currentBuildNote: {
          select: {
            uuid: true,
            status: true,
            createdAt: true,
            duration: true,
            bagNumber: true,
            notes: true,
            shortTag: true,
            media: {
              select: {
                uuid: true,
                mediaKey: true,
                mediaType: true,
                mediaThumbnailKey: true,
                mediaSource: true,
                mediaUrl: true,
                mediaAltKey: true,
                createdAt: true,
              },
              orderBy: [{ position: "asc" }, { createdAt: "desc" }],
              take: 10,
            },
          },
        },
        parentMedia: {
          select: {
            medias: {
              select: {
                media: {
                  select: {
                    uuid: true,
                    mediaKey: true,
                    mediaSource: true,
                    mediaType: true,
                    mediaThumbnailKey: true,
                    mediaUrl: true,
                    mediaAltKey: true,
                    createdAt: true,
                  },
                },
              },
            },
          },
        },
        collectionItem: {
          select: {
            notes: true,
            set: {
              select: {
                uuid: true,
                customSetImageUrl: true,
                name: true,
                setNumber: true,
                setImageUrl: true,
                isRetired: true,
                numberOfParts: true,
                year: true,
                theme: {
                  select: { id: true, name: true },
                },
              },
            },
          },
        },
        collectionMinifig: {
          select: {
            uuid: true,
            createdAt: true,
            shortTag: true,
            quantity: true,
            notes: true,
            addedAt: true,
            minifig: {
              select: {
                uuid: true,
                name: true,
                slug: true,
                shortName: true,
                description: true,
                figureImageUrl: true,
                figureNumber: true,
              },
            },
          },
        },
      },
      where: {
        id: engagement[0].top_activity_id,
      },
    });

    if (data) {
      const temp = data;
      const countData = temp._count;

      const { _count, ...b } = data;

      let parentMedia: CollectionMedia[] | null = null;

      if (data.parentMedia) {
        parentMedia = data.parentMedia.medias.map((subItem) => {
          return subItem.media;
        });
      }

      yibActivity = {
        ...b,
        parentMedia,
        collectionItem: temp.collectionItem || null,
        collectionMinifig: temp.collectionMinifig,
        likes: countData.likes,
        comments: countData.comments,
        views: countData.activityViews,
      };
    }
  }

  if (engagement.length !== 0) {
    yearinBricks.engagement = {
      topPost: yibActivity,
      comments: Number(engagement[0].comments_received),
      likes: Number(engagement[0].likes_received),
      followers: Number(engagement[0].followers_gain),
      views: Number(engagement[0].views_received),
    };
  }

  let yibDiscussion: YIBDiscussion | null = null;
  const discussions = await prisma.$queryRawTyped(
    yibDiscussionEngagement(userId, startDate, endDate)
  );

  if (discussions.length !== 0 && discussions[0].top_discussion_id) {
    const data = await prisma.brickd_Discussion.findFirst({
      select: {
        _count: {
          select: {
            discussionLike: {
              where: {
                createdAt: {
                  gte: startDate,
                  lte: endDate,
                },
              },
            },
            discussionViews: {
              where: {
                createdAt: {
                  gte: startDate,
                  lte: endDate,
                },
              },
            },
            discussionMessages: {
              where: {
                createdAt: {
                  gte: startDate,
                  lte: endDate,
                },
              },
            },
          },
        },
        title: true,
        link: true,
        updatedAt: true,
        imageUri: true,
        mediaAltKey: true,
        uuid: true,
        shortTag: true,
        createdAt: true,
        category: {
          select: {
            uuid: true,
            name: true,
            bgColor: true,
          },
        },
        message: true,
        discussionType: true,
        discussionMedia: {
          select: {
            mediaAltKey: true,
          },
          orderBy: {
            position: "asc",
          },
        },
      },
      where: {
        id: discussions[0].top_discussion_id,
      },
    });

    if (data) {
      const temp = data;
      const countData = temp._count;

      const { _count, ...b } = data;

      yibDiscussion = {
        ...b,
        discussionMedia: b.discussionMedia.map((subItem) => {
          return `https://d3g3b82j64lfzs.cloudfront.net/${subItem.mediaAltKey}`;
        }),
        likes: countData.discussionLike,
        comments: countData.discussionMessages,
        views: countData.discussionViews,
      };
    }
  }

  if (discussions.length !== 0) {
    yearinBricks.discussions = {
      topPost: yibDiscussion,
      count: Number(discussions[0].discussions_created),
      comments: Number(discussions[0].comments_on_your_discussions),
      likes: Number(discussions[0].likes_received_total),
      views: Number(discussions[0].discussion_views_received),
    };
  }

  const media = await prisma.$queryRawTyped(
    yibUserTotalMedia(userId, startDate, endDate)
  );

  if (media.length !== 0) {
    const medias = media[0].top_media as Prisma.JsonArray;

    if (medias.length !== 0) {
      yearinBricks.media = {
        topPosts: medias.map((subItem) => {
          return {
            url: `https://d3g3b82j64lfzs.cloudfront.net/${
              subItem["mediaAltKey"] as string
            }`,
            count: subItem["views"] as number,
          };
        }),
        count: Number(media[0].total_media),
        views: Number(media[0].total_media_views),
      };
    }
  }

  const badges = await prisma.$queryRawTyped(
    yibUserBadges(userId, startDate, endDate)
  );

  if (badges.length !== 0 && Number(badges[0].users_earned) !== 0) {
    const topBadges = badges[0].top_challenges as JsonArray;
    const badgeArtwork = badges[0].last_25_badge_image_urls as JsonArray;

    if (topBadges.length !== 0) {
      yearinBricks.badges = {
        topLevels: topBadges.map((subItem) => {
          return {
            name: subItem["name"] as string,
            level: subItem["levels_earned"] as string,
            imageUrl: subItem["imageUrl"] as string,
          };
        }),
        count: Number(badges[0].users_earned),
        badgeImageUrls: badgeArtwork.map((subItem) => {
          return subItem as string;
        }),
      };
    }
  }

  return yearinBricks;
};

const getUserRecaps = async ({
  userId,
  start,
  end,
  stats,
  mediaCount,
}: {
  userId: number;
  start: string;
  end: string;
  stats: GlobalStats;
  mediaCount: number;
}): Promise<any> => {
  dayjs.extend(utc);
  dayjs.extend(timezone);

  const timeZone = await prisma.brickd_User.findFirst({
    select: {
      timeZone: true,
    },
    where: {
      id: userId,
    },
  });

  console.log(`Start: ${new Date().toISOString()}`);

  if (!timeZone) {
    throw new Error("User Not Found");
  }

  const userStartTime = DateTime.fromISO(start, { zone: "utc" }).setZone(
    timeZone.timeZone || "Etc/GMT",
    { keepLocalTime: true }
  );
  const userEndTime = DateTime.fromISO(end, { zone: "utc" }).setZone(
    timeZone.timeZone || "Etc/GMT",
    { keepLocalTime: true }
  );

  console.log(
    `User Local Start Time: ${userStartTime.toFormat(
      "yyyy-MM-dd HH:mm:ss ZZZZ"
    )}`
  );
  console.log(
    `User Local End Time: ${userEndTime.toFormat("yyyy-MM-dd HH:mm:ss ZZZZ")}`
  );

  const startDate = userStartTime.toJSDate();
  const endDate = userEndTime.toJSDate();

  console.log("Prisma Start Date (UTC):", startDate);
  console.log("Prisma End Date (UTC):", endDate);

  const reportDate = dayjs.utc(start).format("YYYY-MM-DD");

  const user = await prisma.brickd_User.findFirst({
    select: {
      uuid: true,
      name: true,
      userName: true,
      avatar: true,
      isBuilder: true,
      createdAt: true,
    },
    where: {
      id: userId,
    },
  });

  //console.log(`brickd_User.findFirst: ${new Date().toISOString()}`);

  if (!user) {
    throw new Error("Invalid User");
  }

  const globalStats = stats;

  // Stats

  const collectionSetImages = await prisma.brickd_UserCollectionItem.findMany({
    select: {
      set: {
        select: {
          setImageUrl: true,
          customSetImageUrl: true,
        },
      },
    },
    orderBy: {
      createdAt: "desc",
    },
    take: 50,
    where: {
      userId,
      collection: {
        isTestingCollection: 0,
      },
      createdAt: {
        gte: startDate,
        lte: endDate,
      },
    },
  });

  /*console.log(
    `brickd_UserCollectionItem.findMany: ${new Date().toISOString()}`
  );*/

  // Total Sets Added
  const totalCollectionsCreated = await prisma.brickd_UserCollection.count({
    where: {
      userId,
      isTestingCollection: 0,
      createdAt: {
        gte: startDate,
        lte: endDate,
      },
    },
  });

  // console.log(`brickd_UserCollection.count: ${new Date().toISOString()}`);

  const totalCollectionTypes = await prisma.brickd_UserCollection.groupBy({
    by: "collectionType",
    _count: {
      id: true,
    },
    where: {
      userId,
      isTestingCollection: 0,
      createdAt: {
        gte: startDate,
        lte: endDate,
      },
    },
  });

  // console.log(`brickd_UserCollection.groupBy: ${new Date().toISOString()}`);

  // Minifigs

  const totalMinfigsQuanities =
    await prisma.brickd_UserCollectionMinifigItem.count({
      where: {
        userId,
      },
    });

  // console.log(
  //   `brickd_UserCollectionMinifigItem.count: ${new Date().toISOString()}`
  // );

  const totalMinifigsAdded =
    await prisma.brickd_UserCollectionMinifigItem.count({
      where: {
        userId,
        collection: {
          isTestingCollection: 0,
          isWishList: 0,
          collectionType: "MINIFIG",
        },
        createdAt: {
          gte: startDate,
          lte: endDate,
        },
      },
    });

  // console.log(
  //   `brickd_UserCollectionMinifigItem.count (total): ${new Date().toISOString()}`
  // );

  // Total Sets Added
  const totalSetsAdded = await prisma.brickd_UserCollectionItem.count({
    where: {
      userId,
      collection: {
        isTestingCollection: 0,
        isWishList: 0,
        collectionType: "SETS",
      },
      createdAt: {
        gte: startDate,
        lte: endDate,
      },
    },
  });

  // console.log(
  //   `brickd_UserCollectionItem.count (added): ${new Date().toISOString()}`
  // );

  // Total Sets Built
  const totalSetsBuilt = await prisma.brickd_UserCollectionItem.count({
    where: {
      buildStatus: "BUILT",
      userId,
      collection: {
        isTestingCollection: 0,
        isWishList: 0,
        collectionType: "SETS",
      },
      builtDate: {
        gte: startDate,
        lte: endDate,
      },
    },
  });

  // console.log(
  //   `brickd_UserCollectionItem.count (built): ${new Date().toISOString()}`
  // );

  const userStreak = await prisma.$queryRawTyped(
    getUserStreakThisMonth(userId, startDate, endDate)
  );

  // console.log(`userStreak.custom: ${new Date().toISOString()}`);

  const builtSets = await prisma.brickd_UserCollectionItem.findMany({
    select: {
      uuid: true,
      builtDate: true,
      createdAt: true,
      notes: true,
      buildStatus: true,
      quantity: true,
      user: {
        select: {
          uuid: true,
          userName: true,
          avatar: true,
        },
      },
      set: {
        select: {
          name: true,
          uuid: true,
          setNumber: true,
          setImageUrl: true,
          customSetImageUrl: true,
          isRetired: true,
          numberOfParts: true,
          year: true,
          theme: {
            select: { id: true, name: true },
          },
        },
      },
    },
    where: {
      userId,
      createdAt: {
        gte: startDate,
        lte: endDate,
      },
      collection: {
        isTestingCollection: 0,
      },
      buildStatus: "BUILT",
    },
    orderBy: [
      {
        builtDate: "desc",
      },
      {
        createdAt: "desc",
      },
    ],
    take: 5,
  });

  // console.log(
  //   `brickd_UserCollectionItem.findMany: ${new Date().toISOString()}`
  // );

  const sets = builtSets.map((subItem) => {
    subItem.set.setImageUrl = getCloudFrontSetImage(
      subItem.set.setNumber,
      subItem.set.customSetImageUrl,
      subItem.set.setImageUrl
    );

    return subItem;
  });

  // Locations

  const locationCounts = await prisma.$queryRawTyped(
    getUnqiueLocationCountForUser(userId, startDate, endDate)
  );

  // console.log(
  //   `getUnqiueLocationCountForUser.custom: ${new Date().toISOString()}`
  // );

  // const totalLocationsUnique = await prisma.brickd_UserCollectionItem.findMany({
  //   where: {
  //     userId,
  //     collection: {
  //       isTestingCollection: 0,
  //     },
  //     createdAt: {
  //       gte: startDate,
  //       lte: endDate,
  //     },
  //     locationId: { not: null },
  //   },
  //   distinct: ["locationId"],
  // });

  // const totalLocations = await prisma.brickd_UserCollectionItem.count({
  //   where: {
  //     userId,
  //     collection: {
  //       isTestingCollection: 0,
  //     },
  //     createdAt: {
  //       gte: startDate,
  //       lte: endDate,
  //     },
  //     locationId: { not: null },
  //   },
  // });

  const topLocations = await prisma.$queryRawTyped(
    getUserTopLocations(userId, startDate, endDate, 5)
  );

  // console.log(`getUserTopLocations.custom: ${new Date().toISOString()}`);

  // Total Sets Built

  const totalPieceCount = await prisma.$queryRawTyped(
    getTotalPieceCountForUserWithRange(startDate, endDate, userId)
  );

  // console.log(
  //   `getTotalPieceCountForUserWithRange.custom: ${new Date().toISOString()}`
  // );

  // const totalPieceCount = await prisma.brickd_Set.aggregate({
  //   _sum: {
  //     numberOfParts: true,
  //   },
  //   where: {
  //     collectionItems: {
  //       some: {
  //         collection: {
  //           isTestingCollection: 0,
  //         },
  //         buildStatus: "BUILT",
  //         userId,
  //         builtDate: {
  //           gte: startDate,
  //           lte: endDate,
  //         },
  //       },
  //     },
  //   },
  // });

  // Wish List

  const totalWishListCollectionsCreated =
    await prisma.brickd_UserCollection.count({
      where: {
        userId,
        isWishList: 1,
        isTestingCollection: 0,
        createdAt: {
          gte: startDate,
          lte: endDate,
        },
      },
    });

  // console.log(
  //   `brickd_UserCollection.count (wishlist): ${new Date().toISOString()}`
  // );

  const totalWishListAdded = await prisma.brickd_UserCollectionItem.count({
    where: {
      userId,
      collection: {
        isWishList: 1,
        isTestingCollection: 0,
      },
      createdAt: {
        gte: startDate,
        lte: endDate,
      },
    },
  });

  // console.log(
  //   `brickd_UserCollectionItem.count (wishlist added): ${new Date().toISOString()}`
  // );

  const totalWishListMoved = await prisma.brickd_DeleteCollectionItemLog.count({
    where: {
      userId,
      reason: "WISHLIST",
      createdAt: {
        gte: startDate,
        lte: endDate,
      },
    },
  });

  // console.log(
  //   `brickd_DeleteCollectionItemLog.count: ${new Date().toISOString()}`
  // );

  const countData = await prisma.$queryRawTyped(
    getUserRecapStandardStats(userId, startDate, endDate)
  );

  // console.log(`getUserRecapStandardStats.custom: ${new Date().toISOString()}`);

  const totalActivities =
    countData.length !== 0 ? Number(countData[0].user_activity_count || 0) : 0;

  const totalActivityLikes =
    countData.length !== 0 ? Number(countData[0].user_like_count || 0) : 0;

  const totalCommentLikes =
    countData.length !== 0
      ? Number(countData[0].user_comment_like_count || 0)
      : 0;

  const totalComments =
    countData.length !== 0 ? Number(countData[0].user_comment_count || 0) : 0;

  const totalBuildNotes =
    countData.length !== 0
      ? Number(countData[0].user_collection_item_note_count || 0)
      : 0;

  const totalMediaUploaded =
    countData.length !== 0
      ? Number(countData[0].user_collection_item_media_count || 0)
      : 0;

  const globalTotalMediaUploaded = mediaCount;

  const mediaList = await prisma.brickd_UserCollectionItemMedia.findMany({
    where: {
      collectionItem: {
        userId,
        collection: {
          isTestingCollection: 0,
        },
      },
      createdAt: {
        gte: startDate,
        lte: endDate,
      },
    },
    orderBy: {
      createdAt: "desc",
    },
    take: 50,
  });

  const totalActivityType = await prisma.$queryRawTyped(
    getUserTopActivityTypesByDate(userId, startDate, endDate)
  );

  // console.log(
  //   `getUserTopActivityTypesByDate.custom: ${new Date().toISOString()}`
  // );

  const totalMediaTypes = await prisma.$queryRawTyped(
    getUserTopMediaTypesByDates(userId, startDate, endDate)
  );

  // console.log(
  //   `getUserTopMediaTypesByDates.custom: ${new Date().toISOString()}`
  // );

  // top 5 themes

  const topThemes = await prisma.$queryRawTyped(
    getUserTopThemes(userId, startDate, endDate, 5)
  );

  // console.log(
  //   `getUserTopMediaTypesByDates.custom: ${new Date().toISOString()}`
  // );

  const topThemeTotalCount = await prisma.$queryRawTyped(
    getUserTopThemeCount(userId, startDate, endDate)
  );

  // console.log(`getUserTopThemeCount.custom: ${new Date().toISOString()}`);

  const totalSetsPerUser = globalStats.totalSets / globalStats.totalUsers;
  // top 5 viewed sets

  let activity: any | null = null;

  const topActivity = await prisma.$queryRawTyped(
    getTopUserActivity(userId, startDate, endDate, 1)
  );

  // console.log(`getTopUserActivity.custom: ${new Date().toISOString()}`);

  if (topActivity.length !== 0) {
    const data = await prisma.brickd_UserActivity.findFirst({
      select: {
        uuid: true,
        createdAt: true,
        activityType: true,
        collectionItem: {
          select: {
            media: {
              select: {
                mediaKey: true,
                mediaUrl: true,
                mediaType: true,
                mediaAltKey: true,
                mediaSource: true,
                mediaThumbnailKey: true,
              },
              orderBy: {
                createdAt: "desc",
              },
              take: 1,
            },
            set: {
              select: {
                uuid: true,
                setImageUrl: true,
                customSetImageUrl: true,
                name: true,
                slug: true,
                setNumber: true,
                numberOfParts: true,
                year: true,
                theme: {
                  select: {
                    name: true,
                    id: true,
                  },
                },
              },
            },
          },
        },
      },
      where: {
        id: topActivity[0].id,
      },
    });

    // console.log(`brickd_UserActivity.findFirst: ${new Date().toISOString()}`);

    if (data) {
      if (data.collectionItem) {
        data.collectionItem.set.setImageUrl = getCloudFrontSetImage(
          data.collectionItem.set.setNumber,
          data.collectionItem.set.customSetImageUrl,
          data.collectionItem.set.setImageUrl
        );
      }

      activity = {
        ...data,
        totalViews: Number(topActivity[0].totalViews),
        totalCommentLikes: Number(topActivity[0].totalCommentLikes),
        totalLikes: Number(topActivity[0].totalLikes),
        totalComments: Number(topActivity[0].totalComments),
      };
    }
  }

  const activityTypes = totalActivityType.map((item) => {
    return {
      name: item.activityType,
      count: Number(item.totalCount),
    };
  });

  const mediaTypes = totalMediaTypes.map((item) => {
    return {
      name: item.mediaType,
      count: Number(item.totalCount),
    };
  });

  const totalPieceCountValue: number =
    totalPieceCount.length !== 0
      ? Number(totalPieceCount[0].totalPieceCount)
      : 0;

  const totalMinifigQuantityValue: number = totalMinfigsQuanities;

  return {
    reportDate,
    user,
    dates: {
      start: startDate,
      end: endDate,
    },
    stories: {
      global: {
        totalPiecesBuilt: globalStats.totalPieces,
        totalSetsBuilt: globalStats.totalSets,
        userPercentile: parseFloat(
          ((totalPieceCountValue / globalStats.totalPieces) * 100).toFixed(2)
        ),
      },
      streaks: {
        longestStreak:
          userStreak.length !== 0
            ? Number(userStreak[0].longest_streak_length || 0)
            : 0,
        longestStreakDates:
          userStreak.length !== 0 &&
          userStreak[0].longest_streak_start &&
          userStreak[0].longest_streak_end
            ? {
                start: userStreak[0].longest_streak_start,
                end: userStreak[0].longest_streak_end,
              }
            : null,
      },
      location: {
        uniqueTotalLocations:
          locationCounts.length !== 0
            ? Number(locationCounts[0].totalDistinctCount)
            : 0,
        totalLocations:
          locationCounts.length !== 0
            ? Number(locationCounts[0].totalCount)
            : 0,
        topLocations: topLocations.map((subItem) => {
          return {
            uuid: subItem.uuid,
            name: subItem.name,
            isOnline: subItem.isOnline,
            imageUrl: subItem.imageUrl,
            city: subItem.city,
            region: subItem.region,
            fullAddress: subItem.fullAddress,
            url: subItem.url,
            country: {
              name: subItem.countryName,
              codeShort: subItem.codeShort,
            },
            totalCount: Number(subItem.totalCount),
          };
        }),
      },
      collections: {
        totalWishListCollectionsCreated,
        totalCollectionsCreated,
        collectionTypes: totalCollectionTypes.map((subItem) => {
          return {
            name: subItem.collectionType,
            count: subItem._count.id || 0,
          };
        }),
        collectionImages: collectionSetImages.map((subItem) => {
          return subItem.set.customSetImageUrl || subItem.set.setImageUrl;
        }),
      },
      themes: {
        totalThemes: Number(topThemeTotalCount[0].totalCount),
        topThemes: topThemes.map((subItem) => {
          return {
            name: subItem.name,
            count: Number(subItem.totalCount),
          };
        }),
      },
      sets: {
        totalSetsPerUser,
        totalSetsAdded,
        totalSetsBuilt,
        builtSets: sets,
        totalPieceCount: totalPieceCountValue,
        totalWeight:
          totalPieceCountValue === 0 ? null : totalPieceCountValue / 400,
      },
      minifigs: {
        totalMinifigsAdded,
        totalQuantity: totalMinifigQuantityValue,
      },
      wishlist: {
        totalAdded: totalWishListMoved + totalWishListAdded,
        totalMoved: totalWishListMoved,
      },
      media: {
        globalTotalMediaUploaded,
        totalMedia: totalMediaUploaded,
        topMediaType: mediaTypes,
        mediaList: mediaList.map((item) => {
          return {
            mediaKey: item.mediaKey,
            mediaUrl: item.mediaUrl,
            mediaType: item.mediaType,
            mediaAltKey: item.mediaAltKey,
            mediaSource: item.mediaSource,
            mediaThumbnailKey: item.mediaThumbnailKey,
          };
        }),
      },
      activities: {
        totalActivities,
        topActivityTypes: activityTypes,
        totalComments,
        totalLikes: totalActivityLikes + totalCommentLikes,
        topActivity: activity || null,
      },
      buildNotes: {
        totalBuildNotes,
        averagePerDay: totalBuildNotes / 365,
      },
    },
  };
};

const getMonthlyStats = async ({
  startDate,
  endDate,
}: {
  startDate: Date;
  endDate: Date;
}): Promise<GlobalStats> => {
  const cachedData = await upstash.get("global-yib-stats");

  if (cachedData) {
    return <GlobalStats>cachedData;
  }

  const data = await prisma.$queryRawTyped(
    getGlobalBuiltStats(startDate, endDate)
  );

  if (data.length === 0) {
    throw new Error("Empty Global");
  }

  const values = {
    totalUsers: Number(data[0].totalUsers),
    totalDistinctSets: Number(data[0].totalDistinctSets),
    totalPieces: Number(data[0].totalPieces),
    totalSets: Number(data[0].totalSets),
  };

  await upstash.set(`global-yib-stats`, values);

  return values;
};

export const sendLoopsEvent = async ({
  userId,
  eventName,
  properties,
}: {
  userId: string;
  eventName: string;
  properties?: Record<string, string | number>;
}) => {
  const loops = new LoopsClient(process.env.LOOPS_API_KEY || "");

  const data = await loops.sendEvent({
    userId,
    eventName,
    eventProperties: properties,
  });

  return data;
};

export const kickOffTasks = async ({
  reportId,
  rebuild,
}: {
  reportId: number;
  rebuild?: boolean;
}) => {
  dayjs.extend(utc);

  const data = await prisma.brickd_UserRecapReport.findFirst({
    where: { id: reportId },
  });

  if (!data) {
    throw new Error(`Invalid Report found for ${reportId}`);
  }

  let startDate = dayjs.utc(data.reportDate).startOf("month");
  let endDate = dayjs.utc(data.reportDate).endOf("month");

  if (data.isYIB === 1) {
    startDate = dayjs.utc().startOf("year");
    endDate = dayjs.utc().endOf("year");
  }

  const { isYIB, yibYear } = data;

  console.log(
    `Starting for ${startDate.toISOString()} to ${endDate.toISOString()}`
  );

  await prisma.brickd_UserRecapReport.update({
    data: {
      startTime: new Date(),
      updatedAt: new Date(),
      status: "RUNNING",
    },
    where: {
      id: reportId,
    },
  });

  const results = await prisma.$queryRawTyped(
    getAudienceCount(startDate.toDate(), endDate.toDate())
  );

  if (isYIB === 1) {
    console.log(`=== Year in Bricks for ${yibYear} ===`);
  }

  console.log(`Total Users: ${results.length}`);

  await prisma.brickd_UserRecapReport.update({
    data: {
      totalUsers: results.length,
      updatedAt: new Date(),
    },
    where: {
      id: reportId,
    },
  });

  const dataInsert = results.map((subItem) => {
    return {
      reportId,
      createdAt: new Date(),
      userId: subItem.id,
      totalSets: Number(subItem.totalSets),
      totalMinifigs: Number(subItem.totalMinifigs),
    };
  });

  const chunks = chunk(dataInsert, 300);

  for await (const chunk of chunks) {
    await prisma.brickd_UserRecapReportAudience.createMany({ data: chunk });
  }

  // 100 Per Job, that's Fine 🤷‍♂️
  const offsetKey = 100;

  const totalPages = Math.ceil(results.length / offsetKey);

  const pagesArray: number[] = [];

  for (let i = 0; i < totalPages; i++) {
    pagesArray.push(i);
  }

  console.log(`=== Total Pages: ${pagesArray.length} ===`);

  for await (const item of pagesArray) {
    const offset = item * offsetKey;
    const data = await prisma.brickd_UserRecapReportLog.create({
      data: {
        createdAt: new Date(),
        updatedAt: new Date(),
        startTime: new Date(),
        status: "QUEUED",
        offset,
        reportId,
      },
    });

    console.log(`Requested Offset: ${offset} - Page ${item}`);

    try {
      const payload = {
        offset,
        reportId,
        logId: data.id,
        batch: true,
        ...(rebuild && {
          rebuild: true,
        }),
        isYIB,
        ...(yibYear && {
          yibYear,
        }),
      };

      const command = new SendMessageCommand({
        QueueUrl:
          "https://sqs.us-east-1.amazonaws.com/726013842547/brickd-user-recaps",
        MessageBody: JSON.stringify(payload),
      });

      const results = await sqs.send(command);

      await prisma.brickd_UserRecapReportLog.update({
        data: {
          sqsIngestedAt: new Date(),
          sqsMessageId: results.MessageId,
        },
        where: {
          id: data.id,
        },
      });

      console.log(`Lambda for ${offset} - ${JSON.stringify(payload)}`);
    } catch (err) {
      console.log(`🔴 Error`);
      console.error(err);
    }
  }
};

export const getUserRecapS3 = async ({
  key,
}: {
  key: string;
}): Promise<{
  status: number;
  data: any | null;
  date: Date | string | null;
}> => {
  const fileName: string = key;

  const getParams = {
    Bucket: "brickd-user-recaps",
    Key: fileName,
  };

  const response = await s3.send(new GetObjectCommand(getParams));

  if (response && response.Body && response.LastModified) {
    const str = await response.Body.transformToString();
    return { status: 200, data: JSON.parse(str), date: response.LastModified };
  } else {
    return { status: 404, data: null, date: null };
  }
};

export const sendEmails = async ({ reportId }: { reportId: number }) => {
  dayjs.extend(utc);
  const data = await prisma.brickd_UserRecapReport.findFirst({
    where: { id: reportId },
  });

  if (!data) {
    throw new Error(`Invalid Report found for ${reportId}`);
  }

  const { isYIB } = data;

  await prisma.brickd_UserRecapReport.update({
    data: {
      status: "RUNNING",
      updatedAt: new Date(),
    },
    where: {
      id: reportId,
    },
  });

  const users = await prisma.brickd_UserRecap.findMany({
    select: {
      id: true,
      uuid: true,
      dataUrl: true,
      reportDate: true,
      user: {
        select: {
          userName: true,
          uuid: true,
          enableCommunicationEmails: true,
        },
      },
    },
    where: {
      reportId,
      status: "QUEUED",
      emailResponse: null,
    },
    orderBy: { userId: "asc" },
  });

  console.log(`Size: ${users.length}`);

  for await (const user of users) {
    console.log(`Starting with ${user.user?.userName || "Unknown"}`);

    const now = performance.now();

    try {
      if (isYIB === 1) {
        await prisma.brickd_YearInBrickUser.updateMany({
          data: {
            status: "RUNNING",
            updatedAt: new Date(),
          },
          where: {
            reportId,
            userId: user.id,
          },
        });
      } else {
        await prisma.brickd_UserRecap.update({
          data: {
            status: "RUNNING",
            statusDate: new Date(),
          },
          where: {
            uuid: user.uuid,
          },
        });
      }

      if (user.user.enableCommunicationEmails === 0) {
        if (isYIB === 1) {
          await prisma.brickd_YearInBrickUser.updateMany({
            data: {
              status: "COMPLETE",
              emailResponse: JSON.stringify({ skipped: true }),
              updatedAt: new Date(),
            },
            where: {
              reportId,
              userId: user.id,
            },
          });
        } else {
          await prisma.brickd_UserRecap.update({
            data: {
              status: "COMPLETE",
              emailResponse: JSON.stringify({ skipped: true }),
              statusDate: new Date(),
            },
            where: {
              uuid: user.uuid,
            },
          });
        }
      } else {
        const recap = await getUserRecapS3({
          key: user.dataUrl,
        });

        if (user.user && recap.data) {
          const results = await sendLoopsEvent({
            userId: user.user.uuid,
            eventName: isYIB === 1 ? "yib-recap" : "monthly-recaps",
            properties:
              isYIB === 1
                ? {
                    yibYear: data.yibYear,
                  }
                : {
                    totalSets: recap.data?.stories.sets.totalSetsAdded,
                    dateHeader: dayjs
                      .utc(recap.data?.reportDate)
                      .format("MMMM YYYY"),
                    piecesBuilt: recap.data.stories.sets.totalPieceCount,
                    totalMinifigures:
                      recap.data.stories.minifigs.totalMinifigsAdded,
                    setsBuilt: recap.data?.stories.sets.totalSetsBuilt,
                    recapUrl: `https://getbrickd.com/user-recaps/${user.uuid}?utm_source=email`,
                  },
          });

          if (results) {
            if (results.success) {
              const end = performance.now();

              if (isYIB === 1) {
                await prisma.brickd_YearInBrickUser.updateMany({
                  data: {
                    status: "COMPLETE",
                    emailResponse: JSON.stringify(results),
                    updatedAt: new Date(),
                    emailSentAt: new Date(),
                  },
                  where: {
                    reportId,
                    userId: user.id,
                  },
                });
              } else {
                await prisma.brickd_UserRecap.update({
                  data: {
                    status: "COMPLETE",
                    emailResponse: JSON.stringify(results),
                    emailTimeTaken: end - now,
                    emailSentAt: new Date(),
                  },
                  where: {
                    id: user.id,
                  },
                });
              }
            } else {
              if (isYIB === 1) {
                await prisma.brickd_YearInBrickUser.updateMany({
                  data: {
                    status: "ERROR",
                    emailResponse: JSON.stringify(results),
                    updatedAt: new Date(),
                    emailSentAt: new Date(),
                  },
                  where: {
                    reportId,
                    userId: user.id,
                  },
                });
              } else {
                await prisma.brickd_UserRecap.update({
                  data: {
                    status: "ERROR",
                    emailResponse: JSON.stringify(results),
                    emailSentAt: new Date(),
                  },
                  where: {
                    id: user.id,
                  },
                });
              }
            }
          } else {
            if (isYIB === 1) {
              await prisma.brickd_YearInBrickUser.updateMany({
                data: {
                  status: "ERROR",
                  emailResponse: "Unknown Error (Empty Response)",
                  updatedAt: new Date(),
                  emailSentAt: new Date(),
                },
                where: {
                  reportId,
                  userId: user.id,
                },
              });
            } else {
              await prisma.brickd_UserRecap.update({
                data: {
                  status: "ERROR",
                  emailResponse: "Unknown Error (Empty Response)",
                  emailSentAt: new Date(),
                },
                where: {
                  id: user.id,
                },
              });
            }
          }
        } else {
          if (isYIB === 1) {
            await prisma.brickd_YearInBrickUser.updateMany({
              data: {
                status: "ERROR",
                emailResponse: "invaid user or recap.data",
                updatedAt: new Date(),
              },
              where: {
                reportId,
                userId: user.id,
              },
            });
          } else {
            await prisma.brickd_UserRecap.update({
              data: {
                status: "ERROR",
                emailResponse: "invaid user or recap.data",
                statusDate: new Date(),
              },
              where: {
                id: user.id,
              },
            });
          }
        }
      }
    } catch (err: any) {
      console.log(err);

      if (isYIB === 1) {
        await prisma.brickd_YearInBrickUser.updateMany({
          data: {
            status: "ERROR",
            emailResponse: err.message,
            updatedAt: new Date(),
          },
          where: {
            reportId,
            userId: user.id,
          },
        });
      } else {
        await prisma.brickd_UserRecap.update({
          data: {
            status: "ERROR",
            emailResponse: err.message,
            statusDate: new Date(),
          },
          where: {
            id: user.id,
          },
        });
      }
    } finally {
      const end = performance.now();
      console.log(`Time: ${(end - now).toFixed(2)}ms`);
      console.log(`Do with ${user.user?.userName || "Unknown"}`);
    }
  }

  await prisma.brickd_UserRecapReport.update({
    data: {
      status: "JOBCOMPLETE",
      updatedAt: new Date(),
    },
    where: {
      id: reportId,
    },
  });
};

export const sendSingleEmail = async ({
  userId,
  recapId,
}: {
  userId: number;
  recapId: number;
}) => {
  const user = await prisma.brickd_UserRecap.findFirst({
    select: {
      id: true,
      uuid: true,
      dataUrl: true,
      reportDate: true,
      user: {
        select: {
          userName: true,
          uuid: true,
        },
      },
    },
    where: {
      status: "QUEUED",
      user: {
        enableCommunicationEmails: 1,
      },
      emailResponse: null,
      userId,
      id: recapId,
    },
  });

  if (!user) {
    console.log(`Invalid Recap [EMAIL] ${userId} - RecapId: ${recapId}`);
  }

  const now = performance.now();

  try {
    await prisma.brickd_UserRecap.update({
      data: {
        status: "RUNNING",
        statusDate: new Date(),
      },
      where: {
        id: user.id,
        uuid: user.uuid,
      },
    });

    const recap = await getUserRecapS3({
      key: user.dataUrl,
    });

    if (user.user && recap.data) {
      const results = await sendLoopsEvent({
        userId: user.user.uuid,
        eventName: "monthly-recaps",
        properties: {
          totalSets: recap.data?.stories.sets.totalSetsAdded,
          dateHeader: dayjs.utc(recap.data?.reportDate).format("MMMM YYYY"),
          piecesBuilt: recap.data.stories.sets.totalPieceCount,
          totalMinifigures: recap.data.stories.minifigs.totalMinifigsAdded,
          setsBuilt: recap.data?.stories.sets.totalSetsBuilt,
          recapUrl: `https://getbrickd.com/user-recaps/${user.uuid}`,
        },
      });

      if (results) {
        if (results.success) {
          const end = performance.now();

          await prisma.brickd_UserRecap.update({
            data: {
              status: "COMPLETE",
              emailResponse: JSON.stringify(results),
              emailTimeTaken: end - now,
              emailSentAt: new Date(),
            },
            where: {
              id: user.id,
            },
          });
        } else {
          await prisma.brickd_UserRecap.update({
            data: {
              status: "ERROR",
              emailResponse: JSON.stringify(results),
              emailSentAt: new Date(),
            },
            where: {
              id: user.id,
            },
          });
        }
      } else {
        await prisma.brickd_UserRecap.update({
          data: {
            status: "ERROR",
            emailResponse: "Unknown Error (Empty Response)",
            emailSentAt: new Date(),
          },
          where: {
            id: user.id,
          },
        });
      }
    } else {
      await prisma.brickd_UserRecap.update({
        data: {
          status: "ERROR",
          emailResponse: "invaid user or recap.data",
          statusDate: new Date(),
        },
        where: {
          id: user.id,
        },
      });
    }
  } catch (err: any) {
    console.log(err);

    await prisma.brickd_UserRecap.update({
      data: {
        status: "ERROR",
        emailResponse: err.message,
        statusDate: new Date(),
      },
      where: {
        id: user.id,
      },
    });
  } finally {
    const end = performance.now();
    console.log(`Time: ${(end - now).toFixed(2)}ms`);
    console.log(`Done with Email for ${user.user?.userName || "Unknown"}`);
  }
};

export const createRecapReport = async ({
  reportDate,
  isYIB,
  yibYear,
}: {
  reportDate: Date | string;
  isYIB: number;
  yibYear: number | null;
}) => {
  await prisma.brickd_UserRecapReport.create({
    data: {
      createdAt: new Date(),
      reportDate,
      isYIB,
      yibYear,
      status: "QUEUED",
    },
  });
};

export const getMostRecentRecapReport = async () => {
  const data = await prisma.brickd_UserRecapReport.findFirst({
    orderBy: {
      createdAt: "desc",
    },
    take: 1,
  });

  if (!data) {
    return null;
  }

  return { id: data.id, isYIB: data.isYIB };
};

export const getReportMedataData = async ({
  reportId,
}: {
  reportId: number;
}) => {
  const data = await prisma.brickd_UserRecapReport.findFirst({
    where: {
      id: reportId,
    },
  });

  if (!data) {
    return null;
  }

  return { id: data.id, isYIB: data.isYIB };
};

export const getReportMetadataFromId = async ({
  reportId,
}: {
  reportId: number;
}) => {
  const data = await prisma.brickd_UserRecapReport.findFirst({
    where: {
      id: reportId,
    },
  });

  if (!data) {
    return null;
  }

  return { id: data.id, isYIB: data.isYIB };
};

export const processRecap = async ({
  userId,
  offset,
  logId,
  sendEmail,
  reportId,
  rebuild,
  logName,
}: {
  userId?: number;
  offset?: number;
  logId: number | null;
  sendEmail?: boolean;
  reportId: number;
  rebuild?: boolean;
  logName?: string | null;
}) => {
  const data = await prisma.brickd_UserRecapReport.findFirst({
    where: { id: reportId },
  });

  if (!data) {
    throw new Error(`Invalid Report found for ${reportId}`);
  }

  const { reportDate } = data;

  console.log(`
    Report Date: ${reportDate.toISOString()}
    `);

  dayjs.extend(utc);

  const startDate = dayjs.utc(reportDate).startOf("month");
  const endDate = dayjs.utc(reportDate).endOf("month");

  console.log(
    `Starting for ${startDate.toISOString()} to ${endDate.toISOString()}`
  );

  const hasOffset = offset !== undefined;

  if (userId) {
    console.log(`== TEST RUN for ${userId}===`);
  }

  if (hasOffset) {
    console.log(`== OFFSET RUN for ${offset}===`);
  }

  if (logId) {
    await prisma.brickd_UserRecapReportLog.update({
      data: {
        updatedAt: new Date(),
        ...(logName && {
          logName,
        }),
        lambdaStartedAt: new Date(),
        status: "RUNNING",
      },
      where: {
        id: logId,
      },
    });
  }

  if (!userId && !hasOffset) {
    console.log("Missing UserId or Offset");
    return;
  }

  const results = hasOffset
    ? await prisma.$queryRawTyped(
        getAudienceForMonthlyRecapsWithOffset(reportId, offset!, 100)
      )
    : await prisma.$queryRawTyped(
        getAudienceForMonthlyRecapTest(
          startDate.toDate(),
          endDate.toDate(),
          reportId,
          userId
        )
      );

  const globalStats = await getMonthlyStats({
    startDate: startDate.toDate(),
    endDate: endDate.toDate(),
  });

  const globalTotalMediaUploaded =
    await prisma.brickd_UserCollectionItemMedia.count({
      where: {
        createdAt: {
          gte: startDate.toDate(),
          lte: endDate.toDate(),
        },
      },
    });

  console.log(`Global Stats`);
  console.log(JSON.stringify(globalStats));

  console.log(`Global Media Count:`);
  console.log(JSON.stringify(globalTotalMediaUploaded));

  let recap: any | null = null;
  let offsetValue: number = offset || 0;

  if (logId) {
    await prisma.brickd_UserRecapReportLog.update({
      data: {
        updatedAt: new Date(),
        totalUsers: results.length,
      },
      where: {
        id: logId,
      },
    });
  }

  const dateKey = startDate.format("MM_YY");

  console.log(`REPORT DATE: ${dateKey}`);

  for await (const user of results) {
    const now = performance.now();

    console.log(
      `== [${user.userId}] Starting with ${user.userName} - ${user.totalSets} sets ===`
    );

    console.log(`Start with Query: ${new Date().toISOString()}`);

    const start = performance.now();

    const data = await getUserRecaps({
      userId: user.userId,
      start: startDate.toISOString(),
      end: endDate.toISOString(),
      stats: globalStats,
      mediaCount: globalTotalMediaUploaded,
    });

    recap = data;

    const mediaKey = `${dateKey}/${user.uuid}.json`;

    console.log(`Done with Query: ${new Date().toISOString()}`);

    const end = performance.now();

    console.log(`Time: ${end - start}ms`);

    await uploadUserRecaps({
      data: JSON.stringify(data),
      key: mediaKey,
      isYIB: 0,
    });

    console.log(`S3 Upload Complete: ${new Date().toISOString()}`);

    const later = performance.now();

    const timeDiff = (later - now).toFixed(3);

    console.log(`Full Time: ${timeDiff}ms`);

    let recapId: number | null = null;

    const id = await prisma.brickd_UserRecap.findFirst({
      where: {
        userId: user.userId,
        reportDate: startDate.toDate(),
      },
    });

    if (!id) {
      const response = await prisma.brickd_UserRecap.create({
        data: {
          userId: user.userId,
          reportDate: startDate.toDate(),
          reportId,
          timeTaken: parseFloat(timeDiff),
          createdAt: new Date(),
          updatedAt: new Date(),
          dataUrl: mediaKey,
        },
      });

      recapId = response.id;
    } else {
      console.log("UPDATING");
      recapId = id.id;
      await prisma.brickd_UserRecap.updateMany({
        data: {
          updatedAt: new Date(),
          dataUrl: mediaKey,
          timeTaken: parseFloat(timeDiff),
        },
        where: {
          id: id.id,
          userId: user.userId,
        },
      });
    }

    if (sendEmail) {
      await sendSingleEmail({ userId: user.userId, recapId });
    }

    if (logId && offsetValue) {
      await prisma.brickd_UserRecapReportLog.update({
        data: {
          updatedAt: new Date(),
          currentOffset: offsetValue,
        },
        where: {
          id: logId,
        },
      });

      offsetValue++;
    }
  }

  if (logId) {
    await prisma.brickd_UserRecapReportLog.update({
      data: {
        updatedAt: new Date(),
        endTime: new Date(),
        status: "JOBCOMPLETE",
      },
      where: {
        id: logId,
      },
    });

    const count = await prisma.brickd_UserRecapReportLog.count({
      where: {
        reportId,
        offset: { gt: offset },
      },
    });

    if (count === 0) {
      await prisma.brickd_UserRecapReport.update({
        data: {
          status: "COMPLETE",
          endTime: new Date(),
          updatedAt: new Date(),
        },
        where: {
          id: reportId,
        },
      });
    }
  }
};

export const processYearInBricks = async ({
  userId,
  offset,
  logId,
  sendEmail,
  reportId,
  rebuild,
  logName,
}: {
  userId?: number;
  offset?: number;
  logId: number | null;
  sendEmail?: boolean;
  reportId: number;
  rebuild?: boolean;
  logName?: string | null;
}) => {
  const data = await prisma.brickd_UserRecapReport.findFirst({
    where: { id: reportId },
  });

  if (!data) {
    throw new Error(`Invalid Report found for ${reportId}`);
  }

  const { yibYear } = data;

  if (logId) {
    await prisma.brickd_UserRecapReportLog.update({
      data: {
        updatedAt: new Date(),
        ...(logName && {
          logName,
        }),
        lambdaStartedAt: new Date(),
        status: "RUNNING",
      },
      where: {
        id: logId,
      },
    });
  }

  console.log(`Year In Bricks Report: ${yibYear}`);

  dayjs.extend(utc);

  const startDate = dayjs.utc().startOf("year");
  const endDate = dayjs.utc().endOf("year");

  console.log(
    `Starting for ${startDate.toISOString()} to ${endDate.toISOString()}`
  );

  const hasOffset = offset !== undefined;

  if (userId) {
    console.log(`== TEST RUN for ${userId}===`);
  }

  if (hasOffset) {
    console.log(`== OFFSET RUN for ${offset}===`);
  }

  if (!hasOffset && !userId) {
    console.log(`Missing Offset or UserId`);
    return null;
  }

  const results = hasOffset
    ? await prisma.$queryRawTyped(
        yibGetAudienceWithOffset(reportId, offset!, 100)
      )
    : await prisma.$queryRawTyped(
        yibGetAudienceTest(
          startDate.toDate(),
          endDate.toDate(),
          reportId,
          userId
        )
      );

  const globalStats = await getMonthlyStats({
    startDate: startDate.toDate(),
    endDate: endDate.toDate(),
  });

  if (logId) {
    await prisma.brickd_UserRecapReportLog.update({
      data: {
        updatedAt: new Date(),
        totalUsers: results.length,
      },
      where: {
        id: logId,
      },
    });
  }

  console.log(`Global Stats`);
  console.log(JSON.stringify(globalStats));

  let offsetValue: number = offset || 0;
  let masterTime: number, masterEndTime: number;

  masterTime = performance.now();

  for await (const user of results) {
    const now = performance.now();

    console.log(
      `=== [${user.userId}] Starting with ${user.userName} - ${user.totalSets} (YIB) ===`
    );

    const start = performance.now();

    const data = await getYearInBricksReview({
      userId: user.userId,
      start: startDate.toISOString(),
      end: endDate.toISOString(),
      stats: globalStats,
    });

    const mediaKey = `${yibYear}/${user.uuid}.json`;

    console.log(`Done with Query: ${new Date().toISOString()}`);

    const end = performance.now();

    console.log(`Time: ${end - start}ms`);

    await uploadUserRecaps({
      data: JSON.stringify(data),
      key: mediaKey,
      isYIB: 1,
    });

    console.log(`S3 Upload Complete: ${new Date().toISOString()}`);

    const later = performance.now();

    const timeDiff = (later - now).toFixed(3);

    console.log(`Full Time: ${timeDiff}ms`);

    const id = await prisma.brickd_YearInBrickUser.findFirst({
      where: {
        userId: user.userId,
        reportId,
      },
    });

    let recapId: number | null = null;

    console.log(`Recap: ${id ? "YES" : "NO"}`);

    if (!id) {
      const response = await prisma.brickd_YearInBrickUser.create({
        data: {
          userId: Number(user.userId),
          reportId,
          itemCount: Number(user.totalSets),
          updatedAt: new Date(),
          timeTaken: parseFloat(timeDiff),
          createdAt: new Date(),
          collectionCount: 0,
          dataUrlStatus: 200,
          dataUrl: `https://brickd-yib.s3.amazonaws.com/${mediaKey}`,
        },
      });

      recapId = response.id;
    } else {
      recapId = id.id;
      await prisma.brickd_YearInBrickUser.updateMany({
        data: {
          updatedAt: new Date(),
          itemCount: Number(user.totalSets),
          dataUrl: `https://brickd-yib.s3.amazonaws.com/${mediaKey}`,
          timeTaken: parseFloat(timeDiff),
        },
        where: {
          id: id.id,
          userId: user.userId,
        },
      });
    }

    if (logId && offsetValue) {
      await prisma.brickd_UserRecapReportLog.update({
        data: {
          updatedAt: new Date(),
          currentOffset: offsetValue,
        },
        where: {
          id: logId,
        },
      });
    }

    offsetValue++;
  }

  masterEndTime = performance.now();

  if (logId) {
    const timeDiff = (masterEndTime - masterTime).toFixed(3);

    await prisma.brickd_UserRecapReportLog.update({
      data: {
        updatedAt: new Date(),
        endTime: new Date(),
        status: "JOBCOMPLETE",
        timeTaken: parseFloat(timeDiff),
      },
      where: {
        id: logId,
      },
    });
  }

  const count = await prisma.brickd_UserRecapReportLog.count({
    where: {
      reportId,
      offset: { gt: offset },
    },
  });

  if (count === 0) {
    await prisma.brickd_UserRecapReport.update({
      data: {
        status: "COMPLETE",
        endTime: new Date(),
        updatedAt: new Date(),
      },
      where: {
        id: reportId,
      },
    });
  }

  if (logId && offset) {
    console.log(`${offset} 🟢 Complete`);
  }
};

/** ---------- Type guards ---------- */
const isSqsEvent = (event: any): event is { Records: SQSRecord[] } =>
  Array.isArray(event?.Records) &&
  event.Records.length > 0 &&
  event.Records[0].eventSource === "aws:sqs";

const parseApiPayload = (event: any) => {
  // Accept: direct invoke with object, Function URL/API GW with JSON body string
  if (typeof event === "string") return JSON.parse(event);
  if (event?.body && typeof event.body === "string") {
    try {
      return JSON.parse(event.body);
    } catch {
      /* fallthrough */
    }
  }
  return event ?? {};
};

export const runOne = async (event: any, context?: Context) => {
  console.log(`🚧 [DEBUG] 🟢 - Getting Started`);

  console.log(`Event: ${JSON.stringify(event, null, 2)}`);
  console.log(`Context: ${JSON.stringify(context, null, 2)}`);

  let logStreamName: string | null = null;

  if (context.logStreamName) {
    logStreamName = context.logStreamName;
  }

  const {
    userId,
    batch,
    offset,
    emails,
    logId,
    rebuild,
    reportDate,
    incremental,
    reportId,
    isYIB,
    yibYear,
  }: {
    userId?: number;
    incremental?: boolean;
    batch?: boolean;
    emails?: boolean;
    offset?: number;
    rebuild?: boolean;
    reportDate?: string;
    reportId?: number;
    logId?: number;
    isYIB?: number;
    yibYear?: number;
  } = event;

  console.log(`This is batch: ${batch}`);

  if (reportDate) {
    await createRecapReport({
      reportDate,
      isYIB: isYIB || 0,
      yibYear: yibYear || null,
    });
  } else if (userId) {
    console.log(`== SINGLE ${userId} ===`);

    console.log(`Passed Report ID: ${reportId || "NOT_PASSED"}`);

    const reportData = reportId
      ? await getReportMedataData({ reportId })
      : await getMostRecentRecapReport();

    if (!reportData) {
      throw new Error("Unable to find Report ID");
    }

    if (reportData.isYIB === 1) {
      await processYearInBricks({
        userId: event.userId,
        reportId: reportData.id,
        logId: null,
        logName: logStreamName,
      });
    } else {
      await processRecap({
        userId: event.userId,
        reportId: reportData.id,
        logId: null,
      });
    }
  } else if (batch) {
    if (!logId || !reportId) {
      throw new Error("Missing LogId / ReportId");
    }

    const data = await getReportMetadataFromId({ reportId });

    if (!data) {
      throw new Error("Invalid Report ID");
    } else {
      console.log(`== BATCH ${offset} ===`);

      if (data.isYIB === 1) {
        await processYearInBricks({
          reportId,
          logId,
          rebuild,
          offset,
          logName: logStreamName,
        });
      } else {
        await processRecap({ offset: offset || 0, logId, reportId, rebuild });
      }
    }
  } else if (incremental) {
    if (!reportId) {
      throw new Error("Missing ReportId");
    }

    const data = await getReportMetadataFromId({ reportId });

    if (!data) {
      throw new Error("Invalid Report ID");
    } else {
      if (data.isYIB === 1) {
        await processYearInBricks({
          reportId,
          logId: null,
        });
      } else {
        await processRecap({ reportId, logId: null });
      }
    }
  } else if (emails) {
    if (!reportId) {
      throw new Error("Missing ReportId");
    }

    console.log("== Email ===");
    await sendEmails({ reportId });
  } else {
    if (!reportId) {
      console.error(`Missing Report ID`);
    } else {
      console.log("== KICK OFF TASKS ===");
      await kickOffTasks({ reportId, rebuild });
    }
  }
};

export const handler = async (event: any, context?: Context): Promise<any> => {
  console.log("🚧 [DEBUG] 🟢 - Start");
  console.log(`Event: ${JSON.stringify(event, null, 2)}`);
  console.log(`Context: ${JSON.stringify(context, null, 2)}`);

  if (isSqsEvent(event)) {
    console.log(`SQS EVENT`);
    // SQS path: iterate records and return partial failures
    const failures: { itemIdentifier: string }[] = [];

    for (const rec of event.Records) {
      try {
        const body = safeParse(rec.body);
        await runOne(body, context); // runs one at a time, logs in order
      } catch (err) {
        console.error("Record failed:", rec.messageId, err);
        failures.push({ itemIdentifier: rec.messageId });
      }
    }

    const resp: SQSBatchResponse = { batchItemFailures: failures };
    console.log(`SQS result: ${JSON.stringify(resp)}`);
    return resp;
  } else {
    console.log("manual EVENT");
    // API / manual path
    const payload = parseApiPayload(event);
    try {
      const result = await runOne(payload, context);
      return result; // direct invoke
    } catch (err: any) {
      console.error(err);

      if (payload.reportId) {
        await prisma.brickd_UserRecapReport.update({
          data: {
            status: "ERROR",
            error: err.message,
            updatedAt: new Date(),
          },
          where: {
            id: payload.reportId,
          },
        });
      }

      throw err; // let direct invoke surface the error
    }
  }
};

function safeParse(s: string) {
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
}
