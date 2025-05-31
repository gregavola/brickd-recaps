import { Context } from "aws-lambda";
import { DateTime } from "luxon";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import {
  getAudienceForMonthlyRecaps,
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
} from "@prisma/client/sql";
import prisma from "./db";
import s3 from "./s3";
import { ObjectCannedACL, PutObjectCommand } from "@aws-sdk/client-s3";
import { debug } from "console";

export interface GlobalStats {
  totalPieces: number;
  totalUsers: number;
  totalSets: number;
  totalDistinctSets: number;
}

const uploadUserRecaps = async ({
  data,
  key,
}: {
  data: string;
  key: string;
}) => {
  // file name

  const uploadParams = {
    Bucket: "brickd-user-recaps",
    Key: key,
    Body: data,
    ContentType: "application/json",
    CacheControl: "max-age=2628000",
    ACL: ObjectCannedACL.private,
  };

  const response = await s3.send(new PutObjectCommand(uploadParams));

  return response;
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

  console.log(`brickd_User.findFirst: ${new Date().toISOString()}`);

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

  console.log(
    `brickd_UserCollectionItem.findMany: ${new Date().toISOString()}`
  );

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

  console.log(`brickd_UserCollection.count: ${new Date().toISOString()}`);

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

  console.log(`brickd_UserCollection.groupBy: ${new Date().toISOString()}`);

  // Minifigs

  const totalMinfigsQuanities =
    await prisma.brickd_UserCollectionMinifigItem.count({
      where: {
        userId,
      },
    });

  console.log(
    `brickd_UserCollectionMinifigItem.count: ${new Date().toISOString()}`
  );

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

  console.log(
    `brickd_UserCollectionMinifigItem.count (total): ${new Date().toISOString()}`
  );

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

  console.log(
    `brickd_UserCollectionItem.count (added): ${new Date().toISOString()}`
  );

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

  console.log(
    `brickd_UserCollectionItem.count (built): ${new Date().toISOString()}`
  );

  const userStreak = await prisma.$queryRawTyped(
    getUserStreakThisMonth(userId, startDate, endDate)
  );

  console.log(`userStreak.custom: ${new Date().toISOString()}`);

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

  console.log(
    `brickd_UserCollectionItem.findMany: ${new Date().toISOString()}`
  );

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

  console.log(
    `getUnqiueLocationCountForUser.custom: ${new Date().toISOString()}`
  );

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

  console.log(`getUserTopLocations.custom: ${new Date().toISOString()}`);

  // Total Sets Built

  const totalPieceCount = await prisma.$queryRawTyped(
    getTotalPieceCountForUserWithRange(startDate, endDate, userId)
  );

  console.log(
    `getTotalPieceCountForUserWithRange.custom: ${new Date().toISOString()}`
  );

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

  console.log(
    `brickd_UserCollection.count (wishlist): ${new Date().toISOString()}`
  );

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

  console.log(
    `brickd_UserCollectionItem.count (wishlist added): ${new Date().toISOString()}`
  );

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

  console.log(
    `brickd_DeleteCollectionItemLog.count: ${new Date().toISOString()}`
  );

  const countData = await prisma.$queryRawTyped(
    getUserRecapStandardStats(userId, startDate, endDate)
  );

  console.log(`getUserRecapStandardStats.custom: ${new Date().toISOString()}`);

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

  console.log(
    `getUserTopActivityTypesByDate.custom: ${new Date().toISOString()}`
  );

  const totalMediaTypes = await prisma.$queryRawTyped(
    getUserTopMediaTypesByDates(userId, startDate, endDate)
  );

  console.log(
    `getUserTopMediaTypesByDates.custom: ${new Date().toISOString()}`
  );

  // top 5 themes

  const topThemes = await prisma.$queryRawTyped(
    getUserTopThemes(userId, startDate, endDate, 5)
  );

  console.log(
    `getUserTopMediaTypesByDates.custom: ${new Date().toISOString()}`
  );

  const topThemeTotalCount = await prisma.$queryRawTyped(
    getUserTopThemeCount(userId, startDate, endDate)
  );

  console.log(`getUserTopThemeCount.custom: ${new Date().toISOString()}`);

  const totalSetsPerUser = globalStats.totalSets / globalStats.totalUsers;
  // top 5 viewed sets

  let activity: any | null = null;

  const topActivity = await prisma.$queryRawTyped(
    getTopUserActivity(userId, startDate, endDate, 1)
  );

  console.log(`getTopUserActivity.custom: ${new Date().toISOString()}`);

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

    console.log(`brickd_UserActivity.findFirst: ${new Date().toISOString()}`);

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
            ? Number(userStreak[0].longest_streak || 0)
            : 0,
        longestStreakDates:
          userStreak.length !== 0 &&
          userStreak[0].longest_streak_start_date &&
          userStreak[0].longest_streak_end_date
            ? {
                start: userStreak[0].longest_streak_start_date,
                end: userStreak[0].longest_streak_end_date,
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
  const data = await prisma.$queryRawTyped(
    getGlobalBuiltStats(startDate, endDate)
  );

  if (data.length === 0) {
    throw new Error("Empty Global");
  }

  return {
    totalUsers: Number(data[0].totalUsers),
    totalDistinctSets: Number(data[0].totalDistinctSets),
    totalPieces: Number(data[0].totalPieces),
    totalSets: Number(data[0].totalSets),
  };
};

export const processRecap = async ({ userId }: { userId?: number }) => {
  dayjs.extend(utc);

  const startDate = dayjs.utc().startOf("month");
  const endDate = dayjs.utc().endOf("month");

  if (userId) {
    console.log(`== TEST RUN for ${userId}===`);
  }

  const results = userId
    ? await prisma.$queryRawTyped(
        getAudienceForMonthlyRecapTest(
          startDate.toDate(),
          endDate.toDate(),
          startDate.toDate(),
          userId
        )
      )
    : await prisma.$queryRawTyped(
        getAudienceForMonthlyRecaps(
          startDate.toDate(),
          endDate.toDate(),
          startDate.toDate()
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

  const dateKey = startDate.format("MM_YY");

  console.log(`REPORT DATE: ${dateKey}`);

  for await (const user of results) {
    const now = performance.now();
    console.log(
      `--- Starting with ${user.userName} - ${user.totalSets} sets ----`
    );

    const data = await getUserRecaps({
      userId: user.id,
      start: startDate.toISOString(),
      end: endDate.toISOString(),
      stats: globalStats,
      mediaCount: globalTotalMediaUploaded,
    });

    recap = data;

    const mediaKey = `${dateKey}/${user.uuid}.json`;

    console.log(`Done with Query: ${new Date().toISOString()}`);

    await uploadUserRecaps({
      data: JSON.stringify(data),
      key: mediaKey,
    });

    console.log(`S3 Upload Complete: ${new Date().toISOString()}`);

    const later = performance.now();

    const timeDiff = (later - now).toFixed(3);

    const id = await prisma.brickd_UserRecap.findFirst({
      where: {
        userId: user.id,
        reportDate: startDate.toDate(),
      },
    });

    if (!id) {
      await prisma.brickd_UserRecap.create({
        data: {
          userId: user.id,
          reportDate: startDate.toDate(),
          timeTaken: parseFloat(timeDiff),
          createdAt: new Date(),
          updatedAt: new Date(),
          dataUrl: mediaKey,
        },
      });
    } else {
      await prisma.brickd_UserRecap.updateMany({
        data: {
          updatedAt: new Date(),
          dataUrl: mediaKey,
          timeTaken: parseFloat(timeDiff),
        },
        where: {
          reportDate: startDate.toDate(),
          userId: user.id,
        },
      });
    }
  }
};

export const handler = async (event: any, context?: Context) => {
  console.log(`🚧 [DEBUG] 🟢 - Getting Started`);

  console.log(`Event: ${JSON.stringify(event, null, 2)}`);
  console.log(`Context: ${JSON.stringify(context, null, 2)}`);

  await processRecap({});
};
