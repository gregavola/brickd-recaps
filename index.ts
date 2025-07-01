import { Context } from "aws-lambda";
import { DateTime } from "luxon";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import {
  getAudienceCount,
  getAudienceForMonthlyRecaps,
  getAudienceForMonthlyRecapsWithOffset,
  getAudienceForMonthlyRecapsWithOffsetRebuild,
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
import lambdaClient from "./lambda";
import {
  GetObjectCommand,
  ObjectCannedACL,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { debug } from "console";
import {
  InvokeCommand,
  InvokeCommandInput,
  InvokeCommandOutput,
} from "@aws-sdk/client-lambda";
import { LoopsClient } from "loops";

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

  const startDate = dayjs.utc(data.reportDate).startOf("month");
  const endDate = dayjs.utc(data.reportDate).endOf("month");

  console.log(
    `Starting for ${startDate.toISOString()} to ${endDate.toISOString()}`
  );

  await prisma.brickd_UserRecapReport.update({
    data: {
      startTime: new Date(),
      status: "RUNNING",
    },
    where: {
      id: reportId,
    },
  });

  const results = await prisma.$queryRawTyped(
    getAudienceCount(startDate.toDate(), endDate.toDate())
  );

  console.log(`Total Users: ${results.length}`);

  const totalPages = Math.ceil(results.length / 100);

  const pagesArray: number[] = [];

  for (let i = 0; i < totalPages; i++) {
    pagesArray.push(i);
  }

  console.log(`=== Total Pages: ${pagesArray.length} ===`);

  for await (const item of pagesArray) {
    const offset = item * 100;
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
      };

      const input: InvokeCommandInput = {
        FunctionName: "brickd-recaps",
        InvocationType: "Event",
        Payload: Buffer.from(JSON.stringify(payload), "utf8"),
      };

      const command = new InvokeCommand(input);

      const res: InvokeCommandOutput = await lambdaClient.send(command);

      console.log(`Lambda for ${offset} - ${JSON.stringify(payload)}`);
      console.log(JSON.stringify(res));
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
  const data = await prisma.brickd_UserRecapReport.findFirst({
    where: { id: reportId },
  });

  if (!data) {
    throw new Error(`Invalid Report found for ${reportId}`);
  }

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
        },
      },
    },
    where: {
      reportId,
      status: "QUEUED",
      user: {
        enableCommunicationEmails: 1,
      },
      emailResponse: null,
    },
    orderBy: { userId: "asc" },
  });

  console.log(`Size: ${users.length}`);

  for await (const user of users) {
    console.log(`Starting with ${user.user?.userName || "Unknown"}`);

    const now = performance.now();

    try {
      await prisma.brickd_UserRecap.update({
        data: {
          status: "RUNNING",
          statusDate: new Date(),
        },
        where: {
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
}: {
  reportDate: Date | string;
}) => {
  await prisma.brickd_UserRecapReport.create({
    data: {
      createdAt: new Date(),
      reportDate,
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

  return data.id;
};

export const processRecap = async ({
  userId,
  offset,
  logId,
  sendEmail,
  reportId,
  rebuild,
}: {
  userId?: number;
  offset?: number;
  logId: number | null;
  sendEmail?: boolean;
  reportId: number;
  rebuild?: boolean;
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

  const results =
    rebuild && hasOffset
      ? await prisma.$queryRawTyped(
          getAudienceForMonthlyRecapsWithOffsetRebuild(
            startDate.toDate(),
            endDate.toDate(),
            offset,
            100
          )
        )
      : hasOffset
      ? await prisma.$queryRawTyped(
          getAudienceForMonthlyRecapsWithOffset(
            startDate.toDate(),
            endDate.toDate(),
            reportId,
            offset!,
            100
          )
        )
      : userId
      ? await prisma.$queryRawTyped(
          getAudienceForMonthlyRecapTest(
            startDate.toDate(),
            endDate.toDate(),
            reportId,
            userId
          )
        )
      : await prisma.$queryRawTyped(
          getAudienceForMonthlyRecaps(
            startDate.toDate(),
            endDate.toDate(),
            reportId
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

    console.log(`Start with Query: ${new Date().toISOString()}`);

    const start = performance.now();

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

    const end = performance.now();

    console.log(`Time: ${end - start}ms`);

    await uploadUserRecaps({
      data: JSON.stringify(data),
      key: mediaKey,
    });

    console.log(`S3 Upload Complete: ${new Date().toISOString()}`);

    const later = performance.now();

    const timeDiff = (later - now).toFixed(3);

    console.log(`Full Time: ${timeDiff}ms`);

    let recapId: number | null = null;

    const id = await prisma.brickd_UserRecap.findFirst({
      where: {
        userId: user.id,
        reportDate: startDate.toDate(),
      },
    });

    console.log(`Recap: ${id ? "YES" : "NO"}`);

    if (!id) {
      const response = await prisma.brickd_UserRecap.create({
        data: {
          userId: user.id,
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
          userId: user.id,
        },
      });
    }

    if (sendEmail) {
      await sendSingleEmail({ userId: user.id, recapId });
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
          status: { not: "JOBCOMPLETE" },
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
  }
};

export const handler = async (event: any, context?: Context) => {
  console.log(`🚧 [DEBUG] 🟢 - Getting Started`);

  console.log(`Event: ${JSON.stringify(event, null, 2)}`);
  console.log(`Context: ${JSON.stringify(context, null, 2)}`);

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
  } = event;

  console.log(`This is batch: ${batch}`);

  if (reportDate) {
    await createRecapReport({ reportDate });
  } else if (userId) {
    console.log(`== SINGLE ${userId} ===`);

    const reportId = await getMostRecentRecapReport();

    if (!reportId) {
      throw new Error("Unable to find Report ID");
    }

    await processRecap({ userId: event.userId, reportId, logId: null });
  } else if (batch) {
    if (!logId || !reportId) {
      throw new Error("Missing LogId / ReportId");
    }

    console.log(`== BATCH ${offset} ===`);
    await processRecap({ offset: offset || 0, logId, reportId, rebuild });
  } else if (incremental) {
    if (!reportId) {
      throw new Error("Missing ReportId");
    }

    console.log("== LEFT OFF ===");
    await processRecap({ reportId, logId: null });
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
