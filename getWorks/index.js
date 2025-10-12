/**
 * @file getworks.js
 * @version 2.2-data-enhancement
 * @description 获取作品列表的高性能接口，支持服务端分页、筛选、排序，并聚合了关联数据。
 * * --- 更新日志 (v2.2) ---
 * - [数据增强] 使用 $lookup 聚合管道，额外关联 collaborations 集合以获取 taskId。
 * - [健壮性] 确保即使关联的 collaboration 被删除，查询依然能正常返回作品数据。
 */

const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const WORKS_COLLECTION = 'works';

let client;

async function connectToDatabase() {
  if (client && client.topology && client.topology.isConnected()) {
    return client;
  }
  client = new MongoClient(MONGO_URI);
  await client.connect();
  return client;
}

exports.handler = async (event, context) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }
  
  if (!MONGO_URI) {
      return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: "服务器数据库配置不完整。" }) };
  }

  try {
    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(WORKS_COLLECTION);
    
    const queryParams = event.queryStringParameters || {};
    const page = parseInt(queryParams.page) || 1;
    const pageSize = parseInt(queryParams.pageSize) || 15;
    const search = queryParams.search || '';
    const projectId = queryParams.projectId || '';
    const sourceType = queryParams.sourceType || '';
    const sortBy = queryParams.sortBy || 't7_publishedAt';
    const sortOrder = queryParams.sortOrder === 'asc' ? 1 : -1;

    const matchStage = {};
    if (projectId) matchStage.projectId = projectId;
    if (sourceType) matchStage.sourceType = sourceType;
    if (search) {
      matchStage['talentName'] = { $regex: search, $options: 'i' };
    }

    const countPipeline = [{ $match: matchStage }];
    const totalItems = await collection.aggregate([...countPipeline, { $count: 'total' }]).toArray();
    
    const pipeline = [
        { $match: matchStage },
        {
            $lookup: {
                from: 'collaborations',
                localField: 'collaborationId',
                foreignField: 'id',
                as: 'collaborationInfo'
            }
        },
        {
            $lookup: {
                from: 'projects',
                localField: 'projectId',
                foreignField: 'id',
                as: 'projectInfo'
            }
        },
        {
            $lookup: {
                from: 'talents',
                localField: 'talentId',
                foreignField: 'id',
                as: 'talentInfo'
            }
        },
        {
            $project: {
                _id: 0,
                id: 1,
                sourceType: 1,
                collaborationId: 1,
                platformWorkId: 1,
                url: 1,
                // --- T7 Data ---
                t7_platformWorkId: 1,
                t7_publishedAt: 1,
                t7_totalViews: 1,
                t7_likeCount: 1,
                t7_commentCount: 1,
                t7_shareCount: 1,
                t7_componentImpressionCount: 1,
                t7_componentClickCount: 1,
                // --- T21 Data ---
                t21_totalViews: 1,
                t21_likeCount: 1,
                t21_commentCount: 1,
                t21_shareCount: 1,
                t21_componentImpressionCount: 1,
                t21_componentClickCount: 1,
                // --- Aggregated Fields ---
                projectName: { $ifNull: [ { $arrayElemAt: ['$projectInfo.name', 0] }, 'N/A' ] },
                talentName: { $ifNull: [ { $arrayElemAt: ['$talentInfo.nickname', 0] }, 'N/A' ] },
                taskId: { $ifNull: [ { $arrayElemAt: ['$collaborationInfo.taskId', 0] }, 'N/A' ] }
            }
        },
        { $sort: { [sortBy]: sortOrder } },
        { $skip: (page - 1) * pageSize },
        { $limit: pageSize }
    ];

    const works = await collection.aggregate(pipeline).toArray();
    const count = totalItems.length > 0 ? totalItems[0].total : 0;
    const totalPages = Math.ceil(count / pageSize);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        data: {
          pagination: {
            page,
            pageSize,
            totalItems: count,
            totalPages
          },
          works: works
        }
      })
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
    };
  }
};

