/**
 * @file getPerformanceData.js
 * @version 1.0
 * @description
 * 云函数: getPerformanceData (生产版)
 * 专为“近期表现”页面(performance.html)设计的高性能数据接口。
 *
 * 功能特性:
 * - 继承自 getTalentsSearch 的全部功能 (分页, 搜索, 筛选, 排序)。
 * - [增强] 在一次查询中返回 dashboardStats, 用于驱动数据看板。
 * - [增强] 在一次查询中返回 allFilteredIds, 用于支持“全选所有”功能。
 *
 * 触发器: API 网关, GET /performance/search
 */

const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const TALENTS_COLLECTION = process.env.MONGO_TALENTS_COLLECTION || 'talents';

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

  try {
    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);

    // 1. 解析查询参数
    const queryParams = event.queryStringParameters || {};
    const page = parseInt(queryParams.page) || 1;
    const pageSize = parseInt(queryParams.pageSize) || 10;
    const search = queryParams.search || '';
    const tiers = queryParams.tiers ? queryParams.tiers.split(',') : [];
    const types = queryParams.types ? queryParams.types.split(',') : [];
    const sortBy = queryParams.sortBy || 'createdAt';
    const sortOrder = queryParams.sortOrder === 'asc' ? 1 : -1;

    // 2. 构建基础筛选条件 ($match stage)
    const matchStage = {};
    if (search) {
      matchStage.$or = [
        { nickname: { $regex: search, $options: 'i' } },
        { xingtuId: { $regex: search, $options: 'i' } }
      ];
    }
    if (tiers.length > 0) {
      matchStage.talentTier = { $in: tiers };
    }
    if (types.length > 0) {
      matchStage.talentType = { $in: types };
    }
    
    // 3. 构建聚合管道
    const pipeline = [
      { $match: matchStage },
      {
        $facet: {
          // 分支一: 获取分页后的详细数据
          paginatedResults: [
            { $sort: { [sortBy]: sortOrder } },
            { $skip: (page - 1) * pageSize },
            { $limit: pageSize },
            { $project: { _id: 0 } }
          ],
          // 分支二: 计算元数据 (总数, ID列表, 看板统计)
          metadata: [
            {
              $group: {
                _id: null,
                totalItems: { $sum: 1 },
                allFilteredIds: { $push: '$id' },
                // 看板统计: 层级分布
                tierDistribution: { $push: '$talentTier' },
                // 看板统计: CPM分布
                cpmDistribution: { $push: '$performanceData.cpm60s' },
                 // 看板统计: 性别占比
                maleAudienceDistribution: { $push: '$performanceData.maleAudienceRatio' },
                femaleAudienceDistribution: { $push: '$performanceData.femaleAudienceRatio' }
              }
            }
          ]
        }
      }
    ];

    const results = await collection.aggregate(pipeline).toArray();

    // 4. 解析聚合结果
    const paginatedTalents = results[0].paginatedResults;
    const meta = results[0].metadata[0] || {};
    const totalItems = meta.totalItems || 0;
    const totalPages = Math.ceil(totalItems / pageSize);

    // 5. 后处理看板数据
    const processDistribution = (arr, groups, key) => {
        (arr || []).forEach(val => {
            const num = parseFloat(val);
            if (!isNaN(num)) {
                if (key === 'cpm') {
                    if (num < 15) groups['< 15']++;
                    else if (num < 30) groups['15-30']++;
                    else groups['>= 30']++;
                } else if (key === 'gender') {
                    if (num < 0.4) groups['< 40%']++;
                    else if (num < 0.6) groups['40-60%']++;
                    else groups['>= 60%']++;
                }
            } else {
                groups['未填写']++;
            }
        });
        return groups;
    };
    
    const dashboardStats = {
        tierDistribution: (meta.tierDistribution || []).reduce((acc, tier) => {
            const key = tier || '未分类';
            acc[key] = (acc[key] || 0) + 1;
            return acc;
        }, {}),
        cpmDistribution: processDistribution(meta.cpmDistribution, { '< 15': 0, '15-30': 0, '>= 30': 0, '未填写': 0 }, 'cpm'),
        maleAudienceDistribution: processDistribution(meta.maleAudienceDistribution, { '< 40%': 0, '40-60%': 0, '>= 60%': 0, '未填写': 0 }, 'gender'),
        femaleAudienceDistribution: processDistribution(meta.femaleAudienceDistribution, { '< 40%': 0, '40-60%': 0, '>= 60%': 0, '未填写': 0 }, 'gender'),
    };

    // 6. 组装并返回最终响应
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        data: {
          pagination: {
            page,
            pageSize,
            totalItems,
            totalPages
          },
          talents: paginatedTalents,
          metadata: {
              dashboardStats: dashboardStats,
              allFilteredIds: meta.allFilteredIds || []
          }
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
