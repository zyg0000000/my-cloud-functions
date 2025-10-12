/**
 * @file getWorkStats.js
 * @version 1.0-initial
 * @description 为作品库看板提供独立的、高性能的统计数据接口。
 * * --- 更新日志 (v1.0) ---
 * - [新建] 创建接口，支持根据 projectId, sourceType, search 进行动态筛选和统计。
 * - [性能] 使用 MongoDB Aggregation Pipeline 进行高效的服务端计算。
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
        const search = queryParams.search || '';
        const projectId = queryParams.projectId || '';
        const sourceType = queryParams.sourceType || '';

        // 基础筛选条件
        const matchStage = {};
        if (projectId) matchStage.projectId = projectId;
        if (sourceType) matchStage.sourceType = sourceType;

        // 如果有搜索条件，需要先 $lookup talentName
        const pipeline = [];
        if (search) {
            pipeline.push({
                $lookup: {
                    from: 'talents',
                    localField: 'talentId',
                    foreignField: 'id',
                    as: 'talentInfo'
                }
            });
            matchStage['talentInfo.nickname'] = { $regex: search, $options: 'i' };
        }
        
        pipeline.push({ $match: matchStage });
        
        // 聚合统计
        pipeline.push({
            $group: {
                _id: null,
                totalWorks: { $sum: 1 },
                collabWorks: {
                    $sum: { $cond: [{ $eq: ['$sourceType', 'COLLABORATION'] }, 1, 0] }
                },
                totalViews: { $sum: '$t7_totalViews' }
            }
        });
        
        const result = await collection.aggregate(pipeline).toArray();

        let stats = {
            totalWorks: 0,
            collabWorks: 0,
            organicWorks: 0,
            totalViews: 0
        };

        if (result.length > 0) {
            const data = result[0];
            stats.totalWorks = data.totalWorks;
            stats.collabWorks = data.collabWorks;
            stats.organicWorks = data.totalWorks - data.collabWorks;
            stats.totalViews = data.totalViews;
        }

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: true, data: stats })
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

