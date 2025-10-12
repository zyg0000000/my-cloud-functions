/**
 * @file system-status.js
 * @version 1.0
 * @description [新增] 全局数据维护 - 后端API接口
 * - 响应前端 /system-status 的 GET 请求。
 * - 查询并返回全局性的系统状态，用于驱动任务中心顶部的维护板块。
 * - 包括：达人Performance数据的最新状态和达人报价的本月状态。
 */

const { MongoClient } = require('mongodb');

// --- 配置项 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

// --- 数据库集合名称 ---
const TALENTS_COLLECTION = 'talents';
const TASKS_COLLECTION = 'tasks';

let client;

// --- 数据库连接 ---
async function connectToDatabase() {
  if (client && client.topology && client.topology.isConnected()) {
    return client;
  }
  client = new MongoClient(MONGO_URI);
  await client.connect();
  return client;
}

// --- 辅助函数：从任务描述中提取数量 ---
function getCountFromDescription(description) {
    if (!description) return 0;
    const match = description.match(/有 (\d+) 位达人/);
    return match ? parseInt(match[1], 10) : 0;
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
        const db = dbClient.db(DB_NAME);

        const talentsCol = db.collection(TALENTS_COLLECTION);
        const tasksCol = db.collection(TASKS_COLLECTION);

        // 并行执行所有数据库查询以提高效率
        const [
            performanceTask,
            priceTask,
            latestUpdateResult
        ] = await Promise.all([
            // 检查是否存在待办的Performance更新任务
            tasksCol.findOne({ type: 'TALENT_PERFORMANCE_UPDATE_REMINDER', status: 'pending' }),
            // 检查是否存在待办的Price更新任务
            tasksCol.findOne({ type: 'TALENT_PRICE_UPDATE_REMINDER', status: 'pending' }),
            // 查询所有达人中最新的performance更新日期
            talentsCol.aggregate([
                { $match: { "performanceData.lastUpdated": { $exists: true } } },
                { $group: { _id: null, maxDate: { $max: "$performanceData.lastUpdated" } } }
            ]).toArray()
        ]);

        const latestUpdate = latestUpdateResult.length > 0 ? latestUpdateResult[0].maxDate : null;

        const responseData = {
            performance: {
                lastUpdated: latestUpdate,
                isUpdateNeeded: !!performanceTask // 如果任务存在，则为true
            },
            price: {
                unconfirmedCount: priceTask ? getCountFromDescription(priceTask.description) : 0,
                isUpdateNeeded: !!priceTask // 如果任务存在，则为true
            }
        };

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: true, data: responseData })
        };

    } catch (error) {
        console.error('Error in system-status function:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
        };
    }
};
