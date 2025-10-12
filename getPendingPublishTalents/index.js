/**
 * @file getPendingPublishTalents.js
 * @version 2.0-strict-logic
 * @description 智能任务中心 - API接口 (严格逻辑修正版)
 * - [核心修正] 遵循“方案一”，增加了对 plannedReleaseDate 的日期过滤，确保接口返回的数据与后端任务生成逻辑完全一致。
 * - [逻辑变更] 查询条件从宽泛的“所有待发布”收紧为“所有已到期或逾期待发布”。
 * - [API行为] 现在此接口将只返回真正触发了“视频未发布”任务的合作记录。
 */

const { MongoClient, ObjectId } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABORATIONS_COLLECTION = 'collaborations';
const TALENTS_COLLECTION = 'talents';

let client;

async function connectToDatabase() {
  if (client && client.topology && client.topology.isConnected()) {
    return client;
  }
  client = new MongoClient(MONGO_URI);
  await client.connect();
  return client;
}

// [新增] 获取 YYYY-MM-DD 格式的日期字符串
function getTodayDateString() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
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
        const projectId = event.queryStringParameters ? event.queryStringParameters.projectId : null;

        if (!projectId) {
            return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '缺少 projectId 查询参数' }) };
        }

        const dbClient = await connectToDatabase();
        const db = dbClient.db(DB_NAME);

        const collabsCol = db.collection(COLLABORATIONS_COLLECTION);
        const talentsCol = db.collection(TALENTS_COLLECTION);

        const todayStr = getTodayDateString();

        // [核心修正] 修改了这里的查询逻辑
        const pendingCollabs = await collabsCol.find({
            projectId: projectId,
            status: '客户已定档',
            // 关键：增加了日期过滤，只查找计划发布日期是今天或以前的记录
            plannedReleaseDate: { $ne: null, $lte: todayStr } 
        }).toArray();

        if (pendingCollabs.length === 0) {
            return { statusCode: 200, headers, body: JSON.stringify({ success: true, data: [] }) };
        }

        const talentIds = pendingCollabs.map(c => c.talentId);
        
        const talentQuery = { id: { $in: talentIds } };
        const talents = await talentsCol.find(talentQuery).project({ id: 1, nickname: 1, _id: 0 }).toArray();
        const talentMap = new Map(talents.map(t => [t.id, t.nickname]));

        const responseData = pendingCollabs.map(collab => ({
            collaborationId: collab.id,
            talentName: talentMap.get(collab.talentId) || '未知达人',
            videoId: collab.videoId || '',
            // [修正] 返回 plannedReleaseDate 以便调试，实际前端可能不需要
            plannedReleaseDate: collab.plannedReleaseDate || '', 
            publishDate: collab.publishDate || ''
        }));
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: true, data: responseData })
        };

    } catch (error) {
        console.error('Error in getPendingPublishTalents:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
        };
    }
};
