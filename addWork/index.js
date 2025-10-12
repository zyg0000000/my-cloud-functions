/**
 * [生产版 v2.0 - 升级版]
 * 云函数：addWork
 * 描述：新增一条作品记录。
 * 触发器：API 网关, 通过 POST /works 路径调用。
 * --- v2.0 更新日志 ---
 * - [新增] 增加了对 collaborations 集合的引用。
 * - [核心优化] 当创建“合作作品”时，会自动查询并冗余 projectId 字段，以大幅提升未来按项目查询作品的性能。
 * ---------------------
 * 特性：
 * 1. 遵循“彻底扁平化”API路径原则。
 * 2. 实现了“双源数据读取”，兼容 Postman 和在线测试工具。
 * 3. 能够智能判断作品来源 (sourceType)，支持“合作作品”与“非合作作品”的录入。
 */

const { MongoClient, ObjectId } = require('mongodb');

// 从环境变量中获取配置
const MONGO_URI = process.env.MONGO_URI;
const MONGO_DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const WORKS_COLLECTION = 'works';
const TALENTS_COLLECTION = 'talents';
// [v2.0 新增] 引入 collaborations 集合
const COLLABORATIONS_COLLECTION = 'collaborations';

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
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  try {
    const dbClient = await connectToDatabase();
    const db = dbClient.db(MONGO_DB_NAME);
    const worksCollection = db.collection(WORKS_COLLECTION);
    const talentsCollection = db.collection(TALENTS_COLLECTION);
    // [v2.0 新增]
    const collaborationsCollection = db.collection(COLLABORATIONS_COLLECTION);

    let inputData = {};
    if (event.body) { try { inputData = JSON.parse(event.body); } catch (e) { /* ignore */ } }
    if (Object.keys(inputData).length === 0 && event.queryStringParameters) {
        inputData = event.queryStringParameters;
    }

    const {
      platformWorkId,
      talentId,
      collaborationId,
      title,
      url,
      publishedAt
    } = inputData;

    if (!platformWorkId || !talentId) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少必要的字段 (platformWorkId, talentId)。' }) };
    }
    
    const talentExists = await talentsCollection.findOne({ id: talentId });
    if (!talentExists) {
        return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `指定的达人ID '${talentId}' 不存在。` }) };
    }

    const workExists = await worksCollection.findOne({ platformWorkId });
    if (workExists) {
      return { statusCode: 409, headers, body: JSON.stringify({ success: false, message: `作品ID '${platformWorkId}' 已存在，请勿重复添加。` }) };
    }

    const now = new Date();
    const newWork = {
      _id: new ObjectId(),
      id: `work_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
      platformWorkId,
      talentId,
      collaborationId: collaborationId || null,
      sourceType: collaborationId ? 'COLLABORATION' : 'ORGANIC',
      title: title || null,
      url: url || null,
      publishedAt: publishedAt ? new Date(publishedAt) : null,
      dailyStats: [],
      createdAt: now,
      updatedAt: now,
    };
    
    // --- [v2.0 核心升级逻辑] ---
    // 如果是合作作品，自动补充 projectId
    if (newWork.sourceType === 'COLLABORATION') {
        const collaboration = await collaborationsCollection.findOne({ id: newWork.collaborationId });
        if (!collaboration) {
             return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `指定的合作ID '${newWork.collaborationId}' 不存在。` }) };
        }
        // 将 projectId 冗余到作品记录中
        newWork.projectId = collaboration.projectId;
    }
    // -------------------------

    await worksCollection.insertOne(newWork);

    const { _id, ...returnData } = newWork;

    return {
      statusCode: 201,
      headers,
      body: JSON.stringify({
        success: true,
        message: '作品记录创建成功',
        data: returnData
      }),
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
