/**
 * [生产版 v1.0]
 * 云函数：updateWork
 * 描述：更新一条已存在的作品记录。
 * 触发器：API 网关, 通过 PUT /update-work 路径调用。
 * 核心能力：
 * 1. 接收 workId 和要更新的字段。
 * 2. 支持更新作品的基础信息（如 title, url）。
 * 3. 支持更新或追加每日表现数据（dailyStats数组）。
 * 4. 自动更新 updatedAt 时间戳。
 */

const { MongoClient, ObjectId } = require('mongodb');

// 从环境变量中获取配置
const MONGO_URI = process.env.MONGO_URI;
const MONGO_DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
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
    'Access-Control-Allow-Methods': 'PUT, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  try {
    const dbClient = await connectToDatabase();
    const worksCollection = dbClient.db(MONGO_DB_NAME).collection(WORKS_COLLECTION);

    // [核心原则] 实现“双源数据读取”
    let inputData = {};
    if (event.body) { try { inputData = JSON.parse(event.body); } catch (e) { /* ignore */ } }
    if (Object.keys(inputData).length === 0 && event.queryStringParameters) {
        inputData = event.queryStringParameters;
    }

    const { id: workId, ...updates } = inputData;

    if (!workId) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中必须包含要更新的作品 "id"。' }) };
    }
    
    if (Object.keys(updates).length === 0) {
        return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中没有提供任何需要更新的字段。' }) };
    }

    const updateDoc = {
      $set: {}
    };

    // 遍历所有传入的更新字段
    for (const key in updates) {
      if (Object.prototype.hasOwnProperty.call(updates, key)) {
        // 禁止直接更新 id 或 _id
        if (key !== 'id' && key !== '_id') {
          updateDoc.$set[key] = updates[key];
        }
      }
    }
    
    // 自动更新 updatedAt 字段
    updateDoc.$set.updatedAt = new Date();
    
    const result = await worksCollection.updateOne(
      { id: workId },
      updateDoc
    );

    if (result.matchedCount === 0) {
      return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `ID为 '${workId}' 的作品记录未找到。` }) };
    }
    
    // 获取并返回更新后的完整文档
    const updatedWork = await worksCollection.findOne({ id: workId }, { projection: { _id: 0 } });

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: '作品记录更新成功',
        data: updatedWork
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
