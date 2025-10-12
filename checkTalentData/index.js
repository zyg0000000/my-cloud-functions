/**
 * @file index.js (for check_talent_data)
 * @version 1.0-diagnostic
 * @description [诊断工具] 这是一个用于诊断的临时云函数。它只做一件事：根据 talentId，
 * 从 'talents' 集合中获取并返回完整的、未经修改的原始文档数据。
 * 这将帮助我们确认数据库中的实际字段名和数据结构。
 */
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
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
    const queryParams = event.queryStringParameters || {};
    const { talentId } = queryParams;

    if (!talentId) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ success: false, message: '请求参数中必须提供 talentId。' })
      };
    }
    
    const dbClient = await connectToDatabase();
    const talentsCollection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);
    
    // 直接根据ID查询，不进行任何聚合或字段修改
    const talentDocument = await talentsCollection.findOne({ id: talentId });

    if (!talentDocument) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ success: false, message: `在 'talents' 集合中未找到 ID 为 '${talentId}' 的文档。` })
      };
    }

    // 返回最原始的文档数据
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: '返回原始数据库文档。请检查以下data字段中的结构。',
        data: talentDocument,
      }),
    };

  } catch (error) {
    console.error('诊断函数处理请求时发生错误:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
    };
  }
};
