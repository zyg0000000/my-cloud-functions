/**
 * [生产版 v1.0]
 * 云函数：deleteWork
 * 描述：删除一条已存在的作品记录。
 * 触发器：API 网关, 通过 DELETE /delete-work 路径调用。
 * 核心能力：
 * 1. 接收 workId 并从数据库中删除对应的文档。
 * 2. 如果找不到对应的 workId，会返回 404 错误。
 */

const { MongoClient } = require('mongodb');

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
    'Access-Control-Allow-Methods': 'DELETE, OPTIONS',
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

    const { id: workId } = inputData;

    if (!workId) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中必须包含要删除的作品 "id"。' }) };
    }
    
    const result = await worksCollection.deleteOne({ id: workId });

    if (result.deletedCount === 0) {
      return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `ID为 '${workId}' 的作品记录未找到。` }) };
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: '作品记录删除成功'
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

