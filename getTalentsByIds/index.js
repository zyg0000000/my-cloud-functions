/**
 * 云函数：getTalentsByIds
 * 描述：根据提供的程序ID (id) 数组，批量获取达人的完整信息。
 * 触发器：API 网关, 通过 POST /talents/by-ids 路径调用。
 * 核心逻辑：用于“导出勾选达人”功能，确保获取到的是最新的数据以供更新。
 */

const { MongoClient } = require('mongodb');

// [规范统一] 参照 updatetalent.js，从环境变量中获取配置
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const TALENTS_COLLECTION = process.env.MONGO_TALENTS_COLLECTION || 'talents';

let client;

// [规范统一] 共享的数据库连接函数
async function connectToDatabase() {
  if (client && client.topology && client.topology.isConnected()) {
    return client;
  }
  client = new MongoClient(MONGO_URI);
  await client.connect();
  return client;
}

exports.handler = async (event, context) => {
  // [规范统一] 标准响应头
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers, body: JSON.stringify({ success: false, message: 'Method Not Allowed' }) };
  }

  try {
    const requestBody = JSON.parse(event.body || '{}');
    const { ids } = requestBody;

    if (!Array.isArray(ids) || ids.length === 0) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体必须包含一个名为 "ids" 的非空数组。' }) };
    }

    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);
    
    // --- 核心逻辑：使用 $in 高效查询 ---
    const talents = await collection.find({ id: { $in: ids } }).toArray();

    // --- 构造成功响应 ---
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: `成功获取 ${talents.length} 条达人信息。`,
        data: talents
      }),
    };

  } catch (error) {
    console.error('批量获取云函数发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
