/**
 * [生产版 v4.3]
 * 云函数：exportAllTalents
 * 描述：导出所有达人的基础信息，用于生成“全量更新指令表”的基础模板。
 * 触发器：API 网关, 通过 GET /talents/export-all 路径调用。
 * 遵循《KOL项目后端开发核心原则与避坑指南 v3.0》。
 */

const { MongoClient } = require('mongodb');

// 从环境变量中获取配置
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
  // 设置CORS头
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Content-Type': 'application/json'
  };

  // 预检请求处理
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }
  
  // 只接受GET请求
  if (event.httpMethod !== 'GET') {
      return { statusCode: 405, headers, body: JSON.stringify({ success: false, message: '不支持的请求方法。' }) };
  }

  try {
    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);

    // [性能优化] 使用 projection 只查询必要的字段
    const projection = {
        _id: 0, // 不返回 MongoDB 的主键
        id: 1,
        xingtuId: 1,
        nickname: 1,
        talentTier: 1,
        talentSource: 1
    };

    // 查询所有文档
    const allTalents = await collection.find({}).project(projection).toArray();

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: `成功导出 ${allTalents.length} 位达人的基础数据。`,
        data: allTalents
      }),
    };

  } catch (error) {
    console.error('处理导出请求时发生错误:', error);
    return { 
        statusCode: 500, 
        headers, 
        body: JSON.stringify({ 
            success: false, 
            message: '服务器内部错误', 
            error: error.message 
        }) 
    };
  }
};
