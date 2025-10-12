/**
 * [生产版 v1.1 - 标准化升级版]
 * 云函数：processTalents
 * 描述：处理新增或批量更新达人信息的请求。
 * --- v1.1 更新日志 ---
 * - [规范统一] 更新了环境变量和配置读取方式，与项目最新标准对齐。
 * - [规范统一] 实现了“双源数据读取”，以兼容在线测试工具。
 * ---------------------
 * 触发器：API 网关, 通过 POST /talents 路径调用。
 */

const { MongoClient } = require('mongodb');

// [规范统一] 从环境变量中获取配置，并提供默认值
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const TALENTS_COLLECTION = process.env.MONGO_TALENTS_COLLECTION || 'talents';

let client;

async function connectToDatabase() {
  if (client && client.topology && client.topology.isConnected()) {
    return client;
  }
  // [规范统一] 使用 MONGO_URI
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
    // [规范统一] 使用从环境变量读取的集合名
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);

    // [规范统一] 实现“双源数据读取”
    let body = {};
    if (event.body) {
        try { body = JSON.parse(event.body); } catch(e) { /* ignore */ }
    }
    if (Object.keys(body).length === 0 && event.queryStringParameters) {
        body = event.queryStringParameters;
    }
    
    // 确定是单个对象还是数组，统一处理为数组
    const talentsToProcess = Array.isArray(body) ? body : [body];

    if (talentsToProcess.length === 0 || (talentsToProcess.length === 1 && Object.keys(talentsToProcess[0]).length === 0)) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ success: false, message: '请求体为空或格式不正确。' }),
      };
    }

    let createdCount = 0;
    let updatedCount = 0;

    const bulkOperations = talentsToProcess.map(talent => {
      // 数据校验：确保核心字段存在
      if (!talent.xingtuId || !talent.nickname) {
        return null; // 跳过无效数据
      }

      const now = new Date();
      
      // 准备更新操作
      return {
        updateOne: {
          filter: { xingtuId: talent.xingtuId }, // 以 xingtuId 作为唯一键
          update: {
            $set: {
              ...talent, // 传入的所有字段都会被更新
              updatedAt: now,
            },
            $setOnInsert: { // 仅在插入新文档时应用的字段
              id: `talent_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
              createdAt: now,
            }
          },
          upsert: true, // 如果找不到匹配的文档，则创建新文档
        }
      };
    }).filter(op => op !== null); // 过滤掉无效操作

    if (bulkOperations.length > 0) {
      const result = await collection.bulkWrite(bulkOperations);
      createdCount = result.upsertedCount;
      updatedCount = result.modifiedCount;
    }

    // 返回成功响应
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: '操作成功！',
        data: {
            created: createdCount,
            updated: updatedCount,
            skipped: talentsToProcess.length - (createdCount + updatedCount),
        }
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }),
    };
  }
};
