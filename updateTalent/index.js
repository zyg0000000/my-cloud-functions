/**
 * [生产版 v2.2 - 修复版]
 * 云函数：updateTalent
 * 描述：更新指定达人的基础信息。
 * --- v2.2 更新日志 ---
 * - [BUG修复] 移除了对 xingtuId 字段的更新限制，允许前端修改达人星图ID。
 * --- v2.1 更新日志 ---
 * - [规范统一] 更新了环境变量和配置读取方式，与项目最新标准对齐。
 * - [规范统一] 将接收的ID字段从 "talentId" 统一为 "id"，与其他update接口保持一致。
 * ---------------------
 * 触发器：API 网关, 通过 PUT /update-talent 路径调用。
 * 遵循《KOL项目后端开发核心原则与避坑指南 v3.0》。
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
    'Access-Control-Allow-Methods': 'PUT, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  try {
    // --- [原则 2.1] 双源数据读取 ---
    let inputData = {};
    if (event.body) {
        try { inputData = JSON.parse(event.body); } catch(e) { /* ignore */ }
    }
    if (Object.keys(inputData).length === 0 && event.queryStringParameters) {
        inputData = event.queryStringParameters;
    }

    // [规范统一] 将接收的ID字段统一为 "id"
    const { id, ...updateFields } = inputData;

    if (!id) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少达人ID (id)。' }) };
    }
    
    // [安全设计] 不允许通过此接口修改程序内部ID和数据库主键
    delete updateFields.id;
    delete updateFields._id;
    
    if (Object.keys(updateFields).length === 0) {
        return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少需要更新的字段。' }) };
    }

    const dbClient = await connectToDatabase();
    // [规范统一] 使用从环境变量读取的集合名
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);

    const updatePayload = {
        $set: {
            ...updateFields,
            updatedAt: new Date() // 自动更新时间戳
        }
    };

    const result = await collection.updateOne(
      { id: id }, // 使用程序ID进行匹配
      updatePayload
    );

    if (result.matchedCount === 0) {
      return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `ID为 '${id}' 的达人不存在。` }) };
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ success: true, message: '达人信息更新成功。' }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
