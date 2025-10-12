/**
 * [生产版 v1.0]
 * 云函数：deleteTalent
 * 描述：删除一个指定的达人，并级联删除所有相关的合作记录。
 * 触发器：API 网关, 通过 DELETE /delete-talent 调用。
 */

const { MongoClient } = require('mongodb');

// 从环境变量中获取数据库连接字符串
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABS_COLLECTION = 'collaborations';
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
    'Access-Control-Allow-Methods': 'DELETE, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  try {
    let inputData = {};
    if (event.body) {
        try {
            inputData = JSON.parse(event.body);
        } catch(e) { /* ignore */ }
    }
    if (Object.keys(inputData).length === 0 && event.queryStringParameters) {
        inputData = event.queryStringParameters;
    }

    const { talentId } = inputData;

    if (!talentId) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少必要的字段 (talentId)。' }) };
    }

    const dbClient = await connectToDatabase();
    const collabsCollection = dbClient.db(DB_NAME).collection(COLLABS_COLLECTION);
    const talentsCollection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);

    // 1. 先删除所有相关的合作记录
    const collabDeletionResult = await collabsCollection.deleteMany({ talentId: talentId });

    // 2. 再删除达人本身
    const talentDeletionResult = await talentsCollection.deleteOne({ id: talentId });

    if (talentDeletionResult.deletedCount === 0) {
      return { 
          statusCode: 404, 
          headers, 
          body: JSON.stringify({ 
              success: false, 
              message: `达人ID '${talentId}' 未找到，无法删除。`
          }) 
      };
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ 
          success: true, 
          message: `达人删除成功，同时清理了 ${collabDeletionResult.deletedCount} 条关联的合作记录。`
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
