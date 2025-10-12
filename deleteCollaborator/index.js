/**
 * [生产版 v2.0 - 数据完整性增强版]
 * 云函数：deleteCollaborator
 * 描述：删除一条指定的合作记录，并同步删除所有关联的作品数据。
 * --- v2.0 更新日志 ---
 * - [核心功能增强] 新增核心逻辑：在删除 'collaborations' 集合中的记录后，
 * 会继续在 'works' 集合中查找所有具有相同 collaborationId 的记录，并将其全部删除。
 * - [数据一致性] 此修改确保了在删除合作关系时，相关联的作品效果数据也会被彻底清除，
 * 避免了“幽灵数据”的产生，保证了系统的整体数据完整性。
 * ---------------------
 */

const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABS_COLLECTION = 'collaborations';
const WORKS_COLLECTION = 'works'; // 新增：需要操作作品集合

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

    const { collaborationId } = inputData;

    if (!collaborationId) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少必要的字段 (collaborationId)。' }) };
    }

    const dbClient = await connectToDatabase();
    const collabsCollection = dbClient.db(DB_NAME).collection(COLLABS_COLLECTION);
    const worksCollection = dbClient.db(DB_NAME).collection(WORKS_COLLECTION); // 新增

    // 第一步：删除合作记录
    const deletionResult = await collabsCollection.deleteOne({ id: collaborationId });

    if (deletionResult.deletedCount === 0) {
      return { 
          statusCode: 404, 
          headers, 
          body: JSON.stringify({ 
              success: false, 
              message: `合作记录ID '${collaborationId}' 未找到，无法删除。`
          }) 
      };
    }
    
    // [v2.0] 第二步：同步删除所有关联的作品数据
    // 即使没有关联的作品数据，这个操作也是安全的
    const worksDeletionResult = await worksCollection.deleteMany({ collaborationId: collaborationId });
    const deletedWorksCount = worksDeletionResult.deletedCount || 0;


    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ 
          success: true, 
          message: `合作记录及关联的 ${deletedWorksCount} 条作品数据删除成功。`
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
