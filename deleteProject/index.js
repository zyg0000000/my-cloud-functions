/**
 * [生产版 v1.0]
 * 云函数：deleteProject
 * 描述：删除一个指定的项目，并级联删除所有相关的合作记录。
 * 触发器：API 网关, 通过 DELETE /delete-project 调用。
 */

const { MongoClient } = require('mongodb');

// 从环境变量中获取数据库连接字符串
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABS_COLLECTION = 'collaborations';
const PROJECTS_COLLECTION = 'projects';

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

    const { projectId } = inputData;

    if (!projectId) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少必要的字段 (projectId)。' }) };
    }

    const dbClient = await connectToDatabase();
    const collabsCollection = dbClient.db(DB_NAME).collection(COLLABS_COLLECTION);
    const projectsCollection = dbClient.db(DB_NAME).collection(PROJECTS_COLLECTION);

    // 1. 先删除所有相关的合作记录
    const collabDeletionResult = await collabsCollection.deleteMany({ projectId: projectId });

    // 2. 再删除项目本身
    const projectDeletionResult = await projectsCollection.deleteOne({ id: projectId });

    if (projectDeletionResult.deletedCount === 0) {
      return { 
          statusCode: 404, 
          headers, 
          body: JSON.stringify({ 
              success: false, 
              message: `项目ID '${projectId}' 未找到，无法删除。`
          }) 
      };
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ 
          success: true, 
          message: `项目删除成功，同时清理了 ${collabDeletionResult.deletedCount} 条关联的合作记录。`
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};


