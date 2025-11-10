/**
 * @file addProject.js
 * @version 1.4-tracking-status
 * @description 接收前端发送的项目数据，创建一个新的项目文档。
 * * --- 更新日志 (v1.4) ---
 * - [字段升级] 支持新的 `trackingStatus` 字段 (null/'active'/'archived')，替代 `trackingEnabled`
 * - [向后兼容] 仍然支持旧的 `trackingEnabled` 布尔字段
 * * --- 更新日志 (v1.3) ---
 * - [新增字段] 新增了对 `trackingEnabled` (效果追踪开关) 字段的支持。
 * - 在创建新项目时，会接收并存储这个配置项，默认值为 false。
 * * --- 更新日志 (v1.2) ---
 * - [核心功能] 新增了对 `benchmarkCPM` (目标CPM) 字段的支持。
 * - 在创建新项目时，会接收并存储这个关键的考核指标。
 */

const { MongoClient, ObjectId } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const PROJECTS_COLLECTION = process.env.MONGO_PROJECTS_COLLECTION || 'projects';

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
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }
  
  try {
    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(PROJECTS_COLLECTION);

    let inputData = {};
    if (event.body) {
        try { inputData = JSON.parse(event.body); } catch(e) { /* ignore */ }
    }
    if (Object.keys(inputData).length === 0 && event.queryStringParameters) {
        inputData = event.queryStringParameters;
    }
    
    if (!inputData.name || !inputData.type || !inputData.financialYear || !inputData.financialMonth) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
            success: false,
            message: '请求无效，项目名称、类型和财务归属月份为必填项。' 
        }),
      };
    }

    const now = new Date();
    
    // [v1.4] Handle tracking status (new field) or trackingEnabled (legacy field)
    let trackingStatus = null;
    if (inputData.trackingStatus) {
      // Use new trackingStatus field if provided
      trackingStatus = ['active', 'archived'].includes(inputData.trackingStatus) ? inputData.trackingStatus : null;
    } else if (inputData.trackingEnabled === true || inputData.trackingEnabled === 'true') {
      // Convert legacy trackingEnabled to new format
      trackingStatus = 'active';
    }

    const newProjectDocument = {
      _id: new ObjectId(),
      id: `proj_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
      ...inputData,
      // [v1.2] Safely parse benchmarkCPM to a number
      benchmarkCPM: inputData.benchmarkCPM ? parseFloat(inputData.benchmarkCPM) : null,
      // [v1.4] Store trackingStatus (null, 'active', or 'archived')
      trackingStatus: trackingStatus,
      status: '执行中',
      adjustments: [],
      auditLog: [],
      createdAt: now,
      updatedAt: now,
    };

    // Remove legacy field if present
    delete newProjectDocument.trackingEnabled;
    
    delete newProjectDocument._id;
    newProjectDocument._id = new ObjectId();

    await collection.insertOne(newProjectDocument);
    
    const { _id, ...returnData } = newProjectDocument;

    return {
      statusCode: 201,
      headers,
      body: JSON.stringify({
        success: true,
        message: '项目创建成功！',
        data: returnData
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        success: false, 
        message: '服务器内部错误', 
        error: error.message 
      }),
    };
  }
};