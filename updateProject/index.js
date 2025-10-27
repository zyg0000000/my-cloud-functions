/**
 * @file updateProject.js
 * @version 1.4-tracking-enabled
 * @description 更新指定项目的基础信息或状态。
 * * --- 更新日志 (v1.4) ---
 * - [新增字段] 在允许更新的字段白名单中增加了 `trackingEnabled` 字段。
 * - 支持通过此接口开启或关闭项目的"效果追踪"功能。
 * * --- 更新日志 (v1.3) ---
 * - [核心功能] 在允许更新的字段白名单中增加了 `benchmarkCPM` 字段。
 * - 现在可以通过此接口创建或更新项目的"目标CPM"考核指标。
 */

const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const PROJECTS_COLLECTION = process.env.MONGO_PROJECTS_COLLECTION || 'projects';

// [v1.4] Add trackingEnabled to the list of allowed fields
const ALLOWED_UPDATE_FIELDS = [
    'name', 'qianchuanId', 'type', 'budget', 'benchmarkCPM', 'year', 'month',
    'financialYear', 'financialMonth', 'discount', 'capitalRateId',
    'status', 'adjustments', 'projectFiles', 'trackingEnabled'
];


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
    'Access-Control-Allow-Methods': 'PUT, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  try {
    let inputData = {};
    if (event.body) {
        try { inputData = JSON.parse(event.body); } catch(e) { /* ignore */ }
    }
    if (Object.keys(inputData).length === 0 && event.queryStringParameters) {
        inputData = event.queryStringParameters;
    }

    const { id, ...updateFields } = inputData;

    if (!id) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少项目ID (id)。' }) };
    }
    
    const updatePayload = { $set: {}, $unset: {} };
    let hasValidFields = false;

    for (const field of ALLOWED_UPDATE_FIELDS) {
        if (Object.prototype.hasOwnProperty.call(updateFields, field)) {
            hasValidFields = true;
            // [v1.3] Ensure benchmarkCPM is stored as a number
            if (field === 'benchmarkCPM') {
                const value = parseFloat(updateFields[field]);
                if (!isNaN(value)) {
                    updatePayload.$set[field] = value;
                } else {
                    updatePayload.$unset[field] = ""; // Unset if empty or invalid
                }
            } 
            // [v1.4] Ensure trackingEnabled is stored as a boolean
            else if (field === 'trackingEnabled') {
                updatePayload.$set[field] = updateFields[field] === true || updateFields[field] === 'true' ? true : false;
            }
            else if (updateFields[field] === null || updateFields[field] === '') {
                updatePayload.$unset[field] = "";
            } else {
                updatePayload.$set[field] = updateFields[field];
            }
        }
    }

    if (!hasValidFields) {
        return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中没有需要更新的有效字段。' }) };
    }
    
    if (Object.keys(updatePayload.$set).length > 0) {
      updatePayload.$set.updatedAt = new Date();
    }
    
    // If status is updated, add an audit log
    if (updateFields.status) {
        const auditLogEntry = {
            timestamp: new Date(),
            user: "System",
            action: `项目状态由人工变更为: ${updateFields.status}`
        };
        updatePayload.$push = {
            auditLog: {
                $each: [auditLogEntry],
                $position: 0
            }
        };
    }
    
    const finalUpdate = {};
    if (Object.keys(updatePayload.$set).length > 0) finalUpdate.$set = updatePayload.$set;
    if (Object.keys(updatePayload.$unset).length > 0) finalUpdate.$unset = updatePayload.$unset;
    if (updatePayload.$push) finalUpdate.$push = updatePayload.$push;


    if (Object.keys(finalUpdate).length === 0) {
       return { statusCode: 200, headers, body: JSON.stringify({ success: true, message: '没有字段需要更新。' }) };
    }

    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(PROJECTS_COLLECTION);

    const result = await collection.updateOne({ id: id }, finalUpdate);

    if (result.matchedCount === 0) {
      return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `ID为 '${id}' 的项目不存在。` }) };
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ success: true, message: '项目信息更新成功。' }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
