/**
 * [生产版 v2.0 - 支持多次合作]
 * 云函数：updateCollaborator
 * 描述：更新指定的一条合作记录。
 * --- v2.0 更新日志 ---
 * - [核心改造] 在允许更新的字段白名单中增加了 `plannedReleaseDate`，以支持多次合作业务模式。
 * --- v1.6 更新日志 ---
 * - [核心逻辑升级] 扩大了返点专用时间戳 (discrepancyReasonUpdatedAt) 的更新范围。
 * - 现在，任何与返点回收相关的核心字段 (实收金额、回收日期、凭证、差异原因) 发生变更，
 * - 都会自动刷新此时间戳，实现了返点业务操作与通用记录更新的彻底分离。
 * ---------------------
 */

const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABORATIONS_COLLECTION = 'collaborations';

// 白名单：所有允许前端更新的字段
const ALLOWED_UPDATE_FIELDS = [
  'amount', 'priceInfo', 'rebate', 'orderType', 'status',
  'orderDate', 'publishDate', 'videoId', 'paymentDate',
  'actualRebate', 'recoveryDate', 'contentFile', 'taskId',
  'rebateScreenshots',
  'discrepancyReason',
  'discrepancyReasonUpdatedAt',
  'plannedReleaseDate' // [改造步骤 3] 允许更新计划发布日期
];

// ** [v1.6] 定义返点业务的核心字段 **
// 这些字段的任何变更都会触发返点专用时间戳的更新
const REBATE_RELATED_FIELDS = [
    'actualRebate',
    'recoveryDate',
    'rebateScreenshots',
    'discrepancyReason'
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
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少合作记录ID (id)。' }) };
    }
    
    const updatePayload = { $set: {}, $unset: {} };
    let hasValidFields = false;
    let hasRebateRelatedUpdate = false; // ** [v1.6] 标记是否有返点相关更新

    for (const field of ALLOWED_UPDATE_FIELDS) {
        if (Object.prototype.hasOwnProperty.call(updateFields, field)) {
            hasValidFields = true;
            
            // ** [v1.6] 检查是否更新了返点相关字段
            if (REBATE_RELATED_FIELDS.includes(field)) {
                hasRebateRelatedUpdate = true;
            }

            if (updateFields[field] === null || updateFields[field] === '' || (Array.isArray(updateFields[field]) && updateFields[field].length === 0)) {
                updatePayload.$unset[field] = "";
            } else {
                updatePayload.$set[field] = updateFields[field];
            }
        }
    }

    if (!hasValidFields) {
        return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中没有需要更新的有效字段。' }) };
    }
    
    // ** [v1.6] 核心逻辑：如果本次更新包含任何返点相关字段，则刷新专用时间戳
    if (hasRebateRelatedUpdate) {
        // 如果是要清除返点信息，则也清除时间戳
        if (updateFields.actualRebate === null) {
            updatePayload.$unset.discrepancyReasonUpdatedAt = "";
        } else {
            updatePayload.$set.discrepancyReasonUpdatedAt = new Date();
        }
    }

    if (updateFields.publishDate && updateFields.status !== '视频已发布') {
        if(!updatePayload.$set.status) {
            updatePayload.$set.status = '视频已发布';
        }
    }
    
    // 通用更新时间戳：只要有$set操作，就更新
    if (Object.keys(updatePayload.$set).length > 0) {
      updatePayload.$set.updatedAt = new Date();
    }
    
    const finalUpdate = {};
    if (Object.keys(updatePayload.$set).length > 0) finalUpdate.$set = updatePayload.$set;
    if (Object.keys(updatePayload.$unset).length > 0) finalUpdate.$unset = updatePayload.$unset;

    if (Object.keys(finalUpdate).length === 0) {
       return { statusCode: 200, headers, body: JSON.stringify({ success: true, message: '没有字段需要更新。' }) };
    }

    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(COLLABORATIONS_COLLECTION);

    const result = await collection.updateOne({ id: id }, finalUpdate);

    if (result.matchedCount === 0) {
      return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `ID为 '${id}' 的合作记录不存在。` }) };
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ success: true, message: '合作记录更新成功。' }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
