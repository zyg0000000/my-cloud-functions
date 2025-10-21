/**
 * @file updateCollaborator/index.js
 * @version 4.4 - Final Data Source Fix
 * @description [最终BUG修复] 解决了前后端数据传递方式不匹配导致的 400 错误。
 * --- v4.4 更新日志 ---
 * - [核心修复] 重构了ID的获取方式。函数现在从请求的 `body` 中同时解析 `id` 和其他更新字段，而不是从 URL query 中获取 `id`。
 * - [问题解决] 此修改彻底解决了因后端无法获取 `id` 而返回 "400 Bad Request" 或 "404 Not Found" 的问题。
 * - [兼容性] 保留了 v4.3 版本对新版 MongoDB 驱动的兼容性修复。
 */

const { MongoClient, ObjectId } = require('mongodb');

// --- 数据库配置 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABORATIONS_COLLECTION = 'collaborations';
const WORKS_COLLECTION = 'works';

const ALLOWED_UPDATE_FIELDS = [
  'amount', 'priceInfo', 'rebate', 'orderType', 'status',
  'orderDate', 'publishDate', 'videoId', 'paymentDate',
  'actualRebate', 'recoveryDate', 'contentFile', 'taskId',
  'rebateScreenshots',
  'discrepancyReason',
  'discrepancyReasonUpdatedAt',
  'plannedReleaseDate'
];

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
    'Access-Control-Allow-Methods': 'POST, PUT, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }
  
  // 保持对 PUT 和 POST 的兼容
  if (event.httpMethod !== 'POST' && event.httpMethod !== 'PUT') {
      return { statusCode: 405, headers, body: JSON.stringify({ success: false, message: 'Method Not Allowed' }) };
  }

  const dbClient = await connectToDatabase();
  const db = dbClient.db(DB_NAME);
  const collaborationsCollection = db.collection(COLLABORATIONS_COLLECTION);
  const worksCollection = db.collection(WORKS_COLLECTION);

  try {
    // [核心修复] 从 body 中同时解构 id 和其他更新字段
    const { id, ...updateFields } = JSON.parse(event.body || '{}');

    if (!id) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少合作记录ID (id)。' }) };
    }
    
    const updatePayload = { $set: {}, $unset: {} };
    let hasValidFields = false;
    let hasRebateRelatedUpdate = false;

    for (const field of ALLOWED_UPDATE_FIELDS) {
        if (Object.prototype.hasOwnProperty.call(updateFields, field)) {
            hasValidFields = true;
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
    
    if (hasRebateRelatedUpdate) {
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
    
    if (Object.keys(updatePayload.$set).length > 0) {
      updatePayload.$set.updatedAt = new Date();
    }
    
    const finalUpdate = {};
    if (Object.keys(updatePayload.$set).length > 0) finalUpdate.$set = updatePayload.$set;
    if (Object.keys(updatePayload.$unset).length > 0) finalUpdate.$unset = updatePayload.$unset;

    if (Object.keys(finalUpdate).length === 0) {
       return { statusCode: 200, headers, body: JSON.stringify({ success: true, message: '没有字段需要更新。' }) };
    }

    const updatedCollaboration = await collaborationsCollection.findOneAndUpdate(
        { id: id },
        finalUpdate,
        { returnDocument: 'after' }
    );

    if (!updatedCollaboration) {
      return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `ID为 '${id}' 的合作记录不存在。` }) };
    }

    const isVideoPublished = updatedCollaboration.publishDate || updatedCollaboration.videoId;

    if (isVideoPublished) {
        const workExists = await worksCollection.findOne({ collaborationId: id });

        if (!workExists) {
            console.log(`[Work Upsert] Work for collaboration ${id} does not exist. Creating now.`);
            const newWork = {
                _id: new ObjectId(),
                id: `work_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
                collaborationId: updatedCollaboration.id,
                projectId: updatedCollaboration.projectId,
                talentId: updatedCollaboration.talentId,
                taskId: updatedCollaboration.taskId || null,
                platformWorkId: updatedCollaboration.videoId || null,
                publishedAt: updatedCollaboration.publishDate ? new Date(updatedCollaboration.publishDate) : null,
                sourceType: 'COLLABORATION',
                dailyStats: [],
                createdAt: new Date(),
                updatedAt: new Date(),
            };
            await worksCollection.insertOne(newWork);
            console.log(`[Work Upsert] Successfully CREATED new work record ${newWork.id}`);
        } else {
            console.log(`[Work Upsert] Work for collaboration ${id} already exists. Updating now.`);
            const workUpdatePayload = {
                $set: {
                    platformWorkId: updatedCollaboration.videoId || null,
                    publishedAt: updatedCollaboration.publishDate ? new Date(updatedCollaboration.publishDate) : null,
                    taskId: updatedCollaboration.taskId || null,
                    updatedAt: new Date()
                }
            };
            await worksCollection.updateOne({ collaborationId: id }, workUpdatePayload);
            console.log(`[Work Upsert] Successfully UPDATED existing work record for collaboration ${id}.`);
        }
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

