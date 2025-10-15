/**
 * [优化方案 v3.0 - works 记录创建触发器]
 * 云函数：updateCollaborator
 * 描述：更新指定的一条合作记录。
 * --- v3.0 更新日志 ---
 * - [核心架构升级] 此函数现在是 `works` 记录的唯一、权威创建入口。
 * - [新增逻辑] 当一次合作首次被更新并包含有效的 `publishDate` 或 `videoId` 时，此函数会自动检查并创建一条与之关联的、包含所有关键ID（projectId, talentId等）的完整 `works` 记录。
 * - [数据完整性] 此机制从根本上杜绝了因其他流程（如日报录入）被动创建不完整“幽灵”`works`记录的可能性。
 * - [依赖] 新增了对 `works` 集合的数据库操作。
 */

const { MongoClient, ObjectId } = require('mongodb');

// --- 数据库配置 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABORATIONS_COLLECTION = 'collaborations';
const WORKS_COLLECTION = 'works'; // [新增依赖]

// 白名单：所有允许前端更新的字段
const ALLOWED_UPDATE_FIELDS = [
  'amount', 'priceInfo', 'rebate', 'orderType', 'status',
  'orderDate', 'publishDate', 'videoId', 'paymentDate',
  'actualRebate', 'recoveryDate', 'contentFile', 'taskId',
  'rebateScreenshots',
  'discrepancyReason',
  'discrepancyReasonUpdatedAt',
  'plannedReleaseDate'
];

// 返点业务的核心字段
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

  const dbClient = await connectToDatabase();
  const db = dbClient.db(DB_NAME);
  const collaborationsCollection = db.collection(COLLABORATIONS_COLLECTION);
  const worksCollection = db.collection(WORKS_COLLECTION); // [新增依赖]

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

    const result = await collaborationsCollection.updateOne({ id: id }, finalUpdate);

    if (result.matchedCount === 0) {
      return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `ID为 '${id}' 的合作记录不存在。` }) };
    }

    // --- [优化逻辑 v3.0] 开始：创建 works 记录 ---
    const isVideoPublished = updateFields.publishDate || updateFields.videoId;
    if (isVideoPublished) {
        const collaborationRecord = await collaborationsCollection.findOne({ id: id });
        if (collaborationRecord) {
            const workExists = await worksCollection.findOne({ collaborationId: id });

            if (!workExists) {
                console.log(`[Work Creation] Work for collaboration ${id} does not exist. Creating now.`);
                const newWork = {
                    _id: new ObjectId(),
                    id: `work_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
                    collaborationId: collaborationRecord.id,
                    projectId: collaborationRecord.projectId,
                    talentId: collaborationRecord.talentId,
                    taskId: collaborationRecord.taskId || null,
                    platformWorkId: collaborationRecord.videoId || null,
                    publishedAt: collaborationRecord.publishDate ? new Date(collaborationRecord.publishDate) : null,
                    sourceType: 'COLLABORATION',
                    dailyStats: [],
                    createdAt: new Date(),
                    updatedAt: new Date(),
                };
                await worksCollection.insertOne(newWork);
                console.log(`[Work Creation] Successfully created new work record ${newWork.id}`);
            }
        }
    }
    // --- [优化逻辑 v3.0] 结束 ---


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
