/**
 * @file bulkCreateTalents.js
 * @description
 * 云函数: bulkCreateTalents (v3 - 最终修复版)
 * 负责处理从 Excel 文件批量创建新达人的请求。
 *
 * 核心逻辑:
 * 1. 【兼容性】同时兼容【中文表头】和【英文表头】的键名。
 * 2. 【安全性】检查 `xingtuId` 是否已存在，防止重复创建，并返回详细的错误报告。
 * 3. 【规范性】为每条新记录自动生成唯一的 `id` 和标准的时间戳。
 */

const { MongoClient } = require('mongodb');

// [规范统一] 参照 updatetalent.js，从环境变量中获取配置
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const TALENTS_COLLECTION = process.env.MONGO_TALENTS_COLLECTION || 'talents';

let client;

// [规范统一] 共享的数据库连接函数
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
  
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers, body: JSON.stringify({ success: false, message: 'Method Not Allowed' }) };
  }

  try {
    const talentsData = JSON.parse(event.body || '[]');
    if (!Array.isArray(talentsData) || talentsData.length === 0) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体必须是一个非空数组。' }) };
    }

    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);

    // 1. 提取所有传入的 xingtuId (兼容中文和英文)
    const incomingXingtuIds = talentsData.map(t => t.xingtuId || t['星图ID']).filter(id => id);
    if (incomingXingtuIds.length === 0) {
        return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '提交的数据中缺少有效的 xingtuId。' }) };
    }

    // 2. 一次性查询数据库，找出所有已存在的 xingtuId
    const existingTalents = await collection.find({ xingtuId: { $in: incomingXingtuIds } }).project({ xingtuId: 1 }).toArray();
    const existingXingtuIdSet = new Set(existingTalents.map(t => t.xingtuId));

    // 3. 分离出真正需要创建的新达人和已存在的达人
    const talentsToInsert = [];
    const errors = [];
    let failedCount = 0;

    for (const row of talentsData) {
      // 【关键修复】: 为了增加健壮性，使用 || 操作符同时兼容中文和英文表头
      const nickname = row.nickname || row['达人昵称'];
      const xingtuId = row.xingtuId || row['星图ID'];
      const uid = row.uid || row['UID'];
      const talentType = row.talentType || row['内容标签'];
      const talentSource = row.talentSource || row['达人来源'];
      const talentTier = row.talentTier || row['达人层级'];

      if (!xingtuId) {
        failedCount++;
        errors.push({ nickname: nickname || '未知昵称', reason: '缺少 xingtuId' });
        continue;
      }
      if (existingXingtuIdSet.has(xingtuId.toString())) {
        failedCount++;
        errors.push({ xingtuId: xingtuId, nickname: nickname, reason: '该星图ID已存在，创建被拒绝。' });
      } else {
        talentsToInsert.push({
          id: `talent_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          nickname: nickname || '未命名',
          xingtuId: xingtuId.toString(),
          uid: uid ? uid.toString() : null,
          talentType: talentType ? String(talentType).split(',').map(t => t.trim()) : [],
          talentSource: talentSource || '野生达人',
          talentTier: talentTier || null,
          prices: [],
          rebates: [],
          createdAt: new Date(),
          updatedAt: new Date(),
        });
      }
    }

    // 4. 一次性批量插入所有新达人
    let createdCount = 0;
    if (talentsToInsert.length > 0) {
      const insertResult = await collection.insertMany(talentsToInsert, { ordered: false });
      createdCount = insertResult.insertedCount;
    }

    return {
      statusCode: 201, // 201 Created is more appropriate here
      headers,
      body: JSON.stringify({
        success: true,
        message: '批量创建操作完成。',
        data: {
          created: createdCount,
          failed: failedCount,
          errors: errors
        }
      }),
    };

  } catch (error) {
    console.error('批量创建云函数发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};

