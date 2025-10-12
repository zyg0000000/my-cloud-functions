/**
 * 云函数：bulkUpdateTalents
 * @version 2.3 - Ignore Empty Values
 * @description
 * - [核心UX修复] 更新逻辑增强，现在会主动忽略值为 null 或空字符串 ("") 的字段，解决了Excel中空单元格会导致数据库字段被意外清空的问题。
 * - [操作简化] 用户现在可以安全地在Excel中将不想修改的单元格留空，而无需担心数据丢失。
 * - [兼容性] 此修复完全在后端实现，前端代码无需任何改动。
 * - [保留功能] 保留了 v2.2 版本对 `prices` 和 `rebates` 字段的智能合并功能。
 */

const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const TALENTS_COLLECTION = process.env.MONGO_TALENTS_COLLECTION || 'talents';

const ALLOWED_TOP_LEVEL_FIELDS = ['nickname', 'talentTier', 'talentType', 'talentSource', 'uid'];
const ALLOWED_PERFORMANCE_FIELDS = [
    'maleAudienceRatio', 'femaleAudienceRatio', 'cpm60s', 'audience_18_40_ratio', 
    'audience_40_plus_ratio', 'lastUpdated', 'ratio_18_23', 'ratio_24_30',
    'ratio_31_40', 'ratio_41_50', 'ratio_50_plus', 'ratio_town_middle_aged',
    'ratio_senior_middle_class', 'ratio_z_era', 'ratio_urban_silver',
    'ratio_town_youth', 'ratio_exquisite_mom', 'ratio_new_white_collar',
    'ratio_urban_blue_collar'
];
const MERGEABLE_ARRAY_FIELDS = ['prices', 'rebates'];

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
  if (event.httpMethod !== 'PUT') {
    return { statusCode: 405, headers, body: JSON.stringify({ success: false, message: 'Method Not Allowed' }) };
  }

  try {
    const { updates } = JSON.parse(event.body || '{}');
    if (!Array.isArray(updates) || updates.length === 0) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体格式应为 { "updates": [...] } 且数组不能为空。' }) };
    }

    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);
    
    const incomingXingtuIds = updates.map(t => t.xingtuId).filter(id => id);
    if (incomingXingtuIds.length === 0) {
        return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '提交的数据中缺少有效的 xingtuId。' }) };
    }

    const existingTalentsArray = await collection.find(
        { xingtuId: { $in: incomingXingtuIds } },
        { projection: { xingtuId: 1, prices: 1, rebates: 1 } }
    ).toArray();
    
    const existingTalentsMap = new Map(existingTalentsArray.map(t => [t.xingtuId, t]));

    const bulkOps = [];
    const errors = [];
    let failedCount = 0;

    for (const item of updates) {
      if (!item.xingtuId || !existingTalentsMap.has(item.xingtuId)) {
        failedCount++;
        errors.push({ xingtuId: item.xingtuId || 'N/A', reason: !item.xingtuId ? '缺少 xingtuId' : '该星图ID不存在。' });
        continue;
      }

      const updatePayload = {};
      let hasValidUpdate = false;
      const existingTalent = existingTalentsMap.get(item.xingtuId);

      // 1. 处理白名单内的顶层字段
      for (const key of ALLOWED_TOP_LEVEL_FIELDS) {
          // [核心UX修复] 只有当字段值明确存在且不为空时 (不是 undefined, null, 或空字符串)，才加入更新负载
          if (item[key] !== undefined && item[key] !== null && item[key] !== '') {
              updatePayload[key] = item[key];
              hasValidUpdate = true;
          }
      }

      // 2. 处理白名单内的 performanceData 字段
      if (typeof item.performanceData === 'object' && item.performanceData !== null) {
          for (const perfKey of ALLOWED_PERFORMANCE_FIELDS) {
              if (item.performanceData[perfKey] !== undefined && item.performanceData[perfKey] !== null && item.performanceData[perfKey] !== '') {
                  updatePayload[`performanceData.${perfKey}`] = item.performanceData[perfKey];
                  hasValidUpdate = true;
              }
          }
      }
      
      // 3. 特殊处理 prices 和 rebates 字段
      for (const key of MERGEABLE_ARRAY_FIELDS) {
          if (Array.isArray(item[key])) {
              const currentArray = existingTalent[key] || [];
              const newArray = item[key];
              let mergedArray;

              if (key === 'prices') {
                  const priceMap = new Map();
                  currentArray.forEach(p => priceMap.set(`${p.year}-${p.month}`, p));
                  newArray.forEach(p => priceMap.set(`${p.year}-${p.month}`, p));
                  mergedArray = Array.from(priceMap.values());
              } else if (key === 'rebates') {
                  const rebateMap = new Map();
                  currentArray.forEach(r => rebateMap.set(r.rate, r));
                  newArray.forEach(r => rebateMap.set(r.rate, r));
                  mergedArray = Array.from(rebateMap.values());
              }
              
              if (mergedArray) {
                updatePayload[key] = mergedArray;
                hasValidUpdate = true;
              }
          }
      }

      if (!hasValidUpdate) {
          failedCount++;
          errors.push({ xingtuId: item.xingtuId, reason: '提交的数据中没有在白名单内的、且包含有效值的字段。' });
          continue;
      }

      updatePayload.updatedAt = new Date();
      
      bulkOps.push({
        updateOne: {
          filter: { xingtuId: item.xingtuId },
          update: { $set: updatePayload }
        }
      });
    }

    let updatedCount = 0;
    if (bulkOps.length > 0) {
      const bulkWriteResult = await collection.bulkWrite(bulkOps, { ordered: false });
      updatedCount = bulkWriteResult.modifiedCount;
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: '批量更新操作完成。',
        data: { updated: updatedCount, failed: failedCount, errors: errors }
      }),
    };

  } catch (error) {
    console.error('批量更新云函数发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};

