/**
 * 云函数：batchUpdateTalents
 * 描述：根据筛选条件，高性能地批量统一更新达人信息。
 * 触发器：API 网关, 通过 POST /talents/batch-update 路径调用。
 * 核心逻辑：接收筛选条件和更新数据，在数据库层面直接执行 updateMany 操作。
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

/**
 * [核心] 将前端筛选条件对象转换为 MongoDB 查询语句
 * @param {object} filters - 前端传来的筛选对象 (queryState)
 * @returns {object} - MongoDB find() 方法可以接受的查询对象
 */
function buildMongoQuery(filters) {
    const query = {};
    const andConditions = [];

    // 搜索 (昵称或星图ID)
    if (filters.search) {
        const searchRegex = { $regex: filters.search, $options: 'i' };
        andConditions.push({ $or: [{ nickname: searchRegex }, { xingtuId: searchRegex }] });
    }

    // 层级 (多选)
    if (filters.tiers && filters.tiers.length > 0) {
        andConditions.push({ talentTier: { $in: filters.tiers } });
    }
    
    // 类型 (多选)
    if (filters.types && filters.types.length > 0) {
        andConditions.push({ talentType: { $in: filters.types } });
    }

    // 返点率区间
    const rebateQuery = {};
    if (typeof filters.rebateMin === 'number') rebateQuery.$gte = filters.rebateMin;
    if (typeof filters.rebateMax === 'number') rebateQuery.$lte = filters.rebateMax;
    if (Object.keys(rebateQuery).length > 0) {
        // 查询 rebates 数组中，是否存在至少一个 rate 符合条件的元素
        andConditions.push({ 'rebates.rate': rebateQuery });
    }
    
    // 一口价区间 (需要使用 $elemMatch 精确匹配同一元素)
    if (filters.priceYear && filters.priceMonth) {
        const priceElemMatch = { year: filters.priceYear, month: filters.priceMonth };
        const priceConditions = {};
        if (typeof filters.priceMin === 'number') priceConditions.$gte = filters.priceMin;
        if (typeof filters.priceMax === 'number') priceConditions.$lte = filters.priceMax;
        if (Object.keys(priceConditions).length > 0) {
            priceElemMatch.price = priceConditions;
        }
        andConditions.push({ prices: { $elemMatch: priceElemMatch } });
    }
    
    if (andConditions.length > 0) {
        query.$and = andConditions;
    }

    return query;
}


exports.handler = async (event, context) => {
  // [规范统一] 标准响应头
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
    const { filters, updateData } = JSON.parse(event.body || '{}');

    // --- [安全校验] ---
    if (!filters || typeof filters !== 'object' || Object.keys(filters).length === 0) {
        return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体必须包含一个名为 "filters" 的非空对象。为防止意外更新全库，筛选条件不能为空。' }) };
    }
    if (!updateData || typeof updateData !== 'object' || Object.keys(updateData).length === 0) {
        return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体必须包含一个名为 "updateData" 的非空对象。' }) };
    }
    
    // [安全设计] 黑名单，禁止通过此接口更新关键标识符
    const forbiddenFields = ['id', '_id', 'xingtuId', 'createdAt'];
    for (const field of forbiddenFields) {
        if (field in updateData) {
            return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: `不允许通过此接口更新受保护的字段: ${field}` }) };
        }
    }

    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);
    
    // --- 核心逻辑 ---
    // 1. 构建查询
    const mongoQuery = buildMongoQuery(filters);

    // 2. 构建更新操作
    const updatePayload = {
        $set: {
            ...updateData,
            updatedAt: new Date()
        }
    };
    
    // 3. 执行批量更新
    const result = await collection.updateMany(mongoQuery, updatePayload);

    // --- 构造成功响应 ---
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: '批量统一更新操作成功完成。',
        data: {
          updated: result.modifiedCount
        }
      }),
    };

  } catch (error) {
    console.error('批量统一更新云函数发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
