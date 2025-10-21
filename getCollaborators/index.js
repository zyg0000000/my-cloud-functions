/**
 * @file getcollaborators.js
 * @version 6.1-status-filtering
 * @description [架构升级] 增加按合作状态筛选的功能，同时保持向后兼容。
 * * --- 更新日志 (v6.1) ---
 * - [核心功能] 新增了对 `statuses` 查询参数的支持。前端可以传递一个以逗号分隔的状态列表 (如: "客户已定档,视频已发布")。
 * - [动态查询] 如果 `statuses` 参数存在，函数会在数据库查询的 `$match` 阶段动态加入 `$in` 过滤器。
 * - [向后兼容] 如果不传递 `statuses` 参数，函数的行为与之前完全相同，不会对结果进行任何状态过滤，确保了旧页面的调用不受影响。
 * - [代码优化] 重构了查询参数解析和聚合管道构建的逻辑，使其更加清晰和可扩展。
 * * --- 历史更新 (v6.0) ---
 * - [业务升级] 支持达人多次合作模式，返回 `plannedReleaseDate` 字段。
 */

const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABS_COLLECTION = 'collaborations';
const PROJECTS_COLLECTION = 'projects';
const TALENTS_COLLECTION = 'talents';
const CAPITAL_RATES_COLLECTION = 'projectCapitalRates'; 

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
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  try {
    const queryParams = event.queryStringParameters || {};
    const { 
        projectId, 
        collaborationId, 
        view, 
        allowGlobal,
        page = '1',
        limit = '10',
        sortBy = 'createdAt',
        order = 'desc',
        statuses // [v6.1 新增] 接收 statuses 参数
    } = queryParams;

    const pageNum = parseInt(page, 10);
    const limitNum = parseInt(limit, 10);
    const skipNum = (pageNum - 1) * limitNum;
    const sortOrder = order === 'asc' ? 1 : -1;

    const isGlobalQueryAllowed = allowGlobal === 'true';
    const hasRequiredFilter = projectId || collaborationId;

    if (view !== 'simple' && !hasRequiredFilter && !isGlobalQueryAllowed) {
         return { 
             statusCode: 400, 
             headers, 
             body: JSON.stringify({ 
                 success: false, 
                 message: '这是一个重量级查询，请求参数中必须包含 projectId 或 collaborationId，或明确设置 allowGlobal=true。' 
            }) 
        };
    }
    
    const dbClient = await connectToDatabase();
    const db = dbClient.db(DB_NAME);
    const collabsCollection = db.collection(COLLABS_COLLECTION);

    if (view === 'simple') {
      let simpleQuery = {};
      if (projectId) simpleQuery.projectId = projectId;
      if (collaborationId) simpleQuery.id = collaborationId;
      const simpleProjection = { _id: 0, id: 1, talentId: 1, status: 1, projectId: 1 };
      const simpleData = await collabsCollection.find(simpleQuery, { projection: simpleProjection }).toArray();
      return {
        statusCode: 200, headers,
        body: JSON.stringify({ success: true, data: simpleData })
      };
    }
    
    // --- [v6.1 核心修改] ---
    // 动态构建聚合管道的 $match 阶段
    const matchStage = {};
    if (projectId) {
        matchStage.projectId = projectId;
    }
    if (collaborationId) {
        matchStage.id = collaborationId;
    }
    // 如果 statuses 参数存在，则解析并添加到查询条件中
    if (statuses) {
        const statusArray = statuses.split(',').map(s => s.trim()).filter(s => s);
        if (statusArray.length > 0) {
            matchStage.status = { $in: statusArray };
        }
    }
    // --- 修改结束 ---

    const aggregationPipeline = [];
    // 只有当 matchStage 包含至少一个条件时，才将其推入管道
    if (Object.keys(matchStage).length > 0) {
        aggregationPipeline.push({ $match: matchStage });
    }
    
    // 后续的聚合管道阶段保持不变
    aggregationPipeline.push(
      { $lookup: { from: PROJECTS_COLLECTION, localField: 'projectId', foreignField: 'id', as: 'projectInfo' } },
      { $lookup: { from: TALENTS_COLLECTION, localField: 'talentId', foreignField: 'id', as: 'talentInfo' } },
      { $unwind: { path: '$projectInfo', preserveNullAndEmptyArrays: true } },
      { $unwind: { path: '$talentInfo', preserveNullAndEmptyArrays: true } },
      { $lookup: { from: CAPITAL_RATES_COLLECTION, localField: 'projectInfo.capitalRateId', foreignField: 'id', as: 'capitalRateInfo' } },
      { $unwind: { path: '$capitalRateInfo', preserveNullAndEmptyArrays: true } },
      {
        $addFields: {
            'amountNum': { $cond: { if: { $in: ['$amount', [null, ""]] }, then: 0, else: { $toDouble: '$amount' } } },
            'rebateNum': { $cond: { if: { $in: ['$rebate', [null, ""]] }, then: 0, else: { $toDouble: '$rebate' } } },
            'projectDiscountNum': { $cond: { if: { $in: ['$projectInfo.discount', [null, ""]] }, then: 1, else: { $toDouble: '$projectInfo.discount' } } },
            'actualRebateNum': { $cond: { if: { $in: ['$actualRebate', [null, ""]] }, then: null, else: { $toDouble: '$actualRebate' } } },
            'monthlyRatePercent': { $let: { vars: { rateValue: { $ifNull: ['$capitalRateInfo.value', 0.7] } }, in: { $cond: { if: { $eq: ['$$rateValue', ""] }, then: 0.7, else: { $toDouble: '$$rateValue' } } } } },
            'orderDateObj': { $cond: { if: { $and: [{$ne: ['$orderDate', null]}, {$ne: ['$orderDate', ""]}] }, then: { $toDate: '$orderDate' }, else: null } },
            'paymentDateObj': { $cond: { if: { $and: [{$ne: ['$paymentDate', null]}, {$ne: ['$paymentDate', ""]}] }, then: { $toDate: '$paymentDate' }, else: '$$NOW' } }
        }
    },
    {
      $addFields: {
        'income': { $multiply: ['$amountNum', '$projectDiscountNum', 1.05] },
        'expense': { $cond: { if: { $eq: ['$orderType', 'original'] }, then: { $multiply: ['$amountNum', 1.05] }, else: { $cond: { if: { $gt: ['$rebateNum', 20] }, then: { $multiply: ['$amountNum', 0.8, 1.05] }, else: { $multiply: ['$amountNum', { $subtract: [1, { $divide: ['$rebateNum', 100] }] }, 1.05] } } } } },
        'rebateReceivable': { $cond: { if: { $eq: ['$orderType', 'original'] }, then: { $multiply: ['$amountNum', { $divide: ['$rebateNum', 100] }] }, else: { $cond: { if: { $gt: ['$rebateNum', 20] }, then: { $multiply: ['$amountNum', { $subtract: [{ $divide: ['$rebateNum', 100] }, 0.20] }] }, else: 0 } } } }
      }
    },
    {
        $addFields: {
            'occupationDays': { $ifNull: [ { $cond: { if: '$orderDateObj', then: { $max: [0, { $divide: [{ $subtract: ['$paymentDateObj', '$orderDateObj'] }, 1000 * 60 * 60 * 24] }] }, else: 0 }}, 0 ] }
        }
    },
    {
        $addFields: {
            'fundsOccupationCost': { $multiply: [ '$expense', { $divide: [ { $divide: ['$monthlyRatePercent', 100] }, 30 ] }, '$occupationDays' ] },
            'rebateForProfitCalc': { $cond: { if: { $eq: ['$projectInfo.status', '已终结'] }, then: { $ifNull: ['$actualRebateNum', 0] }, else: { $ifNull: ['$actualRebateNum', '$rebateReceivable'] } } }
        }
    },
    {
        $addFields: { 'grossProfit': { $add: ['$income', '$rebateForProfitCalc', { $multiply: ['$expense', -1] }] } }
    },
    {
        $addFields: { 'grossProfitMargin': { $cond: { if: { $eq: ['$income', 0] }, then: 0, else: { $multiply: [{ $divide: ['$grossProfit', '$income'] }, 100] } } } }
    });

    const finalProjection = {
      _id: 0,
      id: 1, projectId: 1, talentId: 1,
      talentSource: 1,
      amount: 1, priceInfo: 1, rebate: 1, orderType: 1,
      status: 1, orderDate: 1, publishDate: 1, videoId: 1, paymentDate: 1,
      plannedReleaseDate: 1,
      actualRebate: 1, recoveryDate: 1, createdAt: 1, updatedAt: 1,
      contentFile: 1, taskId: 1,
      rebateScreenshots: 1,
      discrepancyReason: 1,
      discrepancyReasonUpdatedAt: 1,
      talentInfo: {
        nickname: '$talentInfo.nickname',
        xingtuId: '$talentInfo.xingtuId',
        uid: '$talentInfo.uid',
        level: '$talentInfo.talentTier',
        tags: '$talentInfo.talentType'
      },
      metrics: {
        income: { $round: ['$income', 2] },
        expense: { $round: ['$expense', 2] },
        rebateReceivable: { $round: ['$rebateReceivable', 2] },
        fundsOccupationCost: { $round: ['$fundsOccupationCost', 2] },
        grossProfit: { $round: ['$grossProfit', 2] },
        grossProfitMargin: { $round: ['$grossProfitMargin', 2] }
      }
    };

    const facetStage = {
      $facet: {
        paginatedResults: [
          { $sort: { [sortBy]: sortOrder } },
          { $skip: skipNum },
          { $limit: limitNum },
          { $project: finalProjection }
        ],
        totalCount: [
          { $count: 'count' }
        ]
      }
    };
    aggregationPipeline.push(facetStage);
    
    const results = await collabsCollection.aggregate(aggregationPipeline).toArray();
    const collaborators = results[0].paginatedResults;
    const totalCount = results[0].totalCount.length > 0 ? results[0].totalCount[0].count : 0;

    if (collaborationId) {
        if (collaborators.length === 0) { return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: '未找到指定的合作记录' }) }; }
        return { statusCode: 200, headers, body: JSON.stringify({ success: true, data: collaborators[0] }) };
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ 
          success: true, 
          total: totalCount,
          page: pageNum,
          limit: limitNum,
          data: collaborators 
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
    };
  }
};
