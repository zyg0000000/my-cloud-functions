/**
 * @file getTalentsSearch_v8.2.js
 * @version 8.3-release
 * @description
 * 云函数: getTalentsSearch (最终统一版)
 *
 * [v8.3] 排序修复:
 * 1. [新增修复] 增加了对内嵌文档字段 (performanceData) 的排序支持。现在可以正确处理 'maleAudienceRatio', 'cpm60s' 等字段的排序请求。
 * 2. [健壮性] 确保了所有排序请求都能被正确翻译为数据库可识别的 "点符号" 路径。
 *
 * [v8.2] 兼容性升级:
 * 1. [新增功能] 增加了对 `talent_pool.js` 页面特有的筛选条件的支持。
 * 2. [统一接口] 成为一个统一的、向后兼容的接口，能同时满足 `performance.js` 和 `talent_pool.js` 两个页面的所有数据查询需求。
 *
 * [v8.0] 核心能力:
 * - 接收 POST 请求与 JSON 查询体。
 * - 动态构建复杂的 AND/OR 查询。
 * - 支持丰富的比较操作符。
 * - 保留分页、排序和看板统计功能。
 */

const { MongoClient } = require('mongodb');

// --- 数据库连接配置 (保持不变) ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const TALENTS_COLLECTION = 'talents';

let client;

async function connectToDatabase() {
  if (client && client.topology && client.topology.isConnected()) {
    return client;
  }
  client = new MongoClient(MONGO_URI);
  await client.connect();
  return client;
}

// --- 辅助函数，将前端操作符转换为MongoDB查询语法 ---
function buildMongoQuery(filter) {
    const { dimension, operator, value } = filter;
    
    if (operator === 'isEmpty') {
        return { [dimension]: { $in: [null, "", undefined] } };
    }
    if (operator === 'isNotEmpty') {
        return { [dimension]: { $nin: [null, "", undefined] } };
    }

    const numValue = parseFloat(value);
    const isNumeric = !isNaN(numValue);

    switch(operator) {
        case '>': return isNumeric ? { [dimension]: { $gt: numValue } } : null;
        case '>=': return isNumeric ? { [dimension]: { $gte: numValue } } : null;
        case '<': return isNumeric ? { [dimension]: { $lt: numValue } } : null;
        case '<=': return isNumeric ? { [dimension]: { $lte: numValue } } : null;
        case '!=': return { [dimension]: { $ne: isNumeric ? numValue : value } };
        case '=': return { [dimension]: isNumeric ? numValue : value };
        case 'contains': return { [dimension]: { $regex: String(value), $options: 'i' } };
        case 'notContains': return { [dimension]: { $not: { $regex: String(value), $options: 'i' } } };
        case 'between':
            if (Array.isArray(value) && value.length === 2) {
                const min = parseFloat(value[0]);
                const max = parseFloat(value[1]);
                if (!isNaN(min) && !isNaN(max)) {
                    return { [dimension]: { $gte: min, $lte: max } };
                }
            }
            return null;
        default: return null;
    }
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
        const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);
        
        const body = JSON.parse(event.body || '{}');
        
        const page = body.page || 1;
        const pageSize = body.pageSize || 10;
        const search = body.search || '';
        const tiers = body.tiers || [];
        const types = body.types || [];
        
        let sortBy = body.sortBy || 'createdAt';
        const sortOrder = body.sortOrder === 'asc' ? 1 : -1;
        
        // --- [新增排序修复逻辑] ---
        // 定义哪些排序键是内嵌在 performanceData 对象中的
        const performanceDataSortKeys = new Set([
            'maleAudienceRatio',
            'femaleAudienceRatio',
            'cpm60s',
            'audience_18_40_ratio',
            'audience_40_plus_ratio',
            'lastUpdated'
        ]);

        // 如果请求的排序键属于 performanceData，则为其添加正确的路径前缀
        if (performanceDataSortKeys.has(sortBy)) {
            sortBy = `performanceData.${sortBy}`;
        }
        // --- [排序修复逻辑结束] ---

        const filterLogic = body.filterLogic === 'OR' ? '$or' : '$and';
        const dataFilters = body.filters || [];
        
        const matchConditions = [];

        // --- 基础筛选 ---
        if (search) {
            const isNumericId = /^\d+$/.test(search);
            if (isNumericId) {
                 matchConditions.push({ $or: [ { nickname: { $regex: search, $options: 'i' } }, { xingtuId: search }, { uid: search } ] });
            } else {
                 matchConditions.push({ nickname: { $regex: search, $options: 'i' } });
            }
        }
        if (tiers.length > 0) { matchConditions.push({ talentTier: { $in: tiers } }); }
        if (types.length > 0) { matchConditions.push({ talentType: { $in: types } }); }
        
        // --- `performance.js` 页面的灵活筛选 ---
        if (dataFilters.length > 0) {
            const dataFilterConditions = dataFilters.map(buildMongoQuery).filter(Boolean);
            if (dataFilterConditions.length > 0) {
                if (filterLogic === '$or' && dataFilterConditions.length > 1) {
                    matchConditions.push({ $or: dataFilterConditions });
                } else {
                    matchConditions.push(...dataFilterConditions);
                }
            }
        }

        // --- `talent_pool.js` 页面的特定筛选 ---
        if (body.rebateMin !== undefined || body.rebateMax !== undefined) {
            const rebateQuery = {};
            if (body.rebateMin !== null && body.rebateMin !== undefined) rebateQuery.$gte = body.rebateMin;
            if (body.rebateMax !== null && body.rebateMax !== undefined) rebateQuery.$lte = body.rebateMax;
            if (Object.keys(rebateQuery).length > 0) matchConditions.push({ 'rebates.rate': rebateQuery });
        }

        if (body.priceYear && body.priceMonth) {
            const priceMatch = { year: body.priceYear, month: body.priceMonth };
            const priceRangeQuery = {};
            if (body.priceMin !== null && body.priceMin !== undefined) priceRangeQuery.$gte = body.priceMin;
            if (body.priceMax !== null && body.priceMax !== undefined) priceRangeQuery.$lte = body.priceMax;
            if (Object.keys(priceRangeQuery).length > 0) priceMatch.price = priceRangeQuery;
            matchConditions.push({ prices: { $elemMatch: priceMatch } });
        }
        
        const matchStage = matchConditions.length > 0 ? { $and: matchConditions } : {};
        
        // 注意: 此处的看板统计图 (Dashboard) 边界值是按照百分比 (0-100) 设定的。
        // 如果数据库中的 ratio 字段是小数 (0-1)，需要将这里的 boundaries 对应缩小100倍。
        // 当前的 boundaries 适用于 ratio 字段为百分比的情况。
        const facetPipeline = {
            paginatedResults: [ { $sort: { [sortBy]: sortOrder, _id: 1 } }, { $skip: (page - 1) * pageSize }, { $limit: pageSize }, { $project: { _id: 0 } } ],
            totalCount: [{ $count: 'count' }],
            tierDistribution: [ { $match: { talentTier: { $exists: true, $ne: null, $ne: "" } } }, { $group: { _id: "$talentTier", count: { $sum: 1 } } }, { $project: { _id: 0, tier: "$_id", count: 1 } } ],
            cpmDistribution: [ { $match: { 'performanceData.cpm60s': { $type: "number" } } }, { $bucket: { groupBy: "$performanceData.cpm60s", boundaries: [0, 10, 15, 20, 30, Infinity], default: "Other", output: { "count": { $sum: 1 } } } } ],
            maleAudienceDistribution: [ { $match: { 'performanceData.maleAudienceRatio': { $type: "number" } } }, { $bucket: { groupBy: "$performanceData.maleAudienceRatio", boundaries: [0, 0.4, 0.5, 0.6, 1.01], default: "Other", output: { "count": { $sum: 1 } } } } ],
            femaleAudienceDistribution: [ { $match: { 'performanceData.femaleAudienceRatio': { $type: "number" } } }, { $bucket: { groupBy: "$performanceData.femaleAudienceRatio", boundaries: [0, 0.4, 0.5, 0.6, 1.01], default: "Other", output: { "count": { $sum: 1 } } } } ]
        };

        const pipeline = [ { $match: matchStage }, { $facet: facetPipeline } ];
        const results = await collection.aggregate(pipeline).toArray();
        
        const facetResult = results[0] || {};
        const talents = facetResult.paginatedResults || [];
        const totalItems = (facetResult.totalCount && facetResult.totalCount[0]) ? facetResult.totalCount[0].count : 0;
        
        const responseBody = {
            success: true,
            data: {
                pagination: { page, pageSize, totalItems, totalPages: Math.ceil(totalItems / pageSize) },
                talents: talents,
                dashboardStats: {
                    tierDistribution: facetResult.tierDistribution || [],
                    cpmDistribution: facetResult.cpmDistribution || [],
                    maleAudienceDistribution: facetResult.maleAudienceDistribution || [],
                    femaleAudienceDistribution: facetResult.femaleAudienceDistribution || []
                }
            }
        };
        
        return { statusCode: 200, headers, body: JSON.stringify(responseBody) };

    } catch (error) {
        console.error('An error occurred:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
        };
    }
};

