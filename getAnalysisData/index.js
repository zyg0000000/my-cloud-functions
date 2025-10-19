/**
 * @file my-cloud-functions/getAnalysisData/index.js
 * @version 2.8 - Final Production Version with Status Filter
 * @description [最终修复版] 增加了对合作状态的筛选，确保所有财务指标计算的准确性。
 *
 * @changelog
 * - v2.8 (2025-10-20):
 * - [CRITICAL BUGFIX] 在聚合管道的起始位置增加了一个 $match 阶段，以确保所有计算都只基于状态为 "客户已定档" 或 "视频已发布" 的有效合作记录。此修复解决了所有KPI指标计算不准确的根本问题。
 * - v2.7 (2025-10-20):增加了对财务月份和客户月份两种时间维度的筛选支持。
 * - v2.6 (2025-10-20): 修复了 $lookup 阶段因变量引用错误导致的 "MongoServerError: $in needs an array" 崩溃问题。
 */
const { MongoClient } = require('mongodb');

// --- 数据库配置 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

// --- 集合名称 ---
const PROJECTS_COLLECTION = 'projects';
const COLLABS_COLLECTION = 'collaborations';
const TALENTS_COLLECTION = 'talents';
const RATES_COLLECTION = 'project_configurations'; 

let client;

// --- 数据库连接 ---
async function connectToDatabase() {
    if (client && client.topology && client.topology.isConnected()) {
        return client;
    }
    client = new MongoClient(MONGO_URI);
    await client.connect();
    return client;
}

// --- 辅助函数：安全地将字段转换为 Double ---
const safeToDouble = (field) => ({
    $cond: {
        if: { $and: [{ $ne: [field, null] }, { $ne: [field, ""] }] },
        then: { $toDouble: field },
        else: 0
    }
});
const safeToDoubleWithDefault = (field, defaultValue) => ({
    $cond: {
        if: { $and: [{ $ne: [field, null] }, { $ne: [field, ""] }] },
        then: { $toDouble: field },
        else: defaultValue
    }
});


// --- 主处理函数 ---
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

    const body = JSON.parse(event.body || '{}');
    const { filters = {}, talentSortBy = 'totalProfit', talentLimit = 20 } = body;

    try {
        const dbClient = await connectToDatabase();
        const db = dbClient.db(DB_NAME);
        const projectsCollection = db.collection(PROJECTS_COLLECTION);
        const collabsCollection = db.collection(COLLABS_COLLECTION);

        // --- 1. 获取可用的筛选选项 (轻量查询) ---
        const [availableYears, availableProjectTypes] = await Promise.all([
            projectsCollection.distinct('financialYear'),
            projectsCollection.distinct('type')
        ]);

        // --- 2. 构建主筛选条件 ---
        const matchStage = {};
        if (filters.projectType) matchStage['projectInfo.type'] = filters.projectType;
        
        if (filters.timeDimension === 'customer') {
            if (filters.year) matchStage['projectInfo.year'] = filters.year;
            if (filters.month) matchStage['projectInfo.month'] = filters.month;
        } else {
            if (filters.year) matchStage['projectInfo.financialYear'] = filters.year;
            if (filters.month) matchStage['projectInfo.financialMonth'] = filters.month;
        }

        // --- 3. 聚合管道 ---
        const aggregationResult = await collabsCollection.aggregate([
            // [CRITICAL BUGFIX] 步骤 1: 在所有计算开始前，严格筛选出有效的合作记录
            {
                $match: {
                    status: { $in: ["客户已定档", "视频已发布"] }
                }
            },
            // 步骤 2: 关联项目、费率、达人信息
            { $lookup: { from: PROJECTS_COLLECTION, localField: 'projectId', foreignField: 'id', as: 'projectInfo' } },
            { $unwind: '$projectInfo' },
            { $lookup: { from: RATES_COLLECTION, localField: 'projectInfo.capitalRateId', foreignField: 'values.id', as: 'capitalRateConfig' } },
            { $unwind: { path: '$capitalRateConfig', preserveNullAndEmptyArrays: true } },
            { $lookup: { from: TALENTS_COLLECTION, localField: 'talentId', foreignField: 'id', as: 'talentInfo' } },
            { $unwind: { path: '$talentInfo', preserveNullAndEmptyArrays: true } },
            // 步骤 3: 应用前端传入的筛选条件
            { $match: matchStage },
            // 步骤 4: 在每个合作记录层面计算所有财务指标
            {
                $addFields: {
                    'calculatedMetrics': {
                        'amountNum': safeToDouble('$amount'),
                        'rebateNum': safeToDouble('$rebate'),
                        'projectDiscountNum': safeToDoubleWithDefault('$projectInfo.discount', 1),
                        'actualRebateNum': { $cond: { if: { $in: ['$actualRebate', [null, ""]] }, then: null, else: { $toDouble: '$actualRebate' } } },
                        'monthlyRatePercent': { $let: { vars: { rateDoc: { $arrayElemAt: ['$capitalRateConfig.values', 0] } }, in: { $ifNull: [safeToDouble('$$rateDoc.value'), 0.7] } } },
                        'orderDateObj': { $cond: { if: { $and: [{$ne: ['$orderDate', null]}, {$ne: ['$orderDate', ""]}] }, then: { $toDate: '$orderDate' }, else: null } },
                        'paymentDateObj': { $cond: { if: { $and: [{$ne: ['$paymentDate', null]}, {$ne: ['$paymentDate', ""]}] }, then: { $toDate: '$paymentDate' }, else: new Date() } }
                    }
                }
            },
            {
                $addFields: {
                    'calculatedMetrics.income': { $multiply: ['$calculatedMetrics.amountNum', '$calculatedMetrics.projectDiscountNum', 1.05] },
                    'calculatedMetrics.expense': { $cond: { if: { $eq: ['$orderType', 'original'] }, then: { $multiply: ['$calculatedMetrics.amountNum', 1.05] }, else: { $cond: { if: { $gt: ['$calculatedMetrics.rebateNum', 20] }, then: { $multiply: ['$calculatedMetrics.amountNum', 0.8, 1.05] }, else: { $multiply: ['$calculatedMetrics.amountNum', { $subtract: [1, { $divide: ['$calculatedMetrics.rebateNum', 100] }] }, 1.05] } } } } },
                    'calculatedMetrics.rebateReceivable': { $cond: { if: { $eq: ['$orderType', 'original'] }, then: { $multiply: ['$calculatedMetrics.amountNum', { $divide: ['$calculatedMetrics.rebateNum', 100] }] }, else: { $cond: { if: { $gt: ['$calculatedMetrics.rebateNum', 20] }, then: { $multiply: ['$calculatedMetrics.amountNum', { $subtract: [{ $divide: ['$calculatedMetrics.rebateNum', 100] }, 0.20] }] }, else: 0 } } } }
                }
            },
            {
                $addFields: {
                    'calculatedMetrics.occupationDays': { $ifNull: [ { $cond: { if: '$calculatedMetrics.orderDateObj', then: { $max: [0, { $divide: [{ $subtract: ['$calculatedMetrics.paymentDateObj', '$calculatedMetrics.orderDateObj'] }, 1000 * 60 * 60 * 24] }] }, else: 0 }}, 0 ] }
                }
            },
            {
                $addFields: {
                    'calculatedMetrics.fundsOccupationCost': { $multiply: [ '$calculatedMetrics.expense', { $divide: [ { $divide: ['$calculatedMetrics.monthlyRatePercent', 100] }, 30 ] }, '$calculatedMetrics.occupationDays' ] },
                    'calculatedMetrics.rebateForProfitCalc': { $cond: { if: { $eq: ['$projectInfo.status', '已终结'] }, then: { $ifNull: ['$calculatedMetrics.actualRebateNum', 0] }, else: { $ifNull: ['$calculatedMetrics.actualRebateNum', '$calculatedMetrics.rebateReceivable'] } } }
                }
            },
            {
                $addFields: {
                    'calculatedMetrics.grossProfit': { $add: ['$calculatedMetrics.income', '$calculatedMetrics.rebateForProfitCalc', { $multiply: ['$calculatedMetrics.expense', -1] }] }
                }
            },
            // --- 步骤 5: 使用 $facet 进行多维度并行聚合 ---
            {
                $facet: {
                    kpiSummary: [
                        {
                            $group: {
                                _id: null,
                                projectIds: { $addToSet: '$projectId' },
                                collaboratorIds: { $addToSet: '$id' },
                                totalIncomeAgg: { $sum: '$calculatedMetrics.income' },
                                totalExpense: { $sum: '$calculatedMetrics.expense' },
                                preAdjustmentProfitAgg: { $sum: '$calculatedMetrics.grossProfit' },
                                fundsOccupationCost: { $sum: '$calculatedMetrics.fundsOccupationCost' },
                            }
                        },
                        {
                           $lookup: {
                                from: PROJECTS_COLLECTION,
                                let: { p_ids: "$projectIds" },
                                pipeline: [
                                    { $match: { $expr: { $in: ["$id", { $ifNull: ["$$p_ids", []] }] } } },
                                    { $project: { _id: 0, adjustments: 1, budget: 1 } }
                                ],
                                as: 'relatedProjects'
                            }
                        },
                        {
                            $addFields: {
                                incomeAdjustments: { $sum: { $map: { input: '$relatedProjects', as: 'proj', in: { $sum: { $map: { input: '$$proj.adjustments', as: 'adj', in: { $cond: [ { $gt: [ safeToDouble('$$adj.amount'), 0 ] }, safeToDouble('$$adj.amount'), 0 ] } } } } } } },
                                expenseAdjustments: { $abs: { $sum: { $map: { input: '$relatedProjects', as: 'proj', in: { $sum: { $map: { input: '$$proj.adjustments', as: 'adj', in: { $cond: [ { $lt: [ safeToDouble('$$adj.amount'), 0 ] }, safeToDouble('$$adj.amount'), 0 ] } } } } } } } },
                                totalBudget: { $sum: { $map: { input: '$relatedProjects', as: 'proj', in: safeToDouble('$$proj.budget') } } }
                            }
                        },
                        {
                            $project: {
                                _id: 0,
                                totalProjects: { $ifNull: [{ $size: '$projectIds' }, 0] },
                                totalCollaborators: { $ifNull: [{ $size: '$collaboratorIds' }, 0] },
                                totalIncomeAgg: '$totalIncomeAgg',
                                incomeAdjustments: '$incomeAdjustments',
                                totalIncome: { $add: ['$totalIncomeAgg', '$incomeAdjustments'] },
                                preAdjustmentProfit: { $add: ['$preAdjustmentProfitAgg', '$incomeAdjustments'] },
                                fundsOccupationCost: '$fundsOccupationCost',
                                expenseAdjustments: '$expenseAdjustments',
                                totalExpense: '$totalExpense',
                                totalOperationalCost: { $add: ['$totalExpense', '$expenseAdjustments', '$fundsOccupationCost'] },
                                operationalProfit: { $subtract: [ { $add: ['$preAdjustmentProfitAgg', '$incomeAdjustments'] }, { $add: ['$expenseAdjustments', '$fundsOccupationCost'] } ] }
                            }
                        },
                        {
                            $project: {
                                totalProjects: 1, totalCollaborators: 1, totalIncomeAgg: 1, incomeAdjustments: 1, totalIncome: 1,
                                preAdjustmentProfit: 1, fundsOccupationCost: 1, expenseAdjustments: 1, totalExpense: 1,
                                totalOperationalCost: 1, operationalProfit: 1,
                                preAdjustmentMargin: { $cond: { if: { $gt: ['$totalIncome', 0] }, then: { $multiply: [{ $divide: ['$preAdjustmentProfit', '$totalIncome'] }, 100] }, else: 0 } },
                                operationalMargin: { $cond: { if: { $gt: ['$totalIncome', 0] }, then: { $multiply: [{ $divide: ['$operationalProfit', '$totalIncome'] }, 100] }, else: 0 } },
                                budgetUtilization: { $cond: { if: { $gt: ['$totalBudget', 0] }, then: { $multiply: [{ $divide: ['$totalIncome', '$totalBudget'] }, 100] }, else: 0 } },
                            }
                        }
                    ],
                    monthlyFinancials: [
                        { $group: { _id: { month: '$projectInfo.financialMonth' }, totalIncome: { $sum: '$calculatedMetrics.income' }, totalProfit: { $sum: '$calculatedMetrics.grossProfit' } } },
                        { $addFields: { monthNum: { $toInt: { $substr: ['$_id.month', 1, -1] } } } },
                        { $sort: { monthNum: 1 } },
                        { $project: { _id: 0, month: '$_id.month', totalIncome: 1, totalProfit: 1, margin: { $cond: { if: { $gt: ['$totalIncome', 0] }, then: { $multiply: [{ $divide: ['$totalProfit', '$totalIncome'] }, 100] }, else: 0 } } } }
                    ],
                    byProjectType: [
                        { $group: { _id: '$projectInfo.type', totalIncome: { $sum: '$calculatedMetrics.income' } } },
                        { $project: { _id: 0, projectType: '$_id', totalIncome: 1 } },
                        { $sort: { totalIncome: -1 } }
                    ],
                    topTalents: [
                        { $group: { _id: '$talentId', talentName: { $first: '$talentInfo.nickname' }, collaborationCount: { $sum: 1 }, totalAmount: { $sum: '$calculatedMetrics.amountNum' }, totalProfit: { $sum: '$calculatedMetrics.grossProfit' }, totalRebate: { $sum: '$calculatedMetrics.rebateNum' } } },
                        { $sort: { [talentSortBy]: -1 } },
                        { $limit: talentLimit },
                        { $project: { _id: 0, talentName: 1, collaborationCount: 1, totalAmount: 1, totalProfit: 1, averageRebate: { $cond: { if: { $gt: ['$collaborationCount', 0] }, then: { $divide: ['$totalRebate', '$collaborationCount'] }, else: 0 } } } }
                    ]
                }
            }
        ]).toArray();
        
        const result = aggregationResult[0];

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                data: {
                    availableFilters: {
                        years: availableYears.filter(y => y).sort((a, b) => b - a),
                        projectTypes: availableProjectTypes.filter(t => t).sort()
                    },
                    kpiSummary: result.kpiSummary[0] || {},
                    monthlyFinancials: result.monthlyFinancials || [],
                    byProjectType: result.byProjectType || [],
                    topTalents: result.topTalents || []
                }
            }),
        };

    } catch (error) {
        console.error('Error in getAnalysisData handler:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }),
        };
    }
};

