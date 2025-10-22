/**
 * @file exportComprehensiveData/index.js
 * @version 1.2
 * @description "数据导出中心"后端核心云函数 (多主体聚合引擎)。
 * - 支持 'talent', 'collaboration', 'project' 多种导出主体 (entity)。
 * - 根据前端请求的 entity, fields, filters 动态构建 MongoDB Aggregation Pipeline。
 * - 聚合来自 talents, collaborations, works, projects, automation-tasks 多个集合的数据。
 */

const { MongoClient } = require('mongodb');

// --- 配置 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

// --- 数据库连接 ---
let client;
async function connectToDatabase() {
    if (client && client.topology && client.topology.isConnected()) {
        return client;
    }
    client = new MongoClient(MONGO_URI);
    await client.connect();
    return client;
}

/**
 * 构建用于“按达人导出”的聚合管道。
 * @param {Db} db - MongoDB 数据库实例。
 * @param {string[]} fields - 前端请求导出的字段ID列表。
 * @param {object} filters - 前端传递的筛选条件。
 * @returns {Array<object>} MongoDB 聚合管道阶段数组。
 */
function buildTalentPipeline(db, fields, filters) {
    const pipeline = [];
    const matchStage = {};

    // 1. 构建筛选 ($match)
    if (filters.search) {
        matchStage.nickname = { $regex: filters.search, $options: 'i' };
    }
    if (filters.tiers && filters.tiers.length > 0) {
        matchStage.talentTier = { $in: filters.tiers };
    }
    if (Object.keys(matchStage).length > 0) {
        pipeline.push({ $match: matchStage });
    }

    // 2. 按需关联 ($lookup)
    const requiredLookups = new Set();
    fields.forEach(field => {
        if (field.startsWith('collaboration_')) requiredLookups.add('collaborations');
        if (field.startsWith('work_')) requiredLookups.add('works');
    });

    if (requiredLookups.has('collaborations') || requiredLookups.has('works')) {
        pipeline.push({
            $lookup: {
                from: 'collaborations',
                localField: 'id',
                foreignField: 'talentId',
                as: 'collaborations'
            }
        });
    }
    
    if (requiredLookups.has('works')) {
         pipeline.push({
            $lookup: {
                from: 'works',
                localField: 'id',
                foreignField: 'talentId',
                as: 'works' // 注意：这里简化为直接关联所有作品
            }
        });
    }

    // 3. 构建投影 ($project)
    const projectStage = { _id: 0 };
    fields.forEach(field => {
        switch (field) {
            case 'nickname': projectStage['达人昵称'] = '$nickname'; break;
            case 'xingtuId': projectStage['星图ID'] = '$xingtuId'; break;
            case 'uid': projectStage['UID'] = '$uid'; break;
            case 'talentTier': projectStage['达人层级'] = '$talentTier'; break;
            case 'talentSource': projectStage['达人来源'] = '$talentSource'; break;
            case 'talentType': projectStage['内容标签'] = { $ifNull: [ '$talentType', [] ] }; break;
            case 'price':
                // 简化处理：获取指定月份的价格
                const [year, month] = (filters.timeMonth || '2025-10').split('-').map(Number);
                projectStage['一口价'] = {
                    $let: {
                        vars: {
                            filteredPrices: {
                                $filter: {
                                    input: '$prices',
                                    as: 'p',
                                    cond: { $and: [{ $eq: ['$$p.year', year] }, { $eq: ['$$p.month', month] }] }
                                }
                            }
                        },
                        in: { $ifNull: [{ $arrayElemAt: ['$$filteredPrices.price', 0] }, 'N/A'] }
                    }
                };
                break;
            case 'highestRebate': projectStage['最高返点率'] = { $ifNull: [{ $max: '$rebates.rate' }, 0] }; break;
            case 'collaboration_count': projectStage['历史合作总次数'] = { $size: '$collaborations' }; break;
            case 'work_total_t7_views': projectStage['T+7 总播放量'] = { $sum: '$works.t7_totalViews' }; break;
            // 在此添加更多字段映射...
            default:
                if (field.startsWith('ratio_') || field.endsWith('Ratio') || field === 'cpm60s') {
                    const displayName = field; // 简单处理，实际应有映射表
                    projectStage[displayName] = `$performanceData.${field}`;
                }
                break;
        }
    });
    pipeline.push({ $project: projectStage });

    return pipeline;
}

/**
 * 构建用于“按合作/项目导出”的聚合管道。
 * @param {Db} db - MongoDB 数据库实例。
 * @param {string[]} fields - 前端请求导出的字段ID列表。
 * @param {object} filters - 前端传递的筛选条件。
 * @param {string} entity - 'collaboration' 或 'project'。
 * @returns {Array<object>} MongoDB 聚合管道阶段数组。
 */
function buildCollaborationPipeline(db, fields, filters, entity) {
    const pipeline = [];
    const matchStage = {};

    // 1. 构建筛选 ($match)
    if (entity === 'project' && filters.projectIds && filters.projectIds.length > 0) {
        matchStage.projectId = { $in: filters.projectIds };
    }
    if (filters.status && filters.status.length > 0) {
        matchStage.status = { $in: filters.status };
    }
    // 添加更多合作维度的筛选...
    if (Object.keys(matchStage).length > 0) {
        pipeline.push({ $match: matchStage });
    }

    // 2. 关联其他集合 ($lookup)
    pipeline.push({ $lookup: { from: 'talents', localField: 'talentId', foreignField: 'id', as: 'talentInfo' } });
    pipeline.push({ $lookup: { from: 'projects', localField: 'projectId', foreignField: 'id', as: 'projectInfo' } });
    pipeline.push({ $lookup: { from: 'works', localField: 'id', foreignField: 'collaborationId', as: 'workInfo' } });

    // 3. 展开关联的数组 (使用 $unwind)
    pipeline.push({ $unwind: { path: '$talentInfo', preserveNullAndEmptyArrays: true } });
    pipeline.push({ $unwind: { path: '$projectInfo', preserveNullAndEmptyArrays: true } });
    pipeline.push({ $unwind: { path: '$workInfo', preserveNullAndEmptyArrays: true } });
    
    // 4. 构建投影 ($project)
    const projectStage = { _id: 0 };
    fields.forEach(field => {
        // 这是一个简化的映射，实际项目中会更复杂
        switch (field) {
            case 'collaboration_status': projectStage['合作状态'] = '$status'; break;
            case 'collaboration_amount': projectStage['合作金额'] = '$amount'; break;
            case 'project_name': projectStage['项目名称'] = '$projectInfo.name'; break;
            case 'nickname': projectStage['达人昵称'] = '$talentInfo.nickname'; break;
            case 'talentTier': projectStage['达人层级'] = '$talentInfo.talentTier'; break;
            case 'work_total_t7_views': projectStage['T+7 播放量'] = '$workInfo.t7_totalViews'; break;
            // 在此添加更多字段映射...
        }
    });
    pipeline.push({ $project: projectStage });

    return pipeline;
}

// --- 云函数主入口 ---
exports.handler = async (event) => {
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
        const db = dbClient.db(DB_NAME);

        const { entity, fields, filters } = JSON.parse(event.body || '{}');

        if (!entity || !fields || !filters) {
            return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体缺少 entity, fields 或 filters 参数。' }) };
        }

        let primaryCollection;
        let pipeline = [];

        switch (entity) {
            case 'talent':
                primaryCollection = db.collection('talents');
                pipeline = buildTalentPipeline(db, fields, filters);
                break;
            case 'collaboration':
            case 'project':
                primaryCollection = db.collection('collaborations');
                pipeline = buildCollaborationPipeline(db, fields, filters, entity);
                break;
            default:
                throw new Error(`无效的导出主体: ${entity}`);
        }
        
        console.log("Executing pipeline:", JSON.stringify(pipeline, null, 2));

        const results = await primaryCollection.aggregate(pipeline).toArray();

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: true, data: results })
        };

    } catch (error) {
        console.error('Data export error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
        };
    }
};

