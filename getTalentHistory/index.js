/**
 * getTalentHistory - 云函数（优化版 - 修正月份格式问题）
 * 功能：查询某个达人在其他项目中的合作历史记录
 *
 * 请求参数：
 * - talentId: 达人ID（必需）
 * - excludeProjectId: 要排除的项目ID（可选，通常是当前项目）
 *
 * 返回数据：
 * - success: 是否成功
 * - data: 历史记录数组，包含项目名称、年月、金额、状态、返点率
 */

const { MongoClient } = require('mongodb');

// MongoDB 连接配置
const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.DB_NAME || 'kol_data';

let cachedClient = null;

/**
 * 获取 MongoDB 客户端（复用连接）
 */
async function getMongoClient() {
    if (cachedClient && cachedClient.topology && cachedClient.topology.isConnected()) {
        return cachedClient;
    }

    cachedClient = new MongoClient(MONGODB_URI, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        maxPoolSize: 10,
    });

    await cachedClient.connect();
    return cachedClient;
}

/**
 * 主处理函数
 */
exports.handler = async (event, context) => {
    // CORS 预检请求处理
    if (event.httpMethod === 'OPTIONS') {
        return {
            statusCode: 200,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
            },
            body: '',
        };
    }

    try {
        // 解析请求参数
        let params = {};

        if (event.httpMethod === 'GET') {
            params = event.queryStringParameters || {};
        } else if (event.httpMethod === 'POST') {
            try {
                params = JSON.parse(event.body || '{}');
            } catch (e) {
                params = event.queryStringParameters || {};
            }
        }

        const { talentId, excludeProjectId } = params;

        // 参数验证
        if (!talentId) {
            return {
                statusCode: 400,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                },
                body: JSON.stringify({
                    success: false,
                    message: '缺少必需参数: talentId',
                }),
            };
        }

        console.log(`查询达人历史 - talentId: ${talentId}${excludeProjectId ? `, 排除项目: ${excludeProjectId}` : ''}`);

        // 连接数据库
        const client = await getMongoClient();
        const db = client.db(DB_NAME);
        const collaborationsCollection = db.collection('collaborations');

        // 构建聚合管道
        const pipeline = [
            // 第一步：匹配该达人的所有合作记录
            {
                $match: {
                    talentId: talentId,
                    // 只保留"视频已发布"和"客户已定档"状态
                    status: { $in: ['视频已发布', '客户已定档'] },
                    // 如果提供了 excludeProjectId，则排除该项目
                    ...(excludeProjectId ? { projectId: { $ne: excludeProjectId } } : {}),
                }
            },

            // 第二步：关联 projects 集合，获取项目详情
            {
                $lookup: {
                    from: 'projects',
                    localField: 'projectId',
                    foreignField: 'id',
                    as: 'projectInfo'
                }
            },

            // 第三步：解构 projectInfo 数组
            {
                $unwind: {
                    path: '$projectInfo',
                    preserveNullAndEmptyArrays: true  // 保留没有匹配到项目的记录
                }
            },

            // 第四步：投影需要的字段，并转换年月为数字（处理 "M8" 格式）
            {
                $project: {
                    _id: 0,
                    projectId: 1,
                    projectName: { $ifNull: ['$projectInfo.name', '未知项目'] },
                    projectYear: { $ifNull: ['$projectInfo.financialYear', ''] },
                    projectMonth: { $ifNull: ['$projectInfo.financialMonth', ''] },
                    // 用于排序的数字字段
                    sortYear: {
                        $convert: {
                            input: '$projectInfo.financialYear',
                            to: 'int',
                            onError: 0,
                            onNull: 0
                        }
                    },
                    sortMonth: {
                        $convert: {
                            input: {
                                // 去掉 "M" 前缀（如果存在）
                                $cond: {
                                    if: {
                                        $and: [
                                            { $ne: ['$projectInfo.financialMonth', null] },
                                            { $eq: [{ $substr: ['$projectInfo.financialMonth', 0, 1] }, 'M'] }
                                        ]
                                    },
                                    then: { $substr: ['$projectInfo.financialMonth', 1, -1] },
                                    else: '$projectInfo.financialMonth'
                                }
                            },
                            to: 'int',
                            onError: 0,
                            onNull: 0
                        }
                    },
                    amount: { $ifNull: ['$amount', 0] },
                    status: { $ifNull: ['$status', '未知'] },
                    rebate: '$rebate',  // 返点率（保持原始值，包括null）
                    orderDate: 1,
                    paymentDate: 1,
                    recoveryDate: 1,
                }
            },

            // 第五步：按项目年月降序排序（最新的在前）
            {
                $sort: {
                    sortYear: -1,
                    sortMonth: -1,
                    orderDate: -1
                }
            },

            // 第六步：移除排序用的临时字段
            {
                $project: {
                    sortYear: 0,
                    sortMonth: 0
                }
            }
        ];

        // 执行查询
        const historyRecords = await collaborationsCollection.aggregate(pipeline).toArray();

        console.log(`找到 ${historyRecords.length} 条有效历史记录`);

        // 返回结果
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            body: JSON.stringify({
                success: true,
                data: historyRecords,
                message: historyRecords.length > 0
                    ? `找到 ${historyRecords.length} 条历史记录`
                    : '未找到该达人的其他合作历史',
            }),
        };

    } catch (error) {
        console.error('getTalentHistory 错误:', error.message);

        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            body: JSON.stringify({
                success: false,
                message: '服务器内部错误',
                error: error.message,
            }),
        };
    }
};