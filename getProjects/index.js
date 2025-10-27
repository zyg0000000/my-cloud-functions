/**
 * @file getprojects_2.js
 * @version 4.8.2-tracking-enabled
 * @description 添加了对 trackingEnabled 字段的支持
 * * --- 更新日志 (v4.8.2) ---
 * - [新增字段] 在 simple 和 full 视图中都返回 trackingEnabled 字段
 * - [目的] 支持前端根据此字段控制"效果追踪"功能的显示
 * * --- 更新日志 (v4.8.1) ---
 * - [修复] 使用 $unset 移除 talentsData，避免 MongoDB projection 混合使用 inclusion/exclusion 错误
 * * --- 更新日志 (v4.8) ---
 * - [关键修复] 在聚合管道中添加了 talents 集合的关联查询
 * - [目的] 确保返回的每个 collaboration 对象都包含完整的 talentInfo 字段
 * - [实现] 使用 $lookup 关联 talents，然后用 $map 将 talentInfo 嵌入到每个 collaboration
 * - [影响] 修复了前端显示"(未知达人)"的问题
 * * --- 更新日志 (v4.7) ---
 * - [核心改造] 在完整视图（非 simple view）的聚合管道中，最后的 $project 阶段新增了 `collaborations: 1`。
 * - [目的] 使此接口在返回项目详情和财务指标的同时，也返回所有关联的合作记录。
 * - [优化] 这使得 `order_list.html` 页面可以一次性获取所有数据，避免了主页面和每个Tab模块重复调用 `/collaborations` 接口。
 * - [兼容性] `view=simple` 逻辑保持不变，确保 `index.html` 页面功能不受影响。
 * * --- 历史更新 (v4.6) ---
 * - [核心BUG修复] 修复了资金占用费用因按秒计算导致每次刷新值都变化的问题。
 */
const { MongoClient } = require('mongodb');

// --- 环境变量与数据库连接 (保持不变) ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const PROJECTS_COLLECTION = 'projects';
const COLLABS_COLLECTION = 'collaborations';
const TALENTS_COLLECTION = 'talents'; // [v4.8 新增]
const RATES_COLLECTION = 'projectCapitalRates';
const API_GATEWAY_BASE_URL = process.env.API_GATEWAY_BASE_URL;

let client;
async function connectToDatabase() {
  if (client && client.topology && client.topology.isConnected()) {
    return client;
  }
  client = new MongoClient(MONGO_URI);
  await client.connect();
  return client;
}

// [v4.6 修正] 统一费率集合名称
const CAPITAL_RATES_COLLECTION = 'project_configurations';

const safeToDouble = (field) => ({
    $cond: {
        if: { $and: [ { $ne: [field, null] }, { $ne: [field, ""] } ] },
        then: { $toDouble: field },
        else: 0
    }
});
const safeToDoubleWithDefault = (field, defaultValue) => ({
    $cond: {
        if: { $and: [ { $ne: [field, null] }, { $ne: [field, ""] } ] },
        then: { $toDouble: field },
        else: defaultValue
    }
});


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

  if (!API_GATEWAY_BASE_URL) {
      console.error("严重错误: 环境变量 API_GATEWAY_BASE_URL 未配置。");
      return {
          statusCode: 500,
          headers,
          body: JSON.stringify({ success: false, message: "服务器配置不完整，无法生成预览链接。" })
      };
  }

  try {
    let queryParams = {};
    if (event.queryStringParameters) { queryParams = event.queryStringParameters; }
    if (event.body) { try { Object.assign(queryParams, JSON.parse(event.body)); } catch (e) { /* ignore */ } }

    const { projectId, view } = queryParams;

    const dbClient = await connectToDatabase();
    const projectsCollection = dbClient.db(DB_NAME).collection(PROJECTS_COLLECTION);

    const baseMatch = projectId ? { id: projectId } : {};

    let projectsData;

    // --- [兼容性保留] view=simple 逻辑完全不变 ---
    if (view === 'simple') {
      projectsData = await projectsCollection.find(baseMatch, {
        // [v4.8.2 修改] 添加 trackingEnabled 到简单视图
        projection: { _id: 0, id: 1, name: 1, status: 1, trackingEnabled: 1 }
      }).toArray();
    } else {
      // --- 完整视图逻辑 ---
      const aggregationPipeline = [
        { $match: baseMatch },
        { $project: { _id: 0 } },

        // [v4.7] 关联 collaborations
        {
          $lookup: {
            from: COLLABS_COLLECTION,
            localField: 'id',
            foreignField: 'projectId',
            as: 'collaborations'
          }
        },

        // [v4.8 新增] 关联 talents 集合以获取达人信息
        {
          $lookup: {
            from: TALENTS_COLLECTION,
            localField: 'collaborations.talentId',
            foreignField: 'id',  // 假设 talents 集合使用 'id' 作为主键
            as: 'talentsData'
          }
        },

        // [v4.8 新增] 将 talentInfo 嵌入到每个 collaboration 中
        {
          $addFields: {
            collaborations: {
              $map: {
                input: '$collaborations',
                as: 'collab',
                in: {
                  $mergeObjects: [
                    '$$collab',
                    {
                      talentInfo: {
                        $arrayElemAt: [
                          {
                            $filter: {
                              input: '$talentsData',
                              as: 'talent',
                              cond: { $eq: ['$$talent.id', '$$collab.talentId'] }
                            }
                          },
                          0
                        ]
                      }
                    }
                  ]
                }
              }
            }
          }
        },

        // [v4.6 修正] 关联正确的配置集合
        { $lookup: { from: CAPITAL_RATES_COLLECTION, localField: 'capitalRateId', foreignField: 'values.id', as: 'capitalRateInfo' } },
        { $unwind: { path: '$capitalRateInfo', preserveNullAndEmptyArrays: true } },
        {
          $addFields: {
            // [v4.6 修正] 从 capitalRateInfo.values 中正确提取费率
            monthlyRatePercent: {
              $let: {
                 vars: {
                    rateDoc: {
                       $arrayElemAt: [
                           { $filter: { input: "$capitalRateInfo.values", as: "rate", cond: { $eq: [ "$$rate.id", "$capitalRateId" ] } } }, 0
                       ]
                    }
                 },
                 in: { $ifNull: [ safeToDouble('$$rateDoc.value'), 0.7 ] }
              }
            },
            confirmedCollaborations: { $filter: { input: '$collaborations', as: 'collab', cond: { $in: ['$$collab.status', ['客户已定档', '视频已发布']] } } }
          }
        },
        {
          $addFields: {
            calculatedMetricsPerCollab: {
              $map: {
                input: '$confirmedCollaborations',
                as: 'c',
                in: {
                  income: { $multiply: [ safeToDouble('$$c.amount'), safeToDoubleWithDefault('$discount', 1), 1.05 ] },
                  expense: { $let: {
                      vars: { amountNum: safeToDouble('$$c.amount'), rebateNum: safeToDouble('$$c.rebate') },
                      in: { $cond: { if: { $eq: ['$$c.orderType', 'original'] }, then: { $multiply: ['$$amountNum', 1.05] }, else: { $cond: { if: { $gt: ['$$rebateNum', 20] }, then: { $multiply: ['$$amountNum', 0.8, 1.05] }, else: { $multiply: ['$$amountNum', { $subtract: [1, { $divide: ['$$rebateNum', 100] }] }, 1.05] } } } } }
                  }},
                  rebateForProfitCalc: { $let: {
                      vars: {
                          actualRebate: { $cond: { if: { $ne: ['$$c.actualRebate', null] }, then: safeToDouble('$$c.actualRebate'), else: null } },
                          amountNum: safeToDouble('$$c.amount'),
                          rebateNum: safeToDouble('$$c.rebate')
                      },
                      in: { $cond: { if: { $eq: ['$status', '已终结'] }, then: { $ifNull: ['$$actualRebate', 0] }, else: { $ifNull: ['$$actualRebate', { $cond: { if: { $eq: ['$$c.orderType', 'original'] }, then: { $multiply: ['$$amountNum', { $divide: ['$$rebateNum', 100] }] }, else: { $cond: { if: { $gt: ['$$rebateNum', 20] }, then: { $multiply: ['$$amountNum', { $subtract: [{ $divide: ['$$rebateNum', 100] }, 0.20] }] }, else: 0 } } } } ] } } }
                  }},
                  // --- [v4.6 修复] ---
                  occupationDays: {
                    $let: {
                      vars: {
                        startDate: { $cond: { if: { $and: [{$ne: ['$$c.orderDate', null]}, {$ne: ['$$c.orderDate', ""]}] }, then: { $toDate: '$$c.orderDate' }, else: null } },
                        endDate: {
                            $cond: {
                                if: { $and: [{$ne: ['$$c.paymentDate', null]}, {$ne: ['$$c.paymentDate', ""]}] },
                                then: { $toDate: '$$c.paymentDate' },
                                else: { // 使用今天的日期，但移除时间部分
                                    $dateFromParts: {
                                        'year': { $year: new Date() },
                                        'month': { $month: new Date() },
                                        'day': { $dayOfMonth: new Date() },
                                        'timezone': 'Asia/Shanghai'
                                    }
                                }
                            }
                        }
                      },
                      in: { $ifNull: [ { $max: [0, { $divide: [ { $subtract: ['$$endDate', '$$startDate'] }, 1000 * 60 * 60 * 24 ] }] }, 0 ] }
                    }
                  }
                  // --- 修复结束 ---
                }
              }
            }
          }
        },
        {
          $addFields: {
            calculatedMetricsPerCollab: {
              $map: {
                input: '$calculatedMetricsPerCollab',
                as: 'm',
                in: {
                  income: '$$m.income',
                  expense: '$$m.expense',
                  rebateForProfitCalc: '$$m.rebateForProfitCalc',
                  occupationDays: '$$m.occupationDays',
                  fundsOccupationCost: {
                    $multiply: [
                      '$$m.expense',
                      { $divide: [ { $divide: ['$monthlyRatePercent', 100] }, 30 ] },
                      '$$m.occupationDays'
                    ]
                  }
                }
              }
            }
          }
        },
        {
          $addFields: {
            'metrics.projectBudget': safeToDouble('$budget'),
            'metrics.totalCollaborators': { $size: '$confirmedCollaborations' },
            'metrics.totalIncomeAgg': { $sum: '$calculatedMetricsPerCollab.income' },
            'metrics.totalExpense': { $sum: '$calculatedMetricsPerCollab.expense' },
            'metrics.totalGrossProfit': { $sum: { $map: { input: '$calculatedMetricsPerCollab', as: 'm', in: { $add: ['$$m.income', '$$m.rebateForProfitCalc', { $multiply: ['$$m.expense', -1] }] } } } },
            'metrics.fundsOccupationCost': { $sum: '$calculatedMetricsPerCollab.fundsOccupationCost' },
            'metrics.incomeAdjustments': { $ifNull: [{ $sum: { $map: { input: '$adjustments', as: 'adj', in: { $cond: [{ $gt: [safeToDouble('$$adj.amount'), 0] }, safeToDouble('$$adj.amount'), 0] } } } }, 0] },
            'metrics.expenseAdjustments': { $abs: { $ifNull: [{ $sum: { $map: { input: '$adjustments', as: 'adj', in: { $cond: [{ $lt: [safeToDouble('$$adj.amount'), 0] }, safeToDouble('$$adj.amount'), 0] } } } }, 0] } }
          }
        },
        {
          $addFields: {
            'metrics.totalIncome': { $add: ['$metrics.totalIncomeAgg', '$metrics.incomeAdjustments'] },
            'metrics.totalRebateReceivable': { $sum: '$calculatedMetricsPerCollab.rebateForProfitCalc' }
          }
        },
        {
           $addFields: {
                'metrics.budgetUtilization': { $cond: [{ $gt: ['$metrics.projectBudget', 0] }, { $multiply: [{ $divide: ['$metrics.totalIncome', '$metrics.projectBudget'] }, 100] }, 0] },
                'metrics.preAdjustmentProfit': { $add: ['$metrics.totalGrossProfit', '$metrics.incomeAdjustments'] },
                'metrics.totalOperationalCost': { $add: ['$metrics.totalExpense', '$metrics.expenseAdjustments', '$metrics.fundsOccupationCost'] },
           }
        },
        {
            $addFields: {
                'metrics.preAdjustmentMargin': { $cond: [{ $gt: ['$metrics.totalIncome', 0] }, { $multiply: [{ $divide: ['$metrics.preAdjustmentProfit', '$metrics.totalIncome'] }, 100] }, 0] },
                'metrics.operationalProfit': { $subtract: ['$metrics.preAdjustmentProfit', { $add: ['$metrics.expenseAdjustments', '$metrics.fundsOccupationCost'] }] }
            }
        },
        {
             $addFields: {
                'metrics.operationalMargin': { $cond: [{ $gt: ['$metrics.totalIncome', 0] }, { $multiply: [{ $divide: ['$metrics.operationalProfit', '$metrics.totalIncome'] }, 100] }, 0] }
             }
        },
        {
            $addFields: {
                "metrics.projectBudget": { $round: ["$metrics.projectBudget", 2] },
                "metrics.budgetUtilization": { $round: ["$metrics.budgetUtilization", 2] },
                "metrics.totalIncome": { $round: ["$metrics.totalIncome", 2] },
                "metrics.totalRebateReceivable": { $round: ["$metrics.totalRebateReceivable", 2] },
                "metrics.incomeAdjustments": { $round: ["$metrics.incomeAdjustments", 2] },
                "metrics.totalExpense": { $round: ["$metrics.totalExpense", 2] },
                "metrics.totalGrossProfit": { $round: ["$metrics.totalGrossProfit", 2] },
                "metrics.fundsOccupationCost": { $round: ["$metrics.fundsOccupationCost", 2] },
                "metrics.expenseAdjustments": { $round: ["$metrics.expenseAdjustments", 2] },
                "metrics.totalOperationalCost": { $round: ["$metrics.totalOperationalCost", 2] },
                "metrics.preAdjustmentProfit": { $round: ["$metrics.preAdjustmentProfit", 2] },
                "metrics.preAdjustmentMargin": { $round: ["$metrics.preAdjustmentMargin", 2] },
                "metrics.operationalProfit": { $round: ["$metrics.operationalProfit", 2] },
                "metrics.operationalMargin": { $round: ["$metrics.operationalMargin", 2] }
            }
        },
        {
            $addFields: {
                projectFiles: {
                    $map: {
                        input: { $ifNull: ["$projectFiles", []] },
                        as: "file",
                        in: {
                            name: "$$file.name",
                            url: {
                                $let: {
                                    vars: { urlParts: { $split: ["$$file.url", "/"] } },
                                    in: { $concat: [ API_GATEWAY_BASE_URL, "/preview-file?fileKey=", { $arrayElemAt: ["$$urlParts", -1] } ] }
                                }
                            }
                        }
                    }
                }
            }
        },
        // [v4.8] 先移除临时的 talentsData 字段
        { $unset: 'talentsData' },

        {
          $project: {
             _id: 0, id: 1, name: 1, qianchuanId: 1, type: 1, year: 1, month: 1, financialYear: 1, financialMonth: 1,
            status: 1, discount: 1, capitalRateId: 1, adjustments: 1, auditLog: 1, createdAt: 1, updatedAt: 1, budget: 1,
            benchmarkCPM: 1,
            // [v4.8.2 新增] 添加 trackingEnabled 字段到完整视图
            trackingEnabled: 1,
            projectFiles: 1,

            // [v4.7 核心改造] 将完整的 collaborations 数组包含在响应中
            // [v4.8 修复] 现在每个 collaboration 都包含 talentInfo 字段
            collaborations: 1,

            // [兼容性保留] 保持所有 metrics 指标不变
            "metrics.projectBudget": 1, "metrics.totalCollaborators": 1, "metrics.budgetUtilization": 1, "metrics.totalIncome": 1,
            "metrics.totalRebateReceivable": 1, "metrics.incomeAdjustments": 1, "metrics.totalExpense": 1, "metrics.fundsOccupationCost": 1,
            "metrics.expenseAdjustments": 1, "metrics.totalOperationalCost": 1, "metrics.preAdjustmentProfit": 1, "metrics.preAdjustmentMargin": 1,
            "metrics.operationalProfit": 1, "metrics.operationalMargin": 1
          }
        }
      ];

      projectsData = await projectsCollection.aggregate(aggregationPipeline).toArray();
    }

    // --- [兼容性保留] 单项目查询逻辑不变 ---
    if (projectId) {
        if (projectsData.length > 0) {
             return { statusCode: 200, headers, body: JSON.stringify({ success: true, data: projectsData[0] }) };
        } else {
             return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `未找到 ID 为 '${projectId}' 的项目` }) };
        }
    }

    // --- [兼容性保留] 多项目列表查询逻辑不变 ---
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        count: projectsData.length,
        data: projectsData,
        view: view || 'full'
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