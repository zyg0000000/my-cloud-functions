/**
 * [生产版 v2.1 - 规范与功能升级版]
 * 云函数：getTalents
 * 描述：获取达人列表，并在后端完成“合作次数”和“是否合作中”状态的聚合计算。
 * --- v2.1 更新日志 ---
 * - [功能补全] 增加了通过 talentId 查询单个达人详情的功能。
 * - [健壮性提升] 实现了“双源数据读取”，兼容 Postman 和在线测试工具。
 * - [规范统一] 统一了环境变量名 (MONGO_URI) 和配置读取方式，与其他函数保持一致。
 * ---------------------
 */

const { MongoClient } = require('mongodb');

// 统一使用规范的环境变量
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const TALENTS_COLLECTION = 'talents';
const COLLABS_COLLECTION = 'collaborations';

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
    // [健壮性提升] 实现“双源数据读取”
    let queryParams = {};
    if (event.queryStringParameters) { queryParams = event.queryStringParameters; } 
    // 后备方案，兼容在线测试工具可能将参数放在 body 的情况
    if (event.body) { try { Object.assign(queryParams, JSON.parse(event.body)); } catch(e) { /* ignore */ } }
    
    const { talentId, view } = queryParams;

    const dbClient = await connectToDatabase();
    const talentsCollection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);
    
    let talentsData;

    // --- 核心逻辑 ---
    // 构造基础的筛选条件
    const baseMatch = talentId ? { id: talentId } : {};

    if (view === 'simple') {
      // 简单视图：直接查询，只返回基础字段
      talentsData = await talentsCollection.find(baseMatch, { 
        projection: { _id: 0, id: 1, nickname: 1, xingtuId: 1 } 
      }).toArray();
    } else {
      // 重量级视图：执行聚合计算
      const aggregationPipeline = [
        { $match: baseMatch }, // 第一步：根据ID或空条件进行筛选
        { $project: { _id: 0 } }, // 排除 MongoDB 的 _id 字段
        { $lookup: { // 关联合作记录
            from: COLLABS_COLLECTION,
            localField: 'id',
            foreignField: 'talentId',
            as: 'collaborations'
          }
        },
        { $addFields: { // 计算衍生指标
            collaborationCount: { $size: '$collaborations' },
            inCollaboration: {
              $anyElementTrue: {
                $map: {
                  input: '$collaborations',
                  as: 'collab',
                  in: { $eq: ['$$collab.status', '客户已定档'] }
                }
              }
            }
          }
        },
        { $project: { collaborations: 0 } } // 清理临时数据
      ];
      
      talentsData = await talentsCollection.aggregate(aggregationPipeline).toArray();
    }

    // --- 统一返回格式 ---
    // 如果是按 ID 查询，且找到了结果，则返回单个对象，否则返回404
    if (talentId) {
        if (talentsData.length > 0) {
             return {
                statusCode: 200, headers,
                body: JSON.stringify({ success: true, data: talentsData[0] })
             };
        } else {
             return {
                statusCode: 404, headers,
                body: JSON.stringify({ success: false, message: `未找到 ID 为 '${talentId}' 的达人` })
             };
        }
    }

    // 如果是查询列表，则返回数组
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        count: talentsData.length,
        data: talentsData,
        view: view || 'full'
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
