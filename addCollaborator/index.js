/**
 * [生产版 v6.0 - 支持多次合作]
 * 云函数：addCollaborator
 * 描述：为指定项目新增一条合作记录。
 * --- v6.0 更新日志 ---
 * - [核心改造] 移除了“一个项目一个达人只能合作一次”的限制，以支持新的业务模式。
 * - [功能增强] 新增接收 `plannedReleaseDate` (计划发布日期) 字段，作为区分多次合作的业务标识。
 * --- v5.0 更新日志 ---
 * - [核心解耦] 在创建合作记录时，会从达人库查询达人当前的来源 (talentSource)，
 * 并将其作为“快照”存入合作记录的同名字段中。
 * - [数据固化] 此修改确保了达人来源在交易创建时被永久固化，与达人库解耦。
 */

const { MongoClient, ObjectId } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLABS_COLLECTION = 'collaborations';
const PROJECTS_COLLECTION = 'projects';
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

function calculateRebateReceivable(amount, rebate, orderType) {
    const amountNum = Number(amount) || 0;
    const rebateNum = Number(rebate) || 0;

    if (orderType === 'original') {
        return amountNum * (rebateNum / 100);
    } else { // modified
        if (rebateNum > 20) {
            return amountNum * ((rebateNum / 100) - 0.20);
        } else {
            return 0;
        }
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
    const collabsCollection = dbClient.db(DB_NAME).collection(COLLABS_COLLECTION);
    const projectsCollection = dbClient.db(DB_NAME).collection(PROJECTS_COLLECTION);
    const talentsCollection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);

    let inputData = {};
    if (event.body) {
        try { inputData = JSON.parse(event.body); } catch(e) { /* ignore */ }
    }
    if (Object.keys(inputData).length === 0 && event.queryStringParameters) {
        inputData = event.queryStringParameters;
    }

    // [改造步骤 1.2] 增加 plannedReleaseDate 字段的接收
    const { projectId, talentId, amount, priceInfo, rebate, orderType, plannedReleaseDate } = inputData;

    if (!projectId || !talentId || amount == null || rebate == null) {
      return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体中缺少必要的字段 (projectId, talentId, amount, rebate)。' }) };
    }

    const project = await projectsCollection.findOne({ id: projectId });
    if (!project) {
      return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `项目ID '${projectId}' 不存在。` }) };
    }
    
    const talent = await talentsCollection.findOne({ id: talentId });
    if (!talent) {
        return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: `达人ID '${talentId}' 不存在。` }) };
    }

    /*
    // [改造步骤 1.1] 注释掉重复检查，允许同一达人多次合作
    const existingCollab = await collabsCollection.findOne({ projectId: projectId, talentId: talentId });
    if (existingCollab) {
        return { statusCode: 409, headers, body: JSON.stringify({ success: false, message: '该达人已存在于此项目中，请勿重复添加。' }) };
    }
    */

    const newCollaborator = {
      _id: new ObjectId(),
      id: `collab_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
      projectId,
      talentId,
      talentSource: talent.talentSource || '野生达人',
      amount: Number(amount),
      priceInfo: priceInfo || '',
      rebate: Number(rebate),
      orderType: orderType || 'modified',
      status: '待提报工作台',
      // [改造步骤 1.2] 增加新字段到数据库记录中
      plannedReleaseDate: plannedReleaseDate || null, 
      createdAt: new Date(),
      updatedAt: new Date(),
      orderDate: null, publishDate: null, contentFile: null, taskId: null,
      videoId: null, paymentDate: null, actualRebate: null, recoveryDate: null,
      discrepancyReason: null, rebateScreenshots: [],
    };
    
    if (newCollaborator.talentSource === '机构达人') {
        const rebateReceivable = calculateRebateReceivable(newCollaborator.amount, newCollaborator.rebate, newCollaborator.orderType);
        newCollaborator.actualRebate = Number(rebateReceivable.toFixed(2));
    }

    await collabsCollection.insertOne(newCollaborator);
    
    const { _id, ...returnData } = newCollaborator;

    return {
      statusCode: 201,
      headers,
      body: JSON.stringify({ 
        success: true, 
        message: '合作记录创建成功', 
        data: returnData 
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};

