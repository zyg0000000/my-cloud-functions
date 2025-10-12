/**
 * @file processConfigurations.js
 * @version 2.0-restored-and-cleaned
 * @description [架构重构] 恢复并净化 processConfigurations 逻辑，作为管理后台唯一的API网关。
 * - [核心修正] 恢复了原有处理多种配置类型的 POST 和 DELETE 逻辑。
 * - [BUG修复] 重写了 GET 逻辑，现在可以一次性获取所有配置，解决了前端加载失败的问题。
 * - [核心剔除] 彻底移除了对 'PROJECT_PENDING_PUBLISH' 的支持，与前端保持一致。
 */

const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const COLLECTION_NAME = 'project_configurations';

// [已净化] 更新了类型常量，使其与前端 admin.js (v2.1) 完全匹配
const CONFIG_TYPES = {
  ADJUSTMENT_TYPES: { docId: 'ADJUSTMENT_TYPES', field: 'values' },
  CAPITAL_RATES: { docId: 'CAPITAL_RATES', field: 'values' },
  FRAMEWORK_DISCOUNTS: { docId: 'FRAMEWORK_DISCOUNTS', field: 'values' },
  PROJECT_TYPES: { docId: 'PROJECT_TYPES', field: 'values' },
  FEISHU_NOTIFICATIONS: { docId: 'FEISHU_NOTIFICATIONS', field: 'settings' }
};

// [已净化] 更新了默认通知设置，与前端 admin.js (v2.1) 完全匹配
const DEFAULT_NOTIFICATION_SETTINGS = {
    PROJECT_BUDGET_WARNING: true,
    PROJECT_PAYMENT_REMINDER: true,
};

let client;

async function connectToDatabase() {
  if (client && client.topology && client.topology.isConnected()) {
    return client;
  }
  client = new MongoClient(MONGO_URI);
  await client.connect();
  return client;
}

function getConfigMeta(type) {
    return CONFIG_TYPES[type];
}

exports.handler = async (event, context) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  try {
    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(COLLECTION_NAME);

    const typeInQuery = event.queryStringParameters ? event.queryStringParameters.type : null;
    
    let payload = {};
    if (event.body) { 
        try { 
            payload = JSON.parse(event.body); 
        } catch(e) {
            return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '请求体JSON格式无效。' }) };
        } 
    }
    const typeInBody = payload.type;
    const type = typeInBody || typeInQuery;

    switch (event.httpMethod) {
      case 'GET': {
        // [BUG修复] 重写GET逻辑以支持一次性获取所有配置
        const allConfigs = await collection.find({}).toArray();
        const configMap = new Map(allConfigs.map(c => [c._id, c]));

        // 确保所有类型的配置都有返回值，特别是通知设置
        Object.keys(CONFIG_TYPES).forEach(key => {
            const meta = CONFIG_TYPES[key];
            if (!configMap.has(meta.docId)) {
                const defaultValue = (key === 'FEISHU_NOTIFICATIONS') ? DEFAULT_NOTIFICATION_SETTINGS : [];
                const fieldName = (key === 'FEISHU_NOTIFICATIONS') ? 'settings' : 'values';
                configMap.set(meta.docId, { _id: meta.docId, type: key, [fieldName]: defaultValue });
            }
        });
        
        // 返回前端期望的数组格式
        const responseData = Array.from(configMap.values()).map(doc => ({
            type: Object.keys(CONFIG_TYPES).find(key => CONFIG_TYPES[key].docId === doc._id),
            ...doc
        }));

        return { statusCode: 200, headers, body: JSON.stringify(responseData) };
      }

      case 'POST': {
        if (!type) return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '必须提供配置类型 type。' }) };
        const configMeta = getConfigMeta(type);
        if (!configMeta) return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: `无效的配置类型: ${type}` }) };
        const { docId, field } = configMeta;
        
        // [恢复] 保留原有处理多种配置类型的逻辑
        if (type === 'FEISHU_NOTIFICATIONS') {
            await collection.updateOne({ _id: docId }, { $set: { [field]: payload.settings, updatedAt: new Date() } }, { upsert: true });
        } else if (type === 'ADJUSTMENT_TYPES' || type === 'PROJECT_TYPES') {
            if (!payload.name) return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '类型名称不能为空。' }) };
            await collection.updateOne({ _id: docId }, { $set: { updatedAt: new Date() }, $addToSet: { [field]: payload.name.trim() } }, { upsert: true });
        } else {
            const newItem = (type === 'CAPITAL_RATES')
                ? { id: `rate_${Date.now()}`, name: payload.name, value: parseFloat(payload.value) }
                : { id: `disc_${Date.now()}`, name: payload.name, value: String(payload.value) };
            if (!newItem.name || newItem.value == null) return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '名称和值为必填项。' }) };
            await collection.updateOne({ _id: docId }, { $set: { updatedAt: new Date() }, $push: { [field]: newItem } }, { upsert: true });
        }
        return { statusCode: 201, headers, body: JSON.stringify({ success: true, message: '配置项添加/更新成功。' }) };
      }

      case 'DELETE': {
        if (!type) return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '必须提供配置类型 type。' }) };
        const configMeta = getConfigMeta(type);
        if (!configMeta) return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: `无效的配置类型: ${type}` }) };
        const { docId, field } = configMeta;
        
        // [恢复] 保留原有处理多种配置类型的逻辑
        if (type === 'ADJUSTMENT_TYPES' || type === 'PROJECT_TYPES') {
            if (!payload.name) return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '需要提供要删除的类型名称。' }) };
            await collection.updateOne({ _id: docId }, { $pull: { [field]: payload.name } });
        } else {
            if (!payload.id) return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: '需要提供要删除的配置项ID。' }) };
            await collection.updateOne({ _id: docId }, { $pull: { [field]: { id: payload.id } } });
        }
        return { statusCode: 200, headers, body: JSON.stringify({ success: true, message: '配置项删除成功。' }) };
      }

      default:
        return { statusCode: 405, headers, body: JSON.stringify({ success: false, message: '不支持的请求方法。' }) };
    }
  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }) };
  }
};
