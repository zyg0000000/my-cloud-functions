/**
 * @file getTalentFilterOptions.js
 * @version 1.0
 * @description
 * 云函数: getTalentFilterOptions (生产版)
 * 专为“近期表现”和“达人库”等页面的筛选器提供动态选项。
 *
 * 功能特性:
 * - 实时查询数据库中所有不重复的 `talentTier` (达人层级)。
 * - 实时查询数据库中所有不重复的 `talentType` (达人类型)。
 * - 高效轻量，只返回筛选所需的最少数据。
 *
 * 触发器: API 网关, GET /talents/filter-options
 */

const { MongoClient } = require('mongodb');

// 从环境变量中获取配置
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
const TALENTS_COLLECTION = 'talents';

let client;

// 数据库连接池
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

  // 检查 MONGO_URI 是否配置
  if (!MONGO_URI) {
      console.error("严重错误: 环境变量 MONGO_URI 未配置。");
      return { 
          statusCode: 500, 
          headers, 
          body: JSON.stringify({ success: false, message: "服务器数据库配置不完整。" }) 
      };
  }

  try {
    const dbClient = await connectToDatabase();
    const collection = dbClient.db(DB_NAME).collection(TALENTS_COLLECTION);

    // 1. 使用 Promise.all 并行执行两个独立的数据库查询
    const [tiers, types] = await Promise.all([
        // 使用 distinct 高效获取所有不重复的 'talentTier' 值
        collection.distinct('talentTier'),
        // distinct 同样适用于数组字段，会自动展开并去重
        collection.distinct('talentType')
    ]);

    // 2. 清理数据：过滤掉 null, undefined 或空字符串等无效值
    const cleanTiers = tiers.filter(tier => tier);
    const cleanTypes = types.filter(type => type);

    // 3. 组装并返回响应
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        data: {
          tiers: cleanTiers.sort(), // 按字母顺序排序
          types: cleanTypes.sort()  // 按字母顺序排序
        }
      })
    };

  } catch (error) {
    console.error('获取筛选选项时发生错误:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
    };
  }
};
