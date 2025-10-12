/**
 * @file 云函数: generated-sheets-manager
 * @description 单一云函数，用于处理“历史生成记录”的所有后端逻辑。
 * @author Your Name
 *
 * @trigger path: /generated-sheets
 * @methods
 * - GET: 获取列表 (?projectId=xxx)
 * - POST: 新增记录 (body: {...})
 * - POST: 数据迁移 (?action=migrate, body: [...])
 * - DELETE: 删除记录 (?id=xxx)
 */

const { MongoClient, ObjectId } = require('mongodb');

// --- 配置信息 ---
// 重要：请将您的 MongoDB 连接字符串存储在云函数的环境变量中，以保证安全。
// 不要将密码等敏感信息硬编码在代码里。
const uri = process.env.MONGODB_URI;
const dbName = 'kol_data';
const collectionName = 'generated_sheets';

// --- 数据库连接管理 ---
// 在云函数全局作用域中缓存数据库连接，以提高性能，避免每次调用都重新连接。
let cachedDb = null;

async function connectToDatabase() {
    if (cachedDb) {
        return cachedDb;
    }
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db(dbName);
    cachedDb = db;
    return db;
}

// --- 统一响应工具 ---
const createResponse = (statusCode, body) => {
    return {
        statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*', // 允许跨域，根据需要调整
            'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        body: JSON.stringify(body),
    };
};

// --- 云函数主处理程序 ---
exports.handler = async (event, context) => {
    // 预检请求处理，用于跨域
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(204, {});
    }

    try {
        const db = await connectToDatabase();
        const collection = db.collection(collectionName);
        const method = event.httpMethod;
        const queryParams = event.queryStringParameters || {};

        // --- API 路由逻辑 ---

        // 1. 获取列表 (GET)
        if (method === 'GET') {
            const { projectId } = queryParams;
            if (!projectId) {
                return createResponse(400, { error: 'projectId is required' });
            }
            const records = await collection.find({ projectId }).sort({ createdAt: -1 }).toArray();
            return createResponse(200, { data: records });
        }

        // 2. 新增记录 或 数据迁移 (POST)
        if (method === 'POST') {
            const body = JSON.parse(event.body || 'null');

            // A. 数据迁移 (当 action=migrate 时)
            if (queryParams.action === 'migrate') {
                const recordsToMigrate = body;
                if (!Array.isArray(recordsToMigrate) || recordsToMigrate.length === 0) {
                    return createResponse(400, { error: 'Invalid migration data' });
                }

                const tokens = recordsToMigrate.map(r => r.sheetToken).filter(Boolean);
                const existingRecords = await collection.find({ sheetToken: { $in: tokens } }).toArray();
                const existingTokens = new Set(existingRecords.map(r => r.sheetToken));
                const newRecords = recordsToMigrate.filter(r => r.sheetToken && !existingTokens.has(r.sheetToken));

                if (newRecords.length > 0) {
                    const recordsToInsert = newRecords.map(r => ({
                        projectId: r.projectId,
                        fileName: r.fileName,
                        sheetUrl: r.sheetUrl,
                        sheetToken: r.sheetToken,
                        createdBy: "migration", // 标记为迁移数据
                        createdAt: new Date(r.timestamp || Date.now()) // 使用旧的时间戳或当前时间
                    }));
                    await collection.insertMany(recordsToInsert);
                }
                return createResponse(200, { message: 'Migration complete', migrated: newRecords.length });
            }
            
            // B. 新增单条记录 (默认情况)
            else {
                const { projectId, fileName, sheetUrl, sheetToken, createdBy } = body;
                if (!projectId || !fileName || !sheetUrl || !sheetToken) {
                    return createResponse(400, { error: 'Missing required fields' });
                }

                const newRecord = {
                    projectId,
                    fileName,
                    sheetUrl,
                    sheetToken,
                    createdBy: createdBy || "unknown",
                    createdAt: new Date(),
                };
                const result = await collection.insertOne(newRecord);
                return createResponse(201, { data: { ...newRecord, _id: result.insertedId } });
            }
        }

        // 3. 删除记录 (DELETE)
        if (method === 'DELETE') {
            const { id } = queryParams;
            if (!id || !ObjectId.isValid(id)) {
                return createResponse(400, { error: 'Valid record id is required' });
            }
            await collection.deleteOne({ _id: new ObjectId(id) });
            return createResponse(204, {}); // 204 No Content
        }

        // --- 未匹配到任何路由 ---
        return createResponse(404, { error: 'Not Found' });

    } catch (error) {
        console.error('An error occurred:', error);
        return createResponse(500, { error: 'Internal Server Error', message: error.message });
    }
};

