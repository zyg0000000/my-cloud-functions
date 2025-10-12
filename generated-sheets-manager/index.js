/**
 * @file 云函数: generated-sheets-manager
 * @version 2.0.0 - Feishu File Deletion
 * @description
 * - [核心功能] 新增了在删除历史记录时，同步删除飞书云端电子表格的功能。
 * - [严谨性] 遵循“先删云端，再删本地”的原则。只有当飞书API确认文件删除成功后，才会删除数据库中的记录；否则操作将中断并返回错误。
 * - [依赖] 增加了 `axios` 用于和飞书API进行通信。
 * - [配置] 此函数现在需要 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` 环境变量来获取飞书API的访问权限。
 */

const { MongoClient, ObjectId } = require('mongodb');
const axios = require('axios');

// --- 配置信息 ---
const MONGO_URI = process.env.MONGODB_URI;
const DB_NAME = 'kol_data';
const COLLECTION_NAME = 'generated_sheets';

// 飞书应用凭证，需要配置在云函数环境变量中
const APP_ID = process.env.FEISHU_APP_ID;
const APP_SECRET = process.env.FEISHU_APP_SECRET;

// --- 模块级缓存 ---
let cachedDb = null;
let tenantAccessToken = null;
let tokenExpiresAt = 0;

// --- 数据库连接管理 ---
async function connectToDatabase() {
    if (cachedDb) {
        return cachedDb;
    }
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    const db = client.db(DB_NAME);
    cachedDb = db;
    return db;
}

// --- 飞书认证 ---
async function getTenantAccessToken() {
    if (Date.now() < tokenExpiresAt && tenantAccessToken) {
        return tenantAccessToken;
    }
    if (!APP_ID || !APP_SECRET) {
        throw new Error('环境变量 FEISHU_APP_ID 或 FEISHU_APP_SECRET 未配置。');
    }
    try {
        const response = await axios.post(
            'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
            { app_id: APP_ID, app_secret: APP_SECRET }
        );
        if (response.data.code !== 0) {
            throw new Error(`获取 tenant_access_token 失败: ${response.data.msg}`);
        }
        tenantAccessToken = response.data.tenant_access_token;
        tokenExpiresAt = Date.now() + (response.data.expire - 300) * 1000; // 提前5分钟过期
        return tenantAccessToken;
    } catch (error) {
        const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
        console.error("getTenantAccessToken error:", errorMessage);
        throw new Error(`获取飞书 token 时发生网络或API错误: ${errorMessage}`);
    }
}


// --- 统一响应工具 ---
const createResponse = (statusCode, body) => {
    return {
        statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        body: JSON.stringify(body),
    };
};

// --- 云函数主处理程序 ---
exports.handler = async (event, context) => {
    // 预检请求处理
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(204, {});
    }

    try {
        const db = await connectToDatabase();
        const collection = db.collection(COLLECTION_NAME);
        const method = event.httpMethod;
        const queryParams = event.queryStringParameters || {};

        // --- API 路由逻辑 ---

        // GET 和 POST 逻辑保持不变
        if (method === 'GET') {
            const { projectId } = queryParams;
            if (!projectId) {
                return createResponse(400, { error: 'projectId is required' });
            }
            const records = await collection.find({ projectId }).sort({ createdAt: -1 }).toArray();
            return createResponse(200, { data: records });
        }

        if (method === 'POST') {
            const body = JSON.parse(event.body || 'null');
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
                        createdBy: "migration",
                        createdAt: new Date(r.timestamp || Date.now())
                    }));
                    await collection.insertMany(recordsToInsert);
                }
                return createResponse(200, { message: 'Migration complete', migrated: newRecords.length });
            } else {
                const { projectId, fileName, sheetUrl, sheetToken, createdBy } = body;
                if (!projectId || !fileName || !sheetUrl || !sheetToken) {
                    return createResponse(400, { error: 'Missing required fields' });
                }
                const newRecord = { projectId, fileName, sheetUrl, sheetToken, createdBy: createdBy || "unknown", createdAt: new Date() };
                const result = await collection.insertOne(newRecord);
                return createResponse(201, { data: { ...newRecord, _id: result.insertedId } });
            }
        }

        // [核心修改] 重写 DELETE 逻辑
        if (method === 'DELETE') {
            const { id } = queryParams;
            if (!id || !ObjectId.isValid(id)) {
                return createResponse(400, { error: 'Valid record id is required' });
            }
            
            // 步骤 1: 从数据库查找记录，获取 sheetToken
            const recordToDelete = await collection.findOne({ _id: new ObjectId(id) });
            if (!recordToDelete) {
                // 如果记录本就不存在，直接返回成功，避免前端报错
                return createResponse(204, {});
            }

            const sheetToken = recordToDelete.sheetToken;
            
            // 如果记录没有关联的 sheetToken，直接删除数据库记录即可
            if (!sheetToken) {
                await collection.deleteOne({ _id: new ObjectId(id) });
                return createResponse(204, {});
            }

            // 步骤 2: 调用飞书 API 删除云端文件
            try {
                const accessToken = await getTenantAccessToken();
                console.log(`[Feishu API] Attempting to delete file with token: ${sheetToken}`);
                await axios.delete(
                    `https://open.feishu.cn/open-apis/drive/v1/files/${sheetToken}`,
                    {
                        headers: { 'Authorization': `Bearer ${accessToken}` },
                        params: { type: 'sheet' } // 指定文件类型为电子表格
                    }
                );
                console.log(`[Feishu API] Successfully deleted file: ${sheetToken}`);
            } catch (feishuError) {
                const status = feishuError.response?.status;
                const feishuMsg = feishuError.response?.data?.msg || '未知飞书API错误';

                // 特殊情况：如果文件在飞书上已经不存在 (404)，我们可以认为删除操作是“成功的”，然后继续删除数据库记录
                if (status === 404) {
                    console.warn(`[Feishu API] File with token ${sheetToken} not found on Feishu. Proceeding to delete local record.`);
                } else {
                    // 对于其他所有错误（如权限不足），严格遵守约定，中断操作并返回错误
                    console.error('Failed to delete Feishu file:', feishuError.response ? feishuError.response.data : feishuError.message);
                    return createResponse(502, { // 502 Bad Gateway 表示上游服务器出错
                        error: 'Failed to delete the file in Feishu',
                        message: `删除飞书文件失败：${feishuMsg} (Code: ${feishuError.response?.data?.code})`
                    });
                }
            }
            
            // 步骤 3: 飞书文件删除成功（或已不存在）后，删除数据库记录
            await collection.deleteOne({ _id: new ObjectId(id) });
            console.log(`[MongoDB] Successfully deleted record with id: ${id}`);

            return createResponse(204, {}); // 204 No Content 表示成功删除
        }

        return createResponse(404, { error: 'Not Found' });

    } catch (error) {
        console.error('An error occurred:', error);
        return createResponse(500, { error: 'Internal Server Error', message: error.message });
    }
};
