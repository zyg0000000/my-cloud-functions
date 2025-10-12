/**
 * @file Cloud Function: automation-jobs-update
 * @version 2.2 (POST for Delete)
 * @description 接收前端请求，用于更新或安全地删除一个 Job。
 * - [核心改造] 新增支持 POST 方法来执行删除操作，以绕过环境中 DELETE 方法可能存在的 CORS 问题。
 * - [兼容性] 同时保留了对 DELETE 和 PUT 方法的处理逻辑。
 */
const { MongoClient, ObjectId } = require('mongodb');

// --- Environment Variables ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'kol_data';
const JOBS_COLLECTION = 'automation-jobs';
const TASKS_COLLECTION = 'automation-tasks';

let cachedDb = null;

// --- 数据库连接 ---
async function connectToDatabase() {
    if (cachedDb) return cachedDb;
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    const db = client.db(DB_NAME);
    cachedDb = db;
    return db;
}

// --- 标准化响应 ---
function createResponse(statusCode, body) {
    return {
        statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            // 确保 POST, PUT, DELETE 都在允许列表中
            'Access-Control-Allow-Methods': 'POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        body: JSON.stringify(body),
    };
}

// --- 安全删除逻辑 (提取为独立函数以便复用) ---
async function safeDeleteJob(jobId, db) {
    const tasksCollection = db.collection(TASKS_COLLECTION);
    const jobsCollection = db.collection(JOBS_COLLECTION);

    const associatedTasksCount = await tasksCollection.countDocuments({ jobId });
    if (associatedTasksCount > 0) {
        return createResponse(409, { // 409 Conflict
            success: false, 
            message: `无法删除，该任务批次下仍有 ${associatedTasksCount} 个子任务。请先清空所有子任务。` 
        });
    }

    const result = await jobsCollection.deleteOne({ _id: jobId });
    if (result.deletedCount === 0) {
        return createResponse(404, { success: false, message: 'Job not found.' });
    }
    
    console.log(`[JOB DELETE] Successfully deleted empty job ${jobId}.`);
    return createResponse(204, {}); // 204 No Content
}


// --- 云函数主处理逻辑 ---
exports.handler = async (event, context) => {
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(204, {});
    }

    try {
        const db = await connectToDatabase();
        const { id } = event.queryStringParameters || {};

        if (!id || !ObjectId.isValid(id)) {
            return createResponse(400, { success: false, message: 'A valid Job ID must be provided.' });
        }
        
        const jobId = new ObjectId(id);

        switch (event.httpMethod) {
            case 'PUT': {
                const jobsCollection = db.collection(JOBS_COLLECTION);
                const body = JSON.parse(event.body || '{}');
                if (!body.status) {
                    return createResponse(400, { success: false, message: "Only 'status' field can be updated." });
                }
                const updateData = { status: body.status, updatedAt: new Date() };
                const result = await jobsCollection.updateOne({ _id: jobId }, { $set: updateData });
                if (result.matchedCount === 0) {
                    return createResponse(404, { success: false, message: 'Job not found.' });
                }
                return createResponse(200, { success: true, message: 'Job updated successfully.' });
            }

            // [核心改造] 使用 POST 方法执行删除
            case 'POST':
            case 'DELETE': {
                return await safeDeleteJob(jobId, db);
            }

            default:
                return createResponse(405, { success: false, message: 'Method Not Allowed' });
        }

    } catch (error) {
        console.error('Error in automation-jobs-update handler:', error);
        return createResponse(500, { success: false, message: 'An internal server error occurred.', error: error.message });
    }
};

