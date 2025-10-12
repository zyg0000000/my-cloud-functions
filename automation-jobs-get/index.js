/**
 * @file Cloud Function: automation-jobs-get
 * @version 1.1 - Eager Load Tasks
 * @description 为“自动化任务”Tab提供数据支持。
 * --- UPDATE (v1.1) ---
 * - [OPTIMIZATION] Switched from lazy loading to eager loading.
 * - [CHANGE] The query for a project now returns all jobs with their complete sub-task arrays included.
 * - [REMOVED] The `$project: { tasks: 0 }` stage was removed to facilitate this change.
 * This significantly improves UI responsiveness and reduces the total number of API calls.
 */
const { MongoClient, ObjectId } = require('mongodb');

// 从环境变量中获取配置
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'kol_data';
const JOBS_COLLECTION = 'automation-jobs';
const TASKS_COLLECTION = 'automation-tasks';
const WORKFLOWS_COLLECTION = 'automation-workflows';


let cachedDb = null;

// --- 数据库连接 ---
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

// --- 标准化响应 ---
function createResponse(statusCode, body) {
    return {
        statusCode: statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        body: JSON.stringify(body),
    };
}

// --- 云函数主处理逻辑 ---
exports.handler = async (event, context) => {
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(204, {});
    }
    if (event.httpMethod !== 'GET') {
        return createResponse(405, { success: false, message: 'Method Not Allowed' });
    }

    try {
        const db = await connectToDatabase();
        const { projectId, jobId } = event.queryStringParameters || {};

        if (jobId) {
            // --- 场景2: 获取单个 Job 及其所有 Tasks 的详情 (保持不变, 懒加载逻辑依然可用) ---
            if (!ObjectId.isValid(jobId)) {
                return createResponse(400, { success: false, message: "Invalid Job ID format." });
            }
            const jobsCollection = db.collection(JOBS_COLLECTION);
            const tasksCollection = db.collection(TASKS_COLLECTION);
            
            const jobDetails = await jobsCollection.findOne({ _id: new ObjectId(jobId) });
            if (!jobDetails) {
                return createResponse(404, { success: false, message: "Job not found." });
            }

            const tasks = await tasksCollection.find({ jobId: new ObjectId(jobId) }).sort({ createdAt: 1 }).toArray();
            
            jobDetails.tasks = tasks; // 将 tasks 列表挂载到 job 对象上
            
            return createResponse(200, { success: true, data: jobDetails });

        } else if (projectId) {
            // --- 场景1: 获取一个项目下的所有 Job 列表 (包含完整的 Tasks) ---
            const jobsCollection = db.collection(JOBS_COLLECTION);

            const jobsWithTasks = await jobsCollection.aggregate([
                { $match: { projectId: projectId } },
                { $sort: { createdAt: -1 } },
                {
                    $lookup: {
                        from: TASKS_COLLECTION,
                        localField: '_id',
                        foreignField: 'jobId',
                        as: 'tasks' // 直接关联所有子任务
                    }
                },
                {
                   $lookup: {
                        from: WORKFLOWS_COLLECTION,
                        let: { wfId: { $toObjectId: "$workflowId" } },
                        pipeline: [ { $match: { $expr: { $eq: ["$_id", "$$wfId"] } } } ],
                        as: "workflowInfo"
                    }
                },
                {
                    $addFields: {
                        // 统计数据现在直接基于已关联的 tasks 数组计算
                        totalTasks: { $size: '$tasks' },
                        successTasks: { $size: { $filter: { input: '$tasks', as: 'task', cond: { $eq: ['$$task.status', 'completed'] } } } },
                        failedTasks: { $size: { $filter: { input: '$tasks', as: 'task', cond: { $eq: ['$$task.status', 'failed'] } } } },
                        workflowName: { $ifNull: [{ $arrayElemAt: ["$workflowInfo.name", 0] }, "未知工作流"] },
                    }
                },
                {
                    $project: { // 只移除临时的 workflowInfo
                        workflowInfo: 0
                    }
                }
            ]).toArray();

            return createResponse(200, { success: true, data: jobsWithTasks });

        } else {
            return createResponse(400, { success: false, message: 'Either projectId or jobId must be provided.' });
        }

    } catch (error) {
        console.error('Error getting automation jobs:', error);
        return createResponse(500, { success: false, message: 'An internal server error occurred.', error: error.message });
    }
};
