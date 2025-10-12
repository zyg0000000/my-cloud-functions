/**
 * @file Cloud Function: automation-jobs-create
 * @version 1.1 - Patched field name mismatch
 * @description 接收前端请求，创建主作业记录 (Job)，并批量生成一系列的 automation-tasks 子任务。
 * 这是“一键生成报名表”功能的核心入口。
 * --- UPDATE (v1.1) ---
 * - [FIX] Corrected `target.talentXingtuId` to `target.xingtuId`.
 * - [FIX] Corrected `target.talentNickname` to `target.nickname`.
 * This resolves the issue where these fields were saved as null in the database.
 */
const { MongoClient, ObjectId } = require('mongodb');

// 从环境变量中获取配置
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'kol_data';
const JOBS_COLLECTION = 'automation-jobs';
const TASKS_COLLECTION = 'automation-tasks';

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
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        body: JSON.stringify(body),
    };
}

// --- 云函数主处理逻辑 ---
exports.handler = async (event, context) => {
    // 处理 CORS 预检请求
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(204, {});
    }

    if (event.httpMethod !== 'POST') {
        return createResponse(405, { success: false, message: 'Method Not Allowed' });
    }

    try {
        const db = await connectToDatabase();
        const jobsCollection = db.collection(JOBS_COLLECTION);
        const tasksCollection = db.collection(TASKS_COLLECTION);

        const body = JSON.parse(event.body || '{}');
        const { projectId, workflowId, targets } = body;

        // 1. 输入验证
        if (!projectId || !workflowId || !Array.isArray(targets) || targets.length === 0) {
            return createResponse(400, { success: false, message: 'projectId, workflowId, and a non-empty targets array are required.' });
        }
        
        // 2. 创建主 Job 文档
        const newJob = {
            projectId,
            workflowId,
            status: 'processing', // 状态: processing, awaiting_review, completed, failed
            totalTasks: targets.length,
            successTasks: 0,
            failedTasks: 0,
            createdAt: new Date(),
            updatedAt: new Date(),
        };
        const jobInsertResult = await jobsCollection.insertOne(newJob);
        const jobId = jobInsertResult.insertedId;

        // 3. 准备批量创建子 Tasks
        const tasksToCreate = targets.map(target => ({
            jobId: jobId, // 关联到主 Job
            projectId: projectId,
            workflowId: workflowId,
            // [BUGFIX] 使用前端发送的正确字段名 `xingtuId`
            xingtuId: target.xingtuId,
            // [核心] 存储额外信息，用于UI展示和未来追溯
            metadata: {
                // [BUGFIX] 使用前端发送的正确字段名 `nickname`
                talentNickname: target.nickname,
                collaborationId: target.collaborationId
            },
            status: 'pending', // 所有任务初始状态为 pending
            createdAt: new Date(),
            updatedAt: new Date(),
            result: null,
            errorMessage: null,
        }));
        
        // 4. 批量插入 Tasks
        if (tasksToCreate.length > 0) {
            await tasksCollection.insertMany(tasksToCreate);
        }

        console.log(`[JOB CREATED] Job ${jobId} created with ${tasksToCreate.length} tasks for project ${projectId}.`);

        return createResponse(201, { 
            success: true, 
            message: 'Job and tasks created successfully.',
            data: { jobId: jobId } 
        });

    } catch (error) {
        console.error('Error creating automation job:', error);
        return createResponse(500, { success: false, message: 'An internal server error occurred.', error: error.message });
    }
};
