/**
 * @file Cloud Function: automation-jobs-create
 * @version 3.4 - Dynamic ID Support
 * @description 接收前端请求，创建主作业记录 (Job)，并批量生成一系列的 automation-tasks 子任务。
 * --- UPDATE (v3.4) ---
 * - [核心升级] 增加了对动态ID的支持。函数现在会检查 'workflowId' 以确定需要的目标ID类型 (如 'taskId' 或 'xingtuId')，使其能够处理多种自动化场景。
 * - [健壮性] 增加了对工作流的查询和验证，并能跳过缺少必要ID的目标，提高了任务创建的可靠性。
 * - [向后兼容] 在工作流未定义 requiredInput.key 时，默认使用 'xingtuId'，确保旧功能不受影响。
 */
const { MongoClient, ObjectId } = require('mongodb');

// 从环境变量中获取配置
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'kol_data';
const JOBS_COLLECTION = 'automation-jobs';
const TASKS_COLLECTION = 'automation-tasks';
const WORKFLOWS_COLLECTION = 'automation-workflows'; // 新增对工作流集合的引用

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
        const workflowsCollection = db.collection(WORKFLOWS_COLLECTION);

        const body = JSON.parse(event.body || '{}');
        const { projectId, workflowId, targets } = body;

        // 1. 输入验证
        if (!workflowId || !Array.isArray(targets) || targets.length === 0) {
            return createResponse(400, { success: false, message: 'workflowId and a non-empty targets array are required.' });
        }
        
        // [核心升级] 查询工作流以确定需要的目标ID key
        const workflow = await workflowsCollection.findOne({ _id: new ObjectId(workflowId) });
        if (!workflow) {
            return createResponse(404, { success: false, message: `Workflow with ID ${workflowId} not found.` });
        }
        const requiredInputKey = workflow.requiredInput?.key || 'xingtuId'; // 默认为 xingtuId 以保证向后兼容

        // 2. 创建主 Job 文档
        const newJob = {
            projectId: projectId || null, // projectId 可以为空，代表非项目关联的独立任务
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
        const tasksToCreate = targets.map(target => {
            const targetId = target[requiredInputKey];
            if (!targetId) {
                console.warn(`[Job Creator] Skipping target because it lacks the required ID key '${requiredInputKey}'. Target:`, target);
                return null; // 如果目标缺少必要的ID，则跳过
            }

            return {
                jobId: jobId,
                projectId: projectId || null,
                workflowId: workflowId,
                [requiredInputKey]: targetId, // 使用动态键名
                targetId: targetId, // 统一存储，方便未来按 targetId 查询
                metadata: {
                    talentNickname: target.nickname,
                    collaborationId: target.collaborationId
                },
                status: 'pending',
                createdAt: new Date(),
                updatedAt: new Date(),
                result: null,
                errorMessage: null,
            };
        }).filter(Boolean); // 过滤掉所有为 null 的无效任务
        
        // 4. 批量插入 Tasks
        if (tasksToCreate.length > 0) {
            await tasksCollection.insertMany(tasksToCreate);
        }

        console.log(`[JOB CREATED] Job ${jobId} created with ${tasksToCreate.length} tasks for project ${projectId || 'N/A'}.`);

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
