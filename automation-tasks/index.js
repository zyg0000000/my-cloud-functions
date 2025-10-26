/**
 * @file Cloud Function: automation-tasks
 * @version 4.8 - Project Filtering & Pagination
 * @description Centralized task management API.
 * --- UPDATE (v4.8) ---
 * - [PERFORMANCE] Implemented server-side pagination and filtering for GET requests.
 * - [FEATURE] The GET endpoint now accepts `projectId`, `page`, and `limit` query parameters.
 * - [EFFICIENCY] Switched to an aggregation pipeline using `$facet` to retrieve paginated data and total count in a single database query.
 * - [RESPONSE] The response for list retrieval now includes a `pagination` object.
 * - [COMPATIBILITY] Retains the ability to fetch a single task by ID.
 */
const { MongoClient, ObjectId } = require('mongodb');
const { TosClient } = require('@volcengine/tos-sdk');

// --- Environment Variables ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'kol_data';
const TASKS_COLLECTION = 'automation-tasks';
const JOBS_COLLECTION = 'automation-jobs';
const WORKFLOWS_COLLECTION = 'automation-workflows';

// --- TOS Client Initialization ---
const tosClient = new TosClient({
    accessKeyId: process.env.TOS_ACCESS_KEY_ID,
    accessKeySecret: process.env.TOS_SECRET_ACCESS_KEY,
    endpoint: process.env.TOS_ENDPOINT,
    region: process.env.TOS_REGION,
});

let cachedDb = null;

async function connectToDatabase() {
    if (cachedDb) return cachedDb;
    const client = new MongoClient(MONGO_URI, {
        connectTimeoutMS: 5000,
        serverSelectionTimeoutMS: 5000
    });
    try {
        await client.connect();
        const db = client.db(DB_NAME);
        cachedDb = db;
        return db;
    } catch (error) {
        console.error("MongoDB connection failed:", error);
        throw new Error("Could not connect to the database.");
    }
}

function createResponse(statusCode, body) {
    return {
        statusCode: statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        },
        body: JSON.stringify(body),
    };
}


async function deleteTosFolder(taskId) {
    const bucketName = process.env.TOS_BUCKET_NAME;
    const prefix = `automation_screenshots/${taskId}/`;
    try {
        const listedObjects = await tosClient.listObjects({ bucket: bucketName, prefix });
        const files = listedObjects?.data?.Contents;
        if (!Array.isArray(files) || files.length === 0) return;
        const deleteKeys = files.map(obj => ({ key: obj.Key }));
        await tosClient.deleteMultiObjects({ bucket: bucketName, objects: deleteKeys });
        console.log(`[TOS Manager] Successfully deleted ${deleteKeys.length} screenshots from TOS for task ${taskId}.`);
    } catch (error) {
        console.error(`[TOS Manager] CRITICAL ERROR caught in deleteTosFolder for task ${taskId}:`, error);
    }
}

async function recalculateAndSyncJobStats(jobId, db) {
    if (!jobId || !ObjectId.isValid(jobId)) return;

    console.log(`[Job Sync] Recalculating stats for job ${jobId}...`);
    const tasksCollection = db.collection(TASKS_COLLECTION);
    const jobsCollection = db.collection(JOBS_COLLECTION);

    try {
        const statsPipeline = [
            { $match: { jobId: new ObjectId(jobId) } },
            {
                $group: {
                    _id: "$jobId",
                    totalTasks: { $sum: 1 },
                    successTasks: { $sum: { $cond: [{ $eq: ["$status", "completed"] }, 1, 0] } },
                    failedTasks: { $sum: { $cond: [{ $eq: ["$status", "failed"] }, 1, 0] } },
                    pendingTasks: { $sum: { $cond: [{ $eq: ["$status", "pending"] }, 1, 0] } },
                    processingTasks: { $sum: { $cond: [{ $eq: ["$status", "processing"] }, 1, 0] } },
                }
            }
        ];
        
        const results = await tasksCollection.aggregate(statsPipeline).toArray();
        const stats = results[0] || { totalTasks: 0, successTasks: 0, failedTasks: 0, pendingTasks: 0, processingTasks: 0 };
        
        let newStatus = 'processing';
        if (stats.pendingTasks === 0 && stats.processingTasks === 0) {
            newStatus = 'awaiting_review';
        }
        if (stats.totalTasks === 0) {
             newStatus = 'awaiting_review';
        }

        const updatePayload = {
            status: newStatus,
            totalTasks: stats.totalTasks,
            successTasks: stats.successTasks,
            failedTasks: stats.failedTasks,
            updatedAt: new Date(),
        };

        await jobsCollection.updateOne(
            { _id: new ObjectId(jobId) },
            { $set: updatePayload }
        );

        console.log(`[Job Sync] Successfully synced job ${jobId} with payload:`, updatePayload);

    } catch (error) {
        console.error(`[Job Sync] CRITICAL: Failed to sync stats for job ${jobId}:`, error);
    }
}


exports.handler = async (event, context) => {
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(204, {});
    }

    try {
        const db = await connectToDatabase();
        const collection = db.collection(TASKS_COLLECTION);
        const body = event.body ? JSON.parse(event.body) : {};
        const taskId = event.queryStringParameters?.id;
        const projectId = event.queryStringParameters?.projectId;

        switch (event.httpMethod) {
            case 'GET': {
                if (taskId) {
                    if (!ObjectId.isValid(taskId)) return createResponse(400, { success: false, message: "Invalid ID format" });
                    const task = await collection.findOne({ _id: new ObjectId(taskId) });
                    return createResponse(200, { success: true, data: task });
                } else {
                    const page = parseInt(event.queryStringParameters?.page, 10) || 1;
                    const limit = parseInt(event.queryStringParameters?.limit, 10) || 20;
                    const skip = (page - 1) * limit;

                    // [核心改造] 构建筛选条件
                    const matchStage = {};
                    if (projectId) {
                        matchStage.projectId = projectId;
                    }
                    // 新增功能：按 jobId 筛选（支持查询测试任务）
                    if (event.queryStringParameters?.jobId !== undefined) {
                        const jobIdParam = event.queryStringParameters.jobId;
                        if (jobIdParam === 'null' || jobIdParam === null || jobIdParam === '') {
                            // 查询独立任务（测试任务）
                            matchStage.jobId = null;
                        } else {
                            // 查询特定 Job 的任务
                            try {
                                matchStage.jobId = new ObjectId(jobIdParam);
                            } catch (error) {
                                console.error('Invalid jobId format:', jobIdParam);
                                return createResponse(400, {
                                    success: false,
                                    message: '无效的 jobId 格式'
                                });
                            }
                        }
                    }

                    // 新增功能：按 workflowId 筛选（方便按工作流查询）
                    if (event.queryStringParameters?.workflowId) {
                        matchStage.workflowId = event.queryStringParameters.workflowId;
                    }

                    // [核心改造] 使用聚合管道进行分页和关联查询
                    const aggregationPipeline = [
                        { $match: matchStage },
                        {
                            $facet: {
                                paginatedResults: [
                                    { $sort: { createdAt: -1 } },
                                    { $skip: skip },
                                    { $limit: limit },
                                    {
                                        $lookup: {
                                            from: WORKFLOWS_COLLECTION,
                                            let: { wfId: { $toObjectId: "$workflowId" } },
                                            pipeline: [ { $match: { $expr: { $eq: ["$_id", "$$wfId"] } } } ],
                                            as: "workflowInfo"
                                        }
                                    },
                                    { $addFields: { workflowInfo: { $arrayElemAt: ["$workflowInfo", 0] } } },
                                    { $addFields: { workflowName: { $ifNull: ["$workflowInfo.name", "Unknown Workflow"] } } },
                                    { $project: { workflowInfo: 0 } }
                                ],
                                totalCount: [
                                    { $count: 'count' }
                                ]
                            }
                        }
                    ];

                    const results = await collection.aggregate(aggregationPipeline).toArray();
                    
                    const tasksWithWorkflow = results[0]?.paginatedResults || [];
                    const total = results[0]?.totalCount[0]?.count || 0;
                    const hasNextPage = (page * limit) < total;

                    return createResponse(200, {
                        success: true,
                        data: tasksWithWorkflow,
                        pagination: { total, page, limit, hasNextPage }
                    });
                }
            }

            case 'POST': {
                // [核心改造] 根据工作流定义，确定ID的键名
                const workflowsCollection = db.collection(WORKFLOWS_COLLECTION);
                const workflow = await workflowsCollection.findOne({_id: new ObjectId(body.workflowId)});
                
                if (!workflow) {
                     return createResponse(404, { success: false, message: 'Workflow not found.' });
                }

                const requiredInputKey = workflow.requiredInput?.key || 'xingtuId'; // 默认为 xingtuId 以兼容旧版
                const dynamicId = body[requiredInputKey];

                if (!body.workflowId || !dynamicId) {
                    return createResponse(400, { success: false, message: `workflowId and '${requiredInputKey}' are required.` });
                }
                const newTask = {
                    workflowId: body.workflowId, 
                    jobId: body.jobId ? new ObjectId(body.jobId) : null,
                    projectId: body.projectId || null, // [新增] 保存 projectId
                    [requiredInputKey]: dynamicId, // 使用动态键
                    status: 'pending',
                    createdAt: new Date(),
                    updatedAt: new Date(),
                    result: null, errorMessage: null,
                    metadata: body.metadata || {} // 保存元数据
                };
                
                // 为了查询方便，统一将动态ID也存入一个固定字段
                newTask.targetId = dynamicId;

                const result = await collection.insertOne(newTask);
                const createdTask = await collection.findOne({ _id: result.insertedId });
                return createResponse(201, { success: true, data: createdTask });
            }

            case 'PUT': {
                 if (!taskId) return createResponse(400, { success: false, message: 'Task ID is required for update.' });
                 if (!ObjectId.isValid(taskId)) return createResponse(400, { success: false, message: "Invalid ID format" });

                 const taskBeforeUpdate = await collection.findOne({ _id: new ObjectId(taskId) }, { projection: { jobId: 1 } });
                 const jobId = taskBeforeUpdate?.jobId;

                 if (body.action === 'rerun') {
                     await deleteTosFolder(taskId);
                     const updateResult = await collection.updateOne(
                         { _id: new ObjectId(taskId) },
                         { $set: { status: 'pending', updatedAt: new Date(), result: null, errorMessage: null, failedAt: null, completedAt: null } }
                     );
                     if (updateResult.modifiedCount === 0) return createResponse(404, { success: false, message: 'Task not found for rerun.' });
                     
                     await recalculateAndSyncJobStats(jobId, db);
                     
                     const rerunTask = await collection.findOne({ _id: new ObjectId(taskId) });
                     return createResponse(200, { success: true, data: rerunTask });

                 } else {
                     const updateData = { ...body, updatedAt: new Date() };
                     delete updateData._id;
                     const result = await collection.updateOne({ _id: new ObjectId(taskId) }, { $set: updateData });
                     if (result.matchedCount === 0) return createResponse(404, { success: false, message: 'Task not found.' });

                     await recalculateAndSyncJobStats(jobId, db);

                     return createResponse(200, { success: true, data: { updatedId: taskId } });
                 }
            }

            case 'DELETE': {
                 if (!taskId) return createResponse(400, { success: false, message: 'Task ID is required for deletion.' });
                 if (!ObjectId.isValid(taskId)) return createResponse(400, { success: false, message: "Invalid ID format" });

                 const taskToDelete = await collection.findOne({ _id: new ObjectId(taskId) }, { projection: { jobId: 1 } });
                 const jobIdForDelete = taskToDelete?.jobId;

                 await deleteTosFolder(taskId);
                
                 const result = await collection.deleteOne({ _id: new ObjectId(taskId) });
                
                 if (result.deletedCount === 0) {
                     console.warn(`[Task Deleter] Task ${taskId} not found in DB, but cleanup was attempted.`);
                 } else {
                     console.log(`[Task Deleter] Successfully deleted task ${taskId} from database.`);
                     await recalculateAndSyncJobStats(jobIdForDelete, db);
                 }
                 return createResponse(204, {});
            }

            default:
                return createResponse(405, { success: false, message: `Method Not Allowed: ${event.httpMethod}` });
        }
    } catch (error) {
        console.error('Error in automation-tasks handler:', error);
        return createResponse(500, { success: false, message: 'An internal server error occurred.', error: error.message });
    }
};
