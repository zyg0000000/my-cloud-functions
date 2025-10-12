/**
 * @file Cloud Function: automation-tasks
 * @version 4.7 - Job Sync & Recalculation
 * @description Centralized task management API.
 * --- UPDATE (v4.7) ---
 * - [CRITICAL] Implemented `recalculateAndSyncJobStats`, a robust function to ensure data consistency.
 * - [FEATURE] When a task is updated (PUT) or deleted (DELETE), this function is now triggered.
 * - [LOGIC] It recalculates total, success, and failed task counts for the parent job and updates its status, ensuring the job's data is always accurate.
 * - [FIX] This architecture definitively solves data inconsistency issues when tasks are manipulated.
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

// --- [核心升级] 重新计算并同步父Job状态和统计数据的函数 ---
async function recalculateAndSyncJobStats(jobId, db) {
    if (!jobId || !ObjectId.isValid(jobId)) return;

    console.log(`[Job Sync] Recalculating stats for job ${jobId}...`);
    const tasksCollection = db.collection(TASKS_COLLECTION);
    const jobsCollection = db.collection(JOBS_COLLECTION);

    try {
        // 使用聚合一次性计算所有统计数据
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
        // 如果已经没有任何待处理或处理中的任务，则标记为待审查
        if (stats.pendingTasks === 0 && stats.processingTasks === 0) {
            newStatus = 'awaiting_review';
        }
        
        // 如果任务总数为0（例如，所有任务都被删除了），也标记为待审查
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
                    
                    const tasksWithWorkflow = await collection.aggregate([
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
                    ]).toArray();
                    
                    const total = await collection.countDocuments();
                    const hasNextPage = (page * limit) < total;

                    return createResponse(200, {
                        success: true,
                        data: tasksWithWorkflow,
                        pagination: { total, page, limit, hasNextPage }
                    });
                }
            }

            case 'POST': {
                const xingtuId = body.xingtuId || body.targetXingtuId;
                const { workflowId, jobId = null } = body;
                if (!workflowId || !xingtuId) {
                    return createResponse(400, { success: false, message: 'workflowId and a valid Xingtu ID are required.' });
                }
                const newTask = {
                    workflowId, 
                    jobId: jobId ? new ObjectId(jobId) : null, // 确保jobId存为ObjectId
                    xingtuId,
                    status: 'pending',
                    createdAt: new Date(),
                    updatedAt: new Date(),
                    result: null, errorMessage: null,
                };
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

