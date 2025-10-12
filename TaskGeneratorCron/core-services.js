/**
 * @file core-services.js
 * @version 5.0-purged
 * @description [架构重构] 核心业务服务层 (飞书功能剥离)
 * - [核心净化] 彻底删除了所有与飞书相关的功能、配置及依赖。
 * - [职责] 此服务层现在是一个纯粹的、对内的数据库服务模块，仅负责核心业务的数据操作。
 * - [移除内容] FeishuService, getTenantAccessToken, feishu-related configs.
 */

const { MongoClient } = require('mongodb');

// --- 配置 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

// --- 数据库集合名称 ---
const TASKS_COLLECTION = 'tasks';

let client;
let db;

// --- 数据库连接管理 ---
async function getDb() {
    if (db) return db;
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    return db;
}

// =================================================================
// --- 核心服务模块 ---
// =================================================================

const CollaborationService = {
    // 暂时没有与合作直接相关的服务函数，保留模块以备将来扩展
};

const TaskService = {
    /**
     * 创建一个新任务，或根据 projectId 和 type 更新现有任务。
     * @param {object} taskData - 包含任务详情的对象
     */
    async createOrUpdateTask(taskData) {
        const db = await getDb();
        const tasksCol = db.collection(TASKS_COLLECTION);
        // 使用 relatedProjectId 和 type作为任务的唯一标识符
        const taskIdentifier = { relatedProjectId: taskData.relatedProjectId, type: taskData.type };

        const taskPayload = {
            title: taskData.title,
            description: taskData.description,
            status: 'pending', // 任务创建时默认为 pending
            updatedAt: new Date()
        };
        
        // 如果任务数据中包含 dueDate, 也一并更新
        if (taskData.dueDate) {
            taskPayload.dueDate = taskData.dueDate;
        }

        await tasksCol.updateOne(
            taskIdentifier,
            { $set: taskPayload, $setOnInsert: { createdAt: new Date() } },
            { upsert: true }
        );
    },
    
    /**
     * 根据 projectId 和 taskType 将任务状态更新为 COMPLETED。
     * 主要用于定时任务自动关闭已解决的事项。
     * @param {string} projectId - 项目ID
     * @param {string} taskType - 任务类型
     */
    async completeTask(projectId, taskType) {
        const db = await getDb();
        const tasksCol = db.collection(TASKS_COLLECTION);
        
        // 仅当任务当前不为 COMPLETED 时才更新，避免不必要的写操作
        await tasksCol.updateOne(
            { relatedProjectId: projectId, type: taskType, status: { $ne: 'COMPLETED' } },
            { $set: { status: 'COMPLETED', updatedAt: new Date() } }
        );
    }
};

module.exports = {
    CollaborationService,
    TaskService
};