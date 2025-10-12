/**
 * @file getasks_updated.js
 * @version 2.0-filtered
 * @description 智能任务中心 - 前端API接口 (项目任务专用版)
 * - [核心修改] 新增了任务类型过滤器，此接口现在只返回项目相关的待办任务，不再包含全局维护类任务。
 * - [修正] 扩大了项目信息的查询范围，获取完整的项目文档。
 * - 响应前端 /tasks 的 GET 请求。
 */

const { MongoClient } = require('mongodb');

// --- 配置项 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

// --- 数据库集合名称 ---
const PROJECTS_COLLECTION = 'projects';
const TASKS_COLLECTION = 'tasks';

let client;

// --- 数据库连接 ---
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

    try {
        const dbClient = await connectToDatabase();
        const db = dbClient.db(DB_NAME);

        const tasksCol = db.collection(TASKS_COLLECTION);
        const projectsCol = db.collection(PROJECTS_COLLECTION);

        // [核心修改] 在查询中增加了 type 过滤器，
        // 使用 $nin (not in) 操作符来排除全局维护任务。
        const pendingTasks = await tasksCol.find({
            status: 'pending',
            type: {
                $nin: [
                    'TALENT_PERFORMANCE_UPDATE_REMINDER',
                    'TALENT_PRICE_UPDATE_REMINDER'
                ]
            }
        }).sort({ createdAt: -1 }).toArray();

        if (pendingTasks.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({ success: true, data: {} })
            };
        }

        const projectIds = [...new Set(pendingTasks.map(task => task.relatedProjectId))];

        // 查询完整的项目文档
        const projects = await projectsCol.find({ id: { $in: projectIds } }).toArray();
        const projectMap = new Map(projects.map(p => [p.id, p.name]));

        const tasksByProject = pendingTasks.reduce((acc, task) => {
            const projectId = task.relatedProjectId;
            // 跳过那些没有关联项目ID的脏数据
            if (!projectId) {
                return acc;
            }
            
            if (!acc[projectId]) {
                acc[projectId] = {
                    projectName: projectMap.get(projectId) || '未知项目',
                    tasks: []
                };
            }
            acc[projectId].tasks.push({
                id: task._id.toString(),
                type: task.type,
                title: task.title,
                description: task.description,
                dueDate: task.dueDate
            });
            return acc;
        }, {});

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: true, data: tasksByProject })
        };

    } catch (error) {
        console.error('Error in getTasks function:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message })
        };
    }
};
