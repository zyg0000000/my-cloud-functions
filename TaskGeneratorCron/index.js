/**
 * @file index.js
 * @version 11.0-system-tasks-added (Complete)
 * @description [架构升级] 统一的任务调度中心，通过单一API入口和action参数进行任务分发。
 * - [功能] 支持定时触发，执行核心的任务扫描和生成逻辑。
 * - [功能] 支持单一API入口 (POST /tasks-service)
 * - action: 'triggerScan' -> 手动触发一次扫描。
 * - action: 'getLogs' -> 获取最近的运行记录。
 * - [功能] 每次运行都会将结果写入日志。
 * - [新增 @ 2025-09-16] 增加了系统级任务：每周一提醒更新达人Performance，每月2号提醒更新达人价格。
 */
const { MongoClient } = require('mongodb');

// --- 配置项 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

// --- 数据库集合名称 ---
const PROJECTS_COLLECTION = 'projects';
const COLLABORATIONS_COLLECTION = 'collaborations';
const WORKS_COLLECTION = 'works';
const TASKS_COLLECTION = 'tasks';
const TALENTS_COLLECTION = 'talents'; // [新增] 达人集合
const LOGS_COLLECTION = 'task_run_logs'; // 日志集合

let client;
// 将 CoreServices 直接内联，因为当前只有此文件使用
const CoreServices = {
    TaskService: {
        async createOrUpdateTask(db, taskData) { /* Implemented below */ },
        async completeTask(db, projectId, taskType) { /* Implemented below */ }
    }
};

async function connectToDatabase() {
    if (client && client.topology && client.topology.isConnected()) {
        return client;
    }
    client = new MongoClient(MONGO_URI);
    await client.connect();
    return client;
}

// =================================================================
// --- 核心服务实现 (内联) ---
// =================================================================
CoreServices.TaskService.createOrUpdateTask = async function(db, taskData) {
    const tasksCol = db.collection(TASKS_COLLECTION);
    const taskIdentifier = { relatedProjectId: taskData.relatedProjectId, type: taskData.type };
    const taskPayload = {
        title: taskData.title,
        description: taskData.description,
        status: 'pending',
        updatedAt: new Date()
    };
    if (taskData.dueDate) {
        taskPayload.dueDate = taskData.dueDate;
    }
    await tasksCol.updateOne(
        taskIdentifier,
        { $set: taskPayload, $setOnInsert: { createdAt: new Date() } },
        { upsert: true }
    );
};

CoreServices.TaskService.completeTask = async function(db, projectId, taskType) {
    const tasksCol = db.collection(TASKS_COLLECTION);
    await tasksCol.updateOne(
        { relatedProjectId: projectId, type: taskType, status: { $ne: 'COMPLETED' } },
        { $set: { status: 'COMPLETED', updatedAt: new Date() } }
    );
};


// =================================================================
// --- 云函数主入口 ---
// =================================================================
exports.handler = async (event, context) => {
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
    };

    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 204, headers, body: '' };
    }
    
    const dbClient = await connectToDatabase();
    const db = dbClient.db(DB_NAME);

    // --- API 调用处理 ---
    if (event.httpMethod === 'POST') {
        const body = JSON.parse(event.body || '{}');
        const action = body.action;

        switch (action) {
            case 'getLogs':
                try {
                    const logsCollection = db.collection(LOGS_COLLECTION);
                    const limit = body.limit || 10;
                    const logs = await logsCollection.find({}).sort({ timestamp: -1 }).limit(limit).toArray();
                    return {
                        statusCode: 200,
                        headers,
                        body: JSON.stringify({ success: true, data: logs })
                    };
                } catch (error) {
                    console.error('Error fetching logs:', error);
                    return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: '获取日志失败' }) };
                }
            
            case 'triggerScan':
                break;

            default:
                return {
                    statusCode: 400,
                    headers,
                    body: JSON.stringify({ success: false, message: '无效的action参数' })
                };
        }
    }

    // --- 任务扫描逻辑 (定时触发 或 手动触发'triggerScan') ---
    const isApiCall = event && event.httpMethod === 'POST';
    const triggerType = isApiCall ? 'MANUAL' : 'SCHEDULED';
    let logPayload = {
        timestamp: new Date(),
        triggerType: triggerType,
        status: 'PENDING',
        summary: '任务扫描开始...',
        createdTasks: 0,
        completedTasks: 0,
        details: [] // [新增] 记录更详细的日志
    };

    try {
        // =================================================================
        // --- 1. 项目级任务规则执行 ---
        // =================================================================
        const projectsCol = db.collection(PROJECTS_COLLECTION);
        const collabsCol = db.collection(COLLABORATIONS_COLLECTION);
        const worksCol = db.collection(WORKS_COLLECTION);

        const projects = await projectsCol.find({ status: { $in: ['执行中', '待结算'] } }).toArray();
        const collaborations = await collabsCol.find({ projectId: { $in: projects.map(p => p.id) } }).toArray();
        const works = await worksCol.find({ projectId: { $in: projects.map(p => p.id) } }).toArray();

        const collabsByProject = collaborations.reduce((acc, c) => {
            if (!acc[c.projectId]) acc[c.projectId] = [];
            acc[c.projectId].push(c);
            return acc;
        }, {});
        
        const worksByCollab = works.reduce((acc, w) => {
            acc[w.collaborationId] = w;
            return acc;
        }, {});

        const today = new Date();
        today.setHours(0, 0, 0, 0);

        const projectTaskRules = [
            {
                type: 'PROJECT_PENDING_PUBLISH',
                condition: (project, projectCollabs) => {
                    const pending = projectCollabs.filter(c => {
                        const plannedDate = c.plannedReleaseDate ? new Date(c.plannedReleaseDate) : null;
                        return c.status === '客户已定档' && plannedDate && plannedDate <= today;
                    });
                    return pending.length > 0 ? pending : null;
                },
                generatePayload: (project, data) => ({
                    title: '达人待发布',
                    description: `项目 [${project.name}] 有 ${data.length} 位达人今日或之前应发布但未更新状态。`
                })
            },
            {
                type: 'PROJECT_DATA_OVERDUE_T7',
                condition: (project, projectCollabs) => {
                    // [BUG修复] 检查是否存在任何“已定档”但还未实际发布的合作
                    const isWaitingForRelease = projectCollabs.some(c => c.status === '客户已定档' && !c.publishDate);
                    if (isWaitingForRelease) {
                        return null; // 如果有，则暂停此项目的数据逾期告警
                    }

                    const latestPublishDate = projectCollabs.reduce((latest, c) => c.publishDate && new Date(c.publishDate) > latest ? new Date(c.publishDate) : latest, new Date(0));
                    if (latestPublishDate.getTime() === 0) return null;
                    const dueDate = new Date(latestPublishDate);
                    dueDate.setDate(dueDate.getDate() + 7);
                    if (today <= dueDate) return null;
                    const overdueDays = Math.floor((today - dueDate) / (1000 * 60 * 60 * 24));
                    const isDataMissing = projectCollabs.some(c => c.publishDate && !worksByCollab[c.id]?.t7_statsUpdatedAt);
                    return isDataMissing ? { dueDate, overdueDays } : null;
                },
                generatePayload: (project, data) => ({
                    title: '[告警] T+7 数据已逾期',
                    description: `项目 [${project.name}] 的 T+7 数据已逾期 ${data.overdueDays} 天！`,
                    dueDate: data.dueDate
                })
            },
            {
                type: 'PROJECT_DATA_OVERDUE_T21',
                condition: (project, projectCollabs) => {
                    // [BUG修复] 检查是否存在任何“已定档”但还未实际发布的合作
                    const isWaitingForRelease = projectCollabs.some(c => c.status === '客户已定档' && !c.publishDate);
                    if (isWaitingForRelease) {
                        return null; // 如果有，则暂停此项目的数据逾期告警
                    }
                    
                    const latestPublishDate = projectCollabs.reduce((latest, c) => c.publishDate && new Date(c.publishDate) > latest ? new Date(c.publishDate) : latest, new Date(0));
                    if (latestPublishDate.getTime() === 0) return null;
                    const dueDate = new Date(latestPublishDate);
                    dueDate.setDate(dueDate.getDate() + 21);
                    if (today <= dueDate) return null;
                    const overdueDays = Math.floor((today - dueDate) / (1000 * 60 * 60 * 24));
                    const isDataMissing = projectCollabs.some(c => c.publishDate && !worksByCollab[c.id]?.t21_statsUpdatedAt);
                    return isDataMissing ? { dueDate, overdueDays } : null;
                },
                generatePayload: (project, data) => ({
                    title: '[告警] T+21 数据已逾期',
                    description: `项目 [${project.name}] 的 T+21 数据已逾期 ${data.overdueDays} 天！`,
                    dueDate: data.dueDate
                })
            },
            {
                type: 'PROJECT_FINALIZE_REMINDER',
                condition: (project, projectCollabs) => {
                    if (['待结算', '已收款', '已终结'].includes(project.status)) return null;
                    const latestPublishDate = projectCollabs.reduce((latest, c) => c.publishDate && new Date(c.publishDate) > latest ? new Date(c.publishDate) : latest, new Date(0));
                    if (latestPublishDate.getTime() === 0) return null;
                    const finalizeDate = new Date(latestPublishDate);
                    finalizeDate.setDate(finalizeDate.getDate() + 21);
                    return today > finalizeDate ? true : null;
                },
                generatePayload: (project, data) => ({
                    title: '项目待定案',
                    description: `项目 [${project.name}] 的T+21数据周期已结束，请确认最终数据，发送结算邮件，并将项目状态更新为‘待结算’。`
                })
            }
        ];

        for (const project of projects) {
            const projectCollabs = collabsByProject[project.id] || [];
            for (const rule of projectTaskRules) {
                const conditionResult = rule.condition(project, projectCollabs);
                if (conditionResult) {
                    const payload = rule.generatePayload(project, conditionResult);
                    await CoreServices.TaskService.createOrUpdateTask(db, {
                        relatedProjectId: project.id,
                        type: rule.type,
                        ...payload
                    });
                    logPayload.createdTasks++;
                } else {
                    await CoreServices.TaskService.completeTask(db, project.id, rule.type);
                    logPayload.completedTasks++;
                }
            }
        }

        // =================================================================
        // --- 2. [新增] 系统级/全局性任务规则执行 ---
        // =================================================================
        const talentsCol = db.collection(TALENTS_COLLECTION);
        const allTalents = await talentsCol.find({}).toArray();
        const dayOfWeek = today.getDay(); // 0 = Sunday, 1 = Monday, ...
        const dayOfMonth = today.getDate();

        // 规则 1: 每周一检查达人Performance数据更新情况
        if (dayOfWeek === 1) { // 如果是周一
            const talentsToUpdate = allTalents.filter(talent => {
                if (!talent.performanceData?.lastUpdated) return true; // 如果从未更新过
                const lastUpdatedDate = new Date(talent.performanceData.lastUpdated);
                const diffTime = Math.abs(today - lastUpdatedDate);
                const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                return diffDays > 7;
            });

            if (talentsToUpdate.length > 0) {
                await CoreServices.TaskService.createOrUpdateTask(db, {
                    relatedProjectId: 'system_maintenance', // 使用虚拟ID以兼容前端
                    type: 'TALENT_PERFORMANCE_UPDATE_REMINDER',
                    title: '达人表现数据待更新',
                    description: `有 ${talentsToUpdate.length} 位达人的表现(performance)数据超过一周未更新，请及时处理。`
                });
                logPayload.createdTasks++;
                logPayload.details.push(`Created performance update task for ${talentsToUpdate.length} talents.`);
            } else {
                await CoreServices.TaskService.completeTask(db, 'system_maintenance', 'TALENT_PERFORMANCE_UPDATE_REMINDER');
                logPayload.completedTasks++;
            }
        }

        // 规则 2: 每月2号检查达人价格更新情况
        if (dayOfMonth === 2) { // 如果是2号
            const currentYear = today.getFullYear();
            const currentMonth = today.getMonth() + 1; // getMonth() is 0-indexed

            const talentsWithoutPrice = allTalents.filter(talent => {
                if (!talent.prices || !Array.isArray(talent.prices)) return true;
                const hasConfirmedPrice = talent.prices.some(p => 
                    p.year === currentYear && 
                    p.month === currentMonth && 
                    p.status === 'confirmed'
                );
                return !hasConfirmedPrice;
            });

            if (talentsWithoutPrice.length > 0) {
                 await CoreServices.TaskService.createOrUpdateTask(db, {
                    relatedProjectId: 'system_maintenance', // 使用虚拟ID以兼容前端
                    type: 'TALENT_PRICE_UPDATE_REMINDER',
                    title: '达人报价待更新',
                    description: `有 ${talentsWithoutPrice.length} 位达人缺少本月已确认的报价，请及时更新。`
                });
                logPayload.createdTasks++;
                logPayload.details.push(`Created price update task for ${talentsWithoutPrice.length} talents.`);
            } else {
                await CoreServices.TaskService.completeTask(db, 'system_maintenance', 'TALENT_PRICE_UPDATE_REMINDER');
                logPayload.completedTasks++;
            }
        }
        
        logPayload.status = 'SUCCESS';
        logPayload.summary = `处理了 ${projects.length} 个项目及 ${allTalents.length} 位达人，创建/更新 ${logPayload.createdTasks}，完成 ${logPayload.completedTasks}。`;

    } catch (error) {
        console.error('TaskGeneratorCron 运行时出错:', error);
        logPayload.status = 'FAILURE';
        logPayload.summary = '任务扫描期间发生严重错误。';
        logPayload.error = { message: error.message, stack: error.stack };
    } finally {
        const logsCollection = db.collection(LOGS_COLLECTION);
        await logsCollection.insertOne(logPayload);
    }

    if (isApiCall) {
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: true, message: '手动触发成功，扫描任务已在后台完成。' })
        };
    }
};
