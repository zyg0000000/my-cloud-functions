/**
 * @file index.js
 * @version 12.0.0 (Feishu Integration)
 * @description [架构升级] 统一的任务调度中心，现在负责触发飞书通知。
 * - [核心改造] 在创建或更新任务后，会调用 feishu-notifier 服务来发送飞书卡片消息。
 * - [代码内联] 为简化部署，将 core-services.js 的功能直接整合到本文件中。
 * - [配置驱动] 通过环境变量 FEISHU_NOTIFIER_URL 调用通知服务，实现解耦。
 */
const { MongoClient } = require('mongodb');
const axios = require('axios');

// --- 配置项 ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';
// [新增] 通知服务的URL
const FEISHU_NOTIFIER_URL = process.env.FEISHU_NOTIFIER_URL;

// --- 数据库集合名称 ---
const PROJECTS_COLLECTION = 'projects';
const COLLABORATIONS_COLLECTION = 'collaborations';
const WORKS_COLLECTION = 'works';
const TASKS_COLLECTION = 'tasks';
const TALENTS_COLLECTION = 'talents';
const LOGS_COLLECTION = 'task_run_logs';

let client;

/**
 * [新增] 调用飞书通知服务的辅助函数
 * @param {object} task - 完整的任务对象
 * @param {string} projectName - 任务所属的项目名称
 */
async function triggerFeishuNotification(task, projectName) {
    if (!FEISHU_NOTIFIER_URL) {
        console.warn(`[Feishu] 环境变量 FEISHU_NOTIFIER_URL 未配置，跳过发送任务 ${task._id} 的通知。`);
        return;
    }
    try {
        // 组装发送给 feishu-notifier 的数据体
        const payload = { ...task, projectName };
        await axios.post(FEISHU_NOTIFIER_URL, payload);
        console.log(`[Feishu] 已成功为任务 ${task._id} 触发飞书通知。`);
    } catch (error) {
        console.error(`[Feishu] 调用通知服务失败 (任务ID: ${task._id}):`, error.message);
    }
}


// --- 核心服务实现 (内联并修改) ---
const CoreServices = {
    TaskService: {
        async createOrUpdateTask(db, taskData) {
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
            
            // [核心修改] 使用 findOneAndUpdate 来获取任务是新建还是更新
            const result = await tasksCol.findOneAndUpdate(
                taskIdentifier,
                { $set: taskPayload, $setOnInsert: { createdAt: new Date() } },
                { upsert: true, returnDocument: 'after' }
            );

            const updatedTask = result.value;

            // 只有当任务是新创建的 (createdAt 和 updatedAt 非常接近) 或状态被重置为 pending 时，才发送通知
            if (updatedTask && ( (updatedTask.updatedAt - updatedTask.createdAt) < 1000 || result.lastErrorObject?.updatedExisting === false) ) {
                let projectName = '系统全局任务';
                if (updatedTask.relatedProjectId !== 'system_maintenance') {
                    const projectsCol = db.collection(PROJECTS_COLLECTION);
                    const project = await projectsCol.findOne({ id: updatedTask.relatedProjectId });
                    if (project) {
                        projectName = project.name;
                    }
                }
                // 触发通知
                await triggerFeishuNotification(updatedTask, projectName);
            }
            return updatedTask;
        },
        async completeTask(db, projectId, taskType) {
            const tasksCol = db.collection(TASKS_COLLECTION);
            await tasksCol.updateOne(
                { relatedProjectId: projectId, type: taskType, status: { $ne: 'COMPLETED' } },
                { $set: { status: 'COMPLETED', updatedAt: new Date() } }
            );
        }
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

// --- 云函数主入口 (保持大部分逻辑不变) ---
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

    if (event.httpMethod === 'POST') {
        const body = JSON.parse(event.body || '{}');
        const action = body.action;

        switch (action) {
            case 'getLogs':
                // ... (此部分逻辑保持不变)
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

    const isApiCall = event && event.httpMethod === 'POST';
    const triggerType = isApiCall ? 'MANUAL' : 'SCHEDULED';
    let logPayload = {
        timestamp: new Date(),
        triggerType: triggerType,
        status: 'PENDING',
        summary: '任务扫描开始...',
        createdTasks: 0,
        completedTasks: 0,
        details: []
    };

    try {
        // --- 任务扫描逻辑 (保持不变) ---
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
        
        // --- 项目级任务规则 (保持不变) ---
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
                    const isWaitingForRelease = projectCollabs.some(c => c.status === '客户已定档' && !c.publishDate);
                    if (isWaitingForRelease) return null;
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
                    const isWaitingForRelease = projectCollabs.some(c => c.status === '客户已定档' && !c.publishDate);
                    if (isWaitingForRelease) return null;
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

        // --- 系统级任务规则 (保持不变) ---
        const talentsCol = db.collection(TALENTS_COLLECTION);
        const allTalents = await talentsCol.find({}).toArray();
        const dayOfWeek = today.getDay(); 
        const dayOfMonth = today.getDate();
        if (dayOfWeek === 1) {
            const talentsToUpdate = allTalents.filter(talent => {
                if (!talent.performanceData?.lastUpdated) return true;
                const lastUpdatedDate = new Date(talent.performanceData.lastUpdated);
                const diffTime = Math.abs(today - lastUpdatedDate);
                const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                return diffDays > 7;
            });
            if (talentsToUpdate.length > 0) {
                await CoreServices.TaskService.createOrUpdateTask(db, { relatedProjectId: 'system_maintenance', type: 'TALENT_PERFORMANCE_UPDATE_REMINDER', title: '达人表现数据待更新', description: `有 ${talentsToUpdate.length} 位达人的表现(performance)数据超过一周未更新，请及时处理。` });
                logPayload.createdTasks++;
                logPayload.details.push(`Created performance update task for ${talentsToUpdate.length} talents.`);
            } else {
                await CoreServices.TaskService.completeTask(db, 'system_maintenance', 'TALENT_PERFORMANCE_UPDATE_REMINDER');
                logPayload.completedTasks++;
            }
        }
        if (dayOfMonth === 2) {
            const currentYear = today.getFullYear();
            const currentMonth = today.getMonth() + 1;
            const talentsWithoutPrice = allTalents.filter(talent => {
                if (!talent.prices || !Array.isArray(talent.prices)) return true;
                return !talent.prices.some(p => p.year === currentYear && p.month === currentMonth && p.status === 'confirmed');
            });
            if (talentsWithoutPrice.length > 0) {
                 await CoreServices.TaskService.createOrUpdateTask(db, { relatedProjectId: 'system_maintenance', type: 'TALENT_PRICE_UPDATE_REMINDER', title: '达人报价待更新', description: `有 ${talentsWithoutPrice.length} 位达人缺少本月已确认的报价，请及时更新。` });
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

