/**
 * @file handleProjectReport/index.js
 * @description [V3.2-功能增强] 增加数据录入提醒功能。
 * - [功能增强] getReportData 接口现在会返回当日缺少播放数据的已发布视频列表。
 * - [逻辑修正] 将日报平均CPM修正为项目整体CPM，分母采用截至当日的累积总曝光。
 * - [逻辑修正] 严格区分“定档”与“已发布”的统计口径，确保总览KPI数据的准确性。
 * - [性能] 重构了`saveDailyStats`函数，解决了N+1查询问题，大幅提升了数据保存性能。
 */
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const MONGO_DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

let client;

async function connectToDatabase() {
    if (client && client.topology && client.topology.isConnected()) {
        return client;
    }
    client = new MongoClient(MONGO_URI);
    await client.connect();
    return client;
}

// --- Helper function to handle dates consistently and robustly in UTC ---
function createUTCDate(dateString) {
    const parts = dateString.split('-');
    if (parts.length !== 3) return new Date(NaN);
    const year = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10) - 1;
    const day = parseInt(parts[2], 10);
    return new Date(Date.UTC(year, month, day));
}

function formatDate(dateObject) {
    if (!(dateObject instanceof Date) || isNaN(dateObject)) {
        const today = new Date();
        const year = today.getUTCFullYear();
        const month = String(today.getUTCMonth() + 1).padStart(2, '0');
        const day = String(today.getUTCDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }
    const year = dateObject.getUTCFullYear();
    const month = String(dateObject.getUTCMonth() + 1).padStart(2, '0');
    const day = String(dateObject.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}


// --- 内部业务逻辑函数 ---

/**
 * API 1: 获取日报数据
 * [FIXED] 修正了总览数据的计算逻辑
 */
async function getReportData(db, projectId, date) {
    const project = await db.collection('projects').findOne({ id: projectId });
    if (!project) {
        return { overview: {}, details: {}, missingDataVideos: [] };
    }
    const projectDiscount = parseFloat(project.discount) || 1;

    // --- 1. 定义精确的数据范围 ---
    const scheduledCollaborations = await db.collection('collaborations').find({ projectId, status: { $in: ["客户已定档", "视频已发布"] } }).toArray();
    const publishedCollaborations = scheduledCollaborations.filter(c => c.status === "视频已发布");

    if (publishedCollaborations.length === 0) {
        const totalTalents = new Set(scheduledCollaborations.map(c => c.talentId)).size;
        return { overview: { totalTalents: totalTalents, publishedVideos: 0, totalAmount: 0, totalViews: 0, averageCPM: 0 }, details: {}, missingDataVideos: [] };
    }

    // --- 2. 获取所需的数据 ---
    const collaborationIds = publishedCollaborations.map(c => c.id);
    const works = await db.collection('works').find({ collaborationId: { $in: collaborationIds } }).toArray();
    const allTalentIds = scheduledCollaborations.map(c => c.talentId); // 使用更广的范围获取达人信息
    const talents = await db.collection('talents').find({ id: { $in: allTalentIds } }).toArray();

    const talentMap = new Map(talents.map(t => [t.id, t]));
    
    // --- 3. 计算日报详情, 累积总曝光, 和缺失数据列表 ---
    const dateStr = date ? formatDate(createUTCDate(date)) : formatDate(new Date());
    const yesterdayDate = createUTCDate(dateStr);
    yesterdayDate.setUTCDate(yesterdayDate.getUTCDate() - 1);
    const yesterdayStr = formatDate(yesterdayDate);
    
    let reportVideos = [];
    let overallTotalViews = 0;
    const worksById = new Map(works.map(w => [w.collaborationId, w]));
    const videosWithDataTodayIds = new Set(); // 存储当天有数据的视频ID

    for (const collab of publishedCollaborations) {
        const work = worksById.get(collab.id);
        if (work && work.dailyStats && work.dailyStats.length > 0) {
            
            const relevantStats = work.dailyStats.filter(s => s.date <= dateStr);
            if (relevantStats.length > 0) {
                relevantStats.sort((a, b) => b.date.localeCompare(a.date));
                overallTotalViews += relevantStats[0].totalViews || 0;
            }

            const dayStat = work.dailyStats.find(s => s.date === dateStr);
            if (dayStat) {
                videosWithDataTodayIds.add(collab.id); // 记录ID
                const yesterdayStat = work.dailyStats.find(s => s.date === yesterdayStr);
                const talent = talentMap.get(collab.talentId);
                reportVideos.push({
                    talentName: talent?.nickname || '未知达人',
                    publishDate: collab.publishDate,
                    totalViews: dayStat.totalViews,
                    cpm: dayStat.cpm,
                    cpmChange: yesterdayStat ? dayStat.cpm - yesterdayStat.cpm : null,
                    solution: dayStat.solution || '',
                    collaborationId: work.collaborationId
                });
            }
        }
    }

    // [新增] 找出缺少当日数据的已发布视频
    const missingDataVideos = publishedCollaborations
        .filter(collab => !videosWithDataTodayIds.has(collab.id))
        .map(collab => ({
            collaborationId: collab.id,
            talentName: talentMap.get(collab.talentId)?.nickname || '未知达人'
        }));
   
    const details = {
        hotVideos: reportVideos.filter(v => v.totalViews > 10000000),
        goodVideos: reportVideos.filter(v => v.cpm < 20),
        normalVideos: reportVideos.filter(v => v.cpm >= 20 && v.cpm < 40),
        badVideos: reportVideos.filter(v => v.cpm >= 40 && v.cpm < 100),
        worstVideos: reportVideos.filter(v => v.cpm >= 100)
    };
    
    // --- 4. 计算总览数据 ---
    const totalTalents = new Set(scheduledCollaborations.map(c => c.talentId)).size;
    const publishedVideosCount = publishedCollaborations.length;
    const totalAmount = publishedCollaborations.reduce((sum, c) => {
        const amount = parseFloat(c.amount) || 0;
        return sum + (amount * projectDiscount * 1.05);
    }, 0);
    const totalViewsToday = reportVideos.reduce((sum, v) => sum + (v.totalViews || 0), 0);

    const overview = {
        totalTalents: totalTalents,
        publishedVideos: publishedVideosCount,
        totalAmount: totalAmount,
        totalViews: totalViewsToday,
        averageCPM: totalAmount > 0 && overallTotalViews > 0 ? (totalAmount / overallTotalViews) * 1000 : 0
    };

    return { overview, details, missingDataVideos };
}

/**
 * API 2: 获取待录入数据的视频列表
 */
async function getVideosForEntry(db, projectId, date) {
    const collaborations = await db.collection('collaborations').find({ projectId, status: { $in: ["客户已定档", "视频已发布"] } }).toArray();
    if (collaborations.length === 0) return [];
    const talentIds = collaborations.map(c => c.talentId);
    const collabIds = collaborations.map(c => c.id);
    const [talents, works] = await Promise.all([
        db.collection('talents').find({ id: { $in: talentIds } }).toArray(),
        db.collection('works').find({ collaborationId: { $in: collabIds } }).toArray()
    ]);
    const talentMap = new Map(talents.map(t => [t.id, t.nickname]));
    const workMap = new Map(works.map(w => [w.collaborationId, w]));
    return collaborations.map(collab => {
        const work = workMap.get(collab.id);
        const dailyStat = work?.dailyStats?.find(stat => stat.date === date);
        return {
            collaborationId: collab.id,
            talentName: talentMap.get(collab.talentId) || '未知达人',
            publishDate: collab.publishDate,
            totalViews: dailyStat?.totalViews || null
        };
    });
}

/**
 * API 3: 保存每日录入的数据
 * [PERF] 修复了 N+1 查询问题
 */
async function saveDailyStats(db, projectId, date, data) {
    const project = await db.collection('projects').findOne({ id: projectId });
    const projectDiscount = project ? parseFloat(project.discount) : 1;

    const collaborationIds = data.map(item => item.collaborationId);
    
    const [collaborations, works] = await Promise.all([
        db.collection('collaborations').find({ id: { $in: collaborationIds } }).toArray(),
        db.collection('works').find({ collaborationId: { $in: collaborationIds } }).toArray()
    ]);

    const collaborationMap = new Map(collaborations.map(c => [c.id, c]));
    const workMap = new Map(works.map(w => [w.collaborationId, w]));

    const dateStr = formatDate(createUTCDate(date));
    const yesterdayDate = createUTCDate(dateStr);
    yesterdayDate.setUTCDate(yesterdayDate.getUTCDate() - 1);
    const yesterdayStr = formatDate(yesterdayDate);

    const bulkOps = data.map(item => {
        const collaboration = collaborationMap.get(item.collaborationId);
        if (!collaboration) return null;

        const amount = parseFloat(collaboration.amount) || 0;
        const income = amount * projectDiscount * 1.05;
        const cpm = income > 0 && item.totalViews > 0 ? (income / item.totalViews) * 1000 : 0;
        
        const work = workMap.get(item.collaborationId);
        const yesterdayStat = work?.dailyStats?.find(s => s.date === yesterdayStr);
        const cpmChange = yesterdayStat ? cpm - yesterdayStat.cpm : null;
        
        const pullOp = {
            updateOne: {
                filter: { collaborationId: item.collaborationId },
                update: { $pull: { dailyStats: { date: dateStr } } }
            }
        };
        const pushOp = {
            updateOne: {
                filter: { collaborationId: item.collaborationId },
                update: {
                    $push: { 
                        dailyStats: {
                            $each: [{ date: dateStr, totalViews: item.totalViews, cpm, cpmChange, solution: '' }],
                            $sort: { date: 1 }
                        }
                    }
                },
                upsert: true
            }
        };
        return [pullOp, pushOp];
    }).filter(Boolean).flat();

    if (bulkOps.length > 0) {
        await db.collection('works').bulkWrite(bulkOps);
    }

    return { message: '数据保存成功' };
}

/**
 * API 4: 保存后续解决方案
 */
async function saveReportSolution(db, { collaborationId, date, solution }) {
    const dateStr = formatDate(createUTCDate(date));
    await db.collection('works').updateOne(
        { collaborationId: collaborationId, "dailyStats.date": dateStr },
        { $set: { "dailyStats.$.solution": solution } }
    );
     return { message: '解决方案已保存' };
}

// --- 主处理函数 ---
exports.handler = async (event, context) => {
    const headers = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Content-Type': 'application/json' };
    if (event.httpMethod === 'OPTIONS') return { statusCode: 204, headers, body: '' };
    try {
        const db = (await connectToDatabase()).db(MONGO_DB_NAME);
        const { httpMethod, path, queryStringParameters, body } = event;
        const parsedBody = body ? JSON.parse(body) : {};
        let result;
        if (httpMethod === 'GET' && path.includes('/project-report')) {
            result = await getReportData(db, queryStringParameters.projectId, queryStringParameters.date);
        } else if (httpMethod === 'GET' && path.includes('/videos-for-entry')) {
            result = await getVideosForEntry(db, queryStringParameters.projectId, queryStringParameters.date);
        } else if (httpMethod === 'POST' && path.includes('/daily-stats')) {
            result = await saveDailyStats(db, parsedBody.projectId, parsedBody.date, parsedBody.data);
        } else if (httpMethod === 'POST' && path.includes('/report-solution')) {
            result = await saveReportSolution(db, parsedBody);
        } else {
            return { statusCode: 404, headers, body: JSON.stringify({ message: 'API not found' }) };
        }
        return { statusCode: 200, headers, body: JSON.stringify({ success: true, data: result }) };
    } catch (error) {
        console.error('Error in handler:', error);
        return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: error.message }) };
    }
};

