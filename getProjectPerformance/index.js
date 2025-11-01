/**
 * @file getProjectPerformance.js
 * @version 3.3-t21-complete
 * @description 效果看板API (T+21数据补全版)
 * * --- 更新日志 (v3.3) ---
 * - [T+21数据补全] 新增T+21的所有详细子指标，包括点赞、评论、分享、组件数据、完播率和触达分布
 * - [数据对齐] T+21现在拥有与T+7完全相同的数据维度
 * - [计算字段] 为T+21新增 interactions, interactionRate, likeToViewRatio, completionViews, totalReach 等衍生指标
 */
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

let client;
async function connectToDatabase() {
    if (client && client.topology && client.topology.isConnected()) {
        return client;
    }
    client = new MongoClient(MONGO_URI);
    await client.connect();
    return client;
}

function addDays(date, days) {
    if (!date) return null;
    const result = new Date(date);
    result.setDate(result.getDate() + days);
    return result.toISOString().split('T')[0];
}

exports.handler = async (event, context) => {
    const headers = { 'Content-Type': 'application/json' };

    try {
        const dbClient = await connectToDatabase();
        const db = dbClient.db(DB_NAME);

        const projectsCollection = db.collection('projects');
        const collaborationsCollection = db.collection('collaborations');
        const worksCollection = db.collection('works');
        const talentsCollection = db.collection('talents');

        const { projectId } = JSON.parse(event.body || '{}');
        if (!projectId) {
            return { statusCode: 400, headers, body: JSON.stringify({ message: '请求错误：缺少 projectId。' }) };
        }

        const project = await projectsCollection.findOne({ id: projectId });
        if (!project) {
            return { statusCode: 404, headers, body: JSON.stringify({ message: '找不到指定的项目。' }) };
        }
        const benchmarkCPM = project.benchmarkCPM || null;
        const projectDiscount = (project.discount !== null && project.discount !== undefined) ? Number(project.discount) : 1;

        const collaborations = await collaborationsCollection.find({ projectId }).toArray();
        if (collaborations.length === 0) {
            return { statusCode: 200, headers, body: JSON.stringify({ overall: {}, talents: [] }) };
        }
        
        const collaborationIds = collaborations.map(c => c.id);
        const talentIds = collaborations.map(c => c.talentId);

        const [works, talents] = await Promise.all([
            worksCollection.find({ collaborationId: { $in: collaborationIds } }).toArray(),
            talentsCollection.find({ id: { $in: talentIds } }).toArray()
        ]);
        
        let lastPublishDate = null;

        const talentData = collaborations.map(collab => {
            const work = works.find(w => w.collaborationId === collab.id) || {};
            const talentInfo = talents.find(t => t.id === collab.talentId);
            
            if (collab.publishDate) {
                const currentPublishDate = new Date(collab.publishDate);
                if (!lastPublishDate || currentPublishDate > lastPublishDate) {
                    lastPublishDate = currentPublishDate;
                }
            }

            const executionAmount = (Number(collab.amount) || 0) * projectDiscount * 1.05;

            // --- T+7 Metrics ---
            const t7_views = work.t7_totalViews || 0;
            const t7_likes = work.t7_likeCount || 0;
            const t7_comments = work.t7_commentCount || 0;
            const t7_shares = work.t7_shareCount || 0;
            const t7_interactions = t7_likes + t7_comments + t7_shares;
            
            const t7_componentClicks = work.t7_componentClickCount || 0;
            const t7_componentImpressions = work.t7_componentImpressionCount || 0;

            const t7_completionRate = work.t7_completionRate || 0;
            const t7_completionViews = t7_views * t7_completionRate;

            const t7_reachByFrequency = work.t7_reachByFrequency || {};
            const t7_totalReach = Object.values(t7_reachByFrequency).reduce((sum, count) => sum + (count || 0), 0);

            const t7_cpm = t7_views > 0 ? (executionAmount / t7_views) * 1000 : 0;
            const t7_cpe = t7_interactions > 0 ? executionAmount / t7_interactions : 0;
            const t7_ctr = t7_componentImpressions > 0 ? t7_componentClicks / t7_componentImpressions : 0;
            const t7_interactionRate = t7_views > 0 ? t7_interactions / t7_views : 0;
            const t7_likeToViewRatio = t7_views > 0 ? t7_likes / t7_views : 0;
            
            // --- T+21 Metrics (完整补全) ---
            const t21_views = work.t21_totalViews || 0;
            const t21_likes = work.t21_likeCount || 0;
            const t21_comments = work.t21_commentCount || 0;
            const t21_shares = work.t21_shareCount || 0;
            const t21_interactions = t21_likes + t21_comments + t21_shares;
            
            const t21_componentClicks = work.t21_componentClickCount || 0;
            const t21_componentImpressions = work.t21_componentImpressionCount || 0;

            const t21_completionRate = work.t21_completionRate || 0;
            const t21_completionViews = t21_views * t21_completionRate;

            const t21_reachByFrequency = work.t21_reachByFrequency || {};
            const t21_totalReach = Object.values(t21_reachByFrequency).reduce((sum, count) => sum + (count || 0), 0);

            const t21_cpm = t21_views > 0 ? (executionAmount / t21_views) * 1000 : 0;
            const t21_cpe = t21_interactions > 0 ? executionAmount / t21_interactions : 0;
            const t21_ctr = t21_componentImpressions > 0 ? t21_componentClicks / t21_componentImpressions : 0;
            const t21_interactionRate = t21_views > 0 ? t21_interactions / t21_views : 0;
            const t21_likeToViewRatio = t21_views > 0 ? t21_likes / t21_views : 0;

            return {
                id: collab.id,
                talentName: talentInfo?.nickname || '未知达人',
                publishDate: collab.publishDate,
                executionAmount,
                
                // T+7 完整指标
                t7_views, t7_likes, t7_comments, t7_shares, t7_interactions,
                t7_componentClicks, t7_componentImpressions,
                t7_completionRate, t7_completionViews,
                t7_totalReach,
                t7_cpm, t7_cpe, t7_ctr, t7_interactionRate, t7_likeToViewRatio,
                
                // T+21 完整指标（新增）
                t21_views, t21_likes, t21_comments, t21_shares, t21_interactions,
                t21_componentClicks, t21_componentImpressions,
                t21_completionRate, t21_completionViews,
                t21_totalReach,
                t21_cpm, t21_cpe, t21_ctr, t21_interactionRate, t21_likeToViewRatio
            };
        });

        const overall = talentData.reduce((acc, talent) => {
            acc.totalExecutionAmount += talent.executionAmount;
            
            // T+7 汇总
            acc.t7_totalViews += talent.t7_views;
            acc.t7_totalInteractions += talent.t7_interactions;
            acc.t7_totalComponentClicks += talent.t7_componentClicks;
            acc.t7_totalComponentImpressions += talent.t7_componentImpressions;
            
            // T+21 汇总
            if (talent.t21_views !== null && talent.t21_views !== undefined) {
                acc.t21_totalViews += talent.t21_views;
                acc.t21_totalInteractions += talent.t21_interactions;
            } else {
                acc.hasMissingT21 = true;
            }
            return acc;
        }, {
            totalExecutionAmount: 0, 
            t7_totalViews: 0, t7_totalInteractions: 0,
            t7_totalComponentClicks: 0, t7_totalComponentImpressions: 0,
            t21_totalViews: 0, t21_totalInteractions: 0,
            hasMissingT21: false
        });

        overall.t7_cpm = overall.t7_totalViews > 0 ? (overall.totalExecutionAmount / overall.t7_totalViews) * 1000 : 0;
        overall.t7_cpe = overall.t7_totalInteractions > 0 ? (overall.totalExecutionAmount / overall.t7_totalInteractions) : 0;
        overall.t7_ctr = overall.t7_totalComponentImpressions > 0 ? overall.t7_totalComponentClicks / overall.t7_totalComponentImpressions : 0;

        if (overall.hasMissingT21) {
            overall.t21_totalViews = null;
            overall.t21_totalInteractions = null;
            overall.t21_cpm = null;
            overall.targetViews = null;
            overall.viewsGap = null;
        } else {
            overall.t21_cpm = overall.t21_totalViews > 0 ? (overall.totalExecutionAmount / overall.t21_totalViews) * 1000 : 0;
            if (benchmarkCPM && benchmarkCPM > 0) {
                overall.targetViews = (overall.totalExecutionAmount / benchmarkCPM) * 1000;
                overall.viewsGap = overall.t21_totalViews - overall.targetViews;
            } else {
                overall.targetViews = null;
                overall.viewsGap = null;
            }
        }
        
        const deliveryDate = addDays(lastPublishDate, 21);

        const responseData = {
            overall: {
                benchmarkCPM,
                deliveryDate,
                targetViews: overall.targetViews,
                viewsGap: overall.viewsGap,
                t7_totalViews: overall.t7_totalViews,
                t7_totalInteractions: overall.t7_totalInteractions,
                t7_cpm: overall.t7_cpm,
                t7_cpe: overall.t7_cpe,
                t7_ctr: overall.t7_ctr,
                t21_totalViews: overall.t21_totalViews,
                t21_totalInteractions: overall.t21_totalInteractions,
                t21_cpm: overall.t21_cpm
            },
            talents: talentData
        };

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify(responseData),
        };
    } catch (error) {
        console.error('Error in getProjectPerformance function:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ message: `服务器内部错误: ${error.message}` }),
        };
    } finally {
        if (client) {
            await client.close();
        }
    }
};