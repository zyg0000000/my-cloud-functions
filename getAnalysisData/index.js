/**
 * @file my-cloud-functions/getAnalysisData/index.js
 * @version 1.3
 * @description Performance and stability patch for the analysis data endpoint.
 *
 * @changelog
 * - v1.3 (2025-10-19):
 * - [PERFORMANCE] Refactored the entire function to use a single, efficient `$facet` aggregation pipeline. This replaces the previous four parallel queries, fundamentally resolving the "context canceled" timeout issue by reducing database load.
 * - [ROBUSTNESS] Added basic validation for input filters to prevent query errors.
 * - [LOGGING] Enhanced logging in the catch block to provide more context on failure.
 * - v1.2 (2025-10-19): Fixed month sorting, added dynamic sorting for talent ranks.
 * - v1.1 (2025-10-19): Initial creation.
 */
const { MongoClient } = require('mongodb');

// --- Database Configuration ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'kol_data';

let client;

// --- Database Connection ---
async function connectToDatabase() {
    if (client && client.topology.isConnected()) {
        return client;
    }
    client = new MongoClient(MONGO_URI);
    await client.connect();
    return client;
}

// --- Main Handler ---
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

    const body = JSON.parse(event.body || '{}');
    const { filters = {}, talentSortBy = 'totalProfit', talentLimit = 20 } = body;

    try {
        const dbClient = await connectToDatabase();
        const db = dbClient.db(DB_NAME);
        const projectsCollection = db.collection('projects');
        const collabsCollection = db.collection('collaborations');
        const talentsCollection = db.collection('talents');

        // --- Available Filters (Separate, lightweight query) ---
        const [availableYears, availableProjectTypes] = await Promise.all([
            projectsCollection.distinct('financialYear'),
            projectsCollection.distinct('type')
        ]);

        // --- Main Aggregation Pipeline ---
        const matchStage = {};
        if (filters.year && typeof filters.year === 'string' && filters.year.length > 0) {
            matchStage.financialYear = filters.year;
        }
        if (filters.projectType && typeof filters.projectType === 'string' && filters.projectType.length > 0) {
            matchStage.type = filters.projectType;
        }

        const aggregationResult = await collabsCollection.aggregate([
            { $lookup: { from: 'projects', localField: 'projectId', foreignField: 'id', as: 'projectInfo' } },
            { $unwind: '$projectInfo' },
            { $match: { 'projectInfo.id': { $ne: null }, ...matchStage } },
            { $lookup: { from: 'talents', localField: 'talentId', foreignField: 'id', as: 'talentInfo' } },
            { $unwind: '$talentInfo' },
            {
                $facet: {
                    // 1. KPI Summary
                    kpiSummary: [
                        {
                            $group: {
                                _id: null,
                                totalIncome: { $sum: '$metrics.income' },
                                totalProfit: { $sum: '$metrics.grossProfit' },
                                projectIds: { $addToSet: '$projectId' },
                                totalCollaborations: { $sum: 1 }
                            }
                        },
                        {
                            $project: {
                                _id: 0,
                                totalIncome: 1,
                                totalProfit: 1,
                                totalProjects: { $size: '$projectIds' },
                                totalCollaborations: 1,
                                overallMargin: {
                                    $cond: { if: { $gt: ['$totalIncome', 0] }, then: { $multiply: [{ $divide: ['$totalProfit', '$totalIncome'] }, 100] }, else: 0 }
                                }
                            }
                        }
                    ],
                    // 2. Monthly Financials
                    monthlyFinancials: [
                        {
                            $group: {
                                _id: { month: '$projectInfo.financialMonth' },
                                totalIncome: { $sum: '$metrics.income' },
                                totalProfit: { $sum: '$metrics.grossProfit' }
                            }
                        },
                        {
                            $addFields: {
                                monthNum: { $toInt: { $substr: ['$_id.month', 1, -1] } }
                            }
                        },
                        { $sort: { monthNum: 1 } },
                        {
                            $project: {
                                _id: 0,
                                month: '$_id.month',
                                totalIncome: 1,
                                totalProfit: 1,
                                margin: {
                                    $cond: { if: { $gt: ['$totalIncome', 0] }, then: { $multiply: [{ $divide: ['$totalProfit', '$totalIncome'] }, 100] }, else: 0 }
                                }
                            }
                        }
                    ],
                    // 3. Analysis by Project Type
                    byProjectType: [
                        {
                            $group: {
                                _id: '$projectInfo.type',
                                totalIncome: { $sum: '$metrics.income' }
                            }
                        },
                        {
                            $project: {
                                _id: 0,
                                projectType: '$_id',
                                totalIncome: 1
                            }
                        },
                        { $sort: { totalIncome: -1 } }
                    ],
                    // 4. Top Talents
                    topTalents: [
                        {
                            $group: {
                                _id: '$talentId',
                                talentName: { $first: '$talentInfo.nickname' },
                                collaborationCount: { $sum: 1 },
                                totalAmount: { $sum: '$amount' },
                                totalProfit: { $sum: '$metrics.grossProfit' },
                                totalRebate: { $sum: '$rebate' }
                            }
                        },
                        { $sort: { [talentSortBy]: -1 } },
                        { $limit: talentLimit },
                        {
                            $project: {
                                _id: 0,
                                talentName: 1,
                                collaborationCount: 1,
                                totalAmount: 1,
                                totalProfit: 1,
                                averageRebate: {
                                    $cond: { if: { $gt: ['$collaborationCount', 0] }, then: { $divide: ['$totalRebate', '$collaborationCount'] }, else: 0 }
                                }
                            }
                        }
                    ]
                }
            }
        ]).toArray();
        
        const result = aggregationResult[0];

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                data: {
                    availableFilters: {
                        years: availableYears.sort((a, b) => b - a),
                        projectTypes: availableProjectTypes.sort()
                    },
                    kpiSummary: result.kpiSummary[0] || {},
                    monthlyFinancials: result.monthlyFinancials || [],
                    byProjectType: result.byProjectType || [],
                    topTalents: result.topTalents || []
                }
            }),
        };

    } catch (error) {
        console.error('Error in getAnalysisData handler:', error, 'with filters:', filters);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }),
        };
    }
};

