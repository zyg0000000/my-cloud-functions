/**

 * @file getFieldMetadata/index.js

 * @version 1.0.0

 * @description 返回所有可导出字段的元数据，支持前端动态渲染维度

 */

 

const FIELD_METADATA = {

    talent: {

        '基础信息': [

            { id: 'nickname', label: '达人昵称', backendKey: '达人昵称', dataType: 'string' },

            { id: 'xingtuId', label: '星图ID', backendKey: '星图ID', dataType: 'string' },

            { id: 'uid', label: 'UID', backendKey: 'UID', dataType: 'string' },

            { id: 'talentTier', label: '达人层级', backendKey: '达人层级', dataType: 'string' },

            { id: 'talentSource', label: '达人来源', backendKey: '达人来源', dataType: 'string' },

            { id: 'talentType', label: '内容标签', backendKey: '内容标签', dataType: 'string' },

        ],

        '商务信息': [

            { id: 'price', label: '一口价 (指定月份)', backendKey: '一口价', dataType: 'number' },

            { id: 'highestRebate', label: '最高返点率', backendKey: '最高返点率', dataType: 'percentage' },

        ],

        '合作数据': [

            { id: 'collaboration_count', label: '历史合作总次数', backendKey: '历史合作总次数', dataType: 'number' },

        ],

        '作品表现 (T+7)': [

            { id: 'work_total_t7_views', label: 'T+7 总播放量', backendKey: 'T+7 总播放量', dataType: 'number' }

        ],

        '粉丝画像 - 基础指标': [

            { id: 'cpm60s', label: '60s+预期CPM', backendKey: 'cpm60s', dataType: 'number' },

            { id: 'maleAudienceRatio', label: '男性观众比例', backendKey: 'maleAudienceRatio', dataType: 'percentage' },

            { id: 'femaleAudienceRatio', label: '女性观众比例', backendKey: 'femaleAudienceRatio', dataType: 'percentage' },

        ],

        '粉丝画像 - 年龄段分布': [

            { id: 'ratio_18_23', label: '18-23岁粉丝比例', backendKey: 'ratio_18_23', dataType: 'percentage' },

            { id: 'ratio_24_30', label: '24-30岁粉丝比例', backendKey: 'ratio_24_30', dataType: 'percentage' },

            { id: 'ratio_31_40', label: '31-40岁粉丝比例', backendKey: 'ratio_31_40', dataType: 'percentage' },

            { id: 'ratio_41_50', label: '41-50岁粉丝比例', backendKey: 'ratio_41_50', dataType: 'percentage' },

            { id: 'ratio_50_plus', label: '50岁以上粉丝比例', backendKey: 'ratio_50_plus', dataType: 'percentage' },

            { id: 'audience_18_40_ratio', label: '18-40岁观众占比（计算）', backendKey: 'audience_18_40_ratio', dataType: 'percentage' },

            { id: 'audience_40_plus_ratio', label: '40岁以上观众占比（计算）', backendKey: 'audience_40_plus_ratio', dataType: 'percentage' },

        ],

        '粉丝画像 - 八大人群': [

            { id: 'ratio_town_middle_aged', label: '小镇中老年粉丝比例', backendKey: 'ratio_town_middle_aged', dataType: 'percentage' },

            { id: 'ratio_senior_middle_class', label: '资深中产粉丝比例', backendKey: 'ratio_senior_middle_class', dataType: 'percentage' },

            { id: 'ratio_z_era', label: 'Z时代粉丝比例', backendKey: 'ratio_z_era', dataType: 'percentage' },

            { id: 'ratio_urban_silver', label: '都市银发粉丝比例', backendKey: 'ratio_urban_silver', dataType: 'percentage' },

            { id: 'ratio_town_youth', label: '小镇青年粉丝比例', backendKey: 'ratio_town_youth', dataType: 'percentage' },

            { id: 'ratio_exquisite_mom', label: '精致妈妈粉丝比例', backendKey: 'ratio_exquisite_mom', dataType: 'percentage' },

            { id: 'ratio_new_white_collar', label: '新锐白领粉丝比例', backendKey: 'ratio_new_white_collar', dataType: 'percentage' },

            { id: 'ratio_urban_blue_collar', label: '都市蓝领粉丝比例', backendKey: 'ratio_urban_blue_collar', dataType: 'percentage' },

        ]

    },

 

    collaboration: {

        '合作信息': [

            { id: 'collaboration_status', label: '合作状态', backendKey: '合作状态', dataType: 'string' },

            { id: 'collaboration_amount', label: '合作金额', backendKey: '合作金额', dataType: 'number' },

            { id: 'collaboration_orderType', label: '下单方式', backendKey: '下单方式', dataType: 'string' },

            { id: 'collaboration_plannedReleaseDate', label: '计划发布日期', backendKey: '计划发布日期', dataType: 'date' },

            { id: 'collaboration_publishDate', label: '实际发布日期', backendKey: '实际发布日期', dataType: 'date' },

        ],

        '项目信息': [

            { id: 'project_name', label: '所属项目', backendKey: '项目名称', dataType: 'string' },

            { id: 'project_type', label: '项目类型', backendKey: '项目类型', dataType: 'string' },

        ],

        '达人信息': [

            { id: 'nickname', label: '达人昵称', backendKey: '达人昵称', dataType: 'string' },

            { id: 'talentTier', label: '达人层级', backendKey: '达人层级', dataType: 'string' },

        ],

        '作品表现 (T+7)': [

            { id: 'work_t7_totalViews', label: 'T+7 播放量', backendKey: 'T+7 播放量', dataType: 'number' },

            { id: 'work_t7_likeCount', label: 'T+7 点赞数', backendKey: 'T+7 点赞数', dataType: 'number' },

        ]

    },

 

    project: {

        '合作信息': [

            { id: 'collaboration_status', label: '合作状态', backendKey: '合作状态', dataType: 'string' },

            { id: 'collaboration_amount', label: '合作金额', backendKey: '合作金额', dataType: 'number' },

            { id: 'collaboration_orderType', label: '下单方式', backendKey: '下单方式', dataType: 'string' },

            { id: 'collaboration_plannedReleaseDate', label: '计划发布日期', backendKey: '计划发布日期', dataType: 'date' },

            { id: 'collaboration_publishDate', label: '实际发布日期', backendKey: '实际发布日期', dataType: 'date' },

        ],

        '项目信息': [

            { id: 'project_name', label: '所属项目', backendKey: '项目名称', dataType: 'string' },

            { id: 'project_type', label: '项目类型', backendKey: '项目类型', dataType: 'string' },

        ],

        '达人信息': [

            { id: 'nickname', label: '达人昵称', backendKey: '达人昵称', dataType: 'string' },

            { id: 'talentTier', label: '达人层级', backendKey: '达人层级', dataType: 'string' },

        ],

        '作品表现 (T+7)': [

            { id: 'work_t7_totalViews', label: 'T+7 播放量', backendKey: 'T+7 播放量', dataType: 'number' },

            { id: 'work_t7_likeCount', label: 'T+7 点赞数', backendKey: 'T+7 点赞数', dataType: 'number' },

        ]

    }

};

 

/**

 * 云函数主入口

 */

exports.handler = async (event) => {

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

        // 解析查询参数

        const params = event.queryStringParameters || {};

        const entity = params.entity;

 

        // 如果指定了实体类型，返回该实体的元数据

        if (entity && FIELD_METADATA[entity]) {

            return {

                statusCode: 200,

                headers,

                body: JSON.stringify({

                    success: true,

                    entity: entity,

                    metadata: FIELD_METADATA[entity]

                })

            };

        }

 

        // 否则返回所有实体的元数据

        return {

            statusCode: 200,

            headers,

            body: JSON.stringify({

                success: true,

                metadata: FIELD_METADATA

            })

        };

 

    } catch (error) {

        console.error('Get field metadata error:', error);

        return {

            statusCode: 500,

            headers,

            body: JSON.stringify({

                success: false,

                message: '获取字段元数据失败',

                error: error.message

            })

        };

    }

};