/**
 * @file feishu-notifier/index.js
 * @version 2.0.0 (Bot App API Version)
 * @description
 * - [核心架构升级] 彻底重构了消息发送方式。
 * - [移除] 不再使用自定义机器人的 Webhook URL。
 * - [新增] 改为使用飞书官方的 IM API (`/im/v1/messages`) 来发送消息。
 * - [认证] 函数现在会自行获取 tenant_access_token，以“KOL项目数据同步助手”应用的身份进行认证和发送。
 * - [目标] 能够通过环境变量中配置的 Chat ID，将消息精确发送到指定群聊。
 * - [目的] 解决了因发送主体（自定义机器人）与交互主体（应用）不一致而导致的回调失败问题。
 */
const axios = require('axios');

// --- 从环境变量中获取配置 ---
const APP_ID = process.env.FEISHU_APP_ID;
const APP_SECRET = process.env.FEISHU_APP_SECRET;
// [新增] 目标群聊的ID
const CHAT_ID = process.env.FEISHU_CHAT_ID;

// --- 模块级缓存，用于存储 access_token ---
let tenantAccessToken = null;
let tokenExpiresAt = 0;

/**
 * 获取并缓存飞书的 tenant_access_token
 * @returns {Promise<string>} tenant_access_token
 */
async function getTenantAccessToken() {
    if (Date.now() < tokenExpiresAt && tenantAccessToken) {
        return tenantAccessToken;
    }

    if (!APP_ID || !APP_SECRET) {
        throw new Error('环境变量 FEISHU_APP_ID 或 FEISHU_APP_SECRET 未配置。');
    }

    try {
        const response = await axios.post(
            'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal', {
                app_id: APP_ID,
                app_secret: APP_SECRET,
            }
        );

        if (response.data.code !== 0) {
            throw new Error(`获取 tenant_access_token 失败: ${response.data.msg}`);
        }

        tenantAccessToken = response.data.tenant_access_token;
        // 在过期前5分钟刷新token
        tokenExpiresAt = Date.now() + (response.data.expire - 300) * 1000;
        console.log("成功获取新的 tenant_access_token。");
        return tenantAccessToken;
    } catch (error) {
        const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
        console.error("getTenantAccessToken error:", errorMessage);
        throw new Error(`获取 tenant_access_token 发生网络或API错误: ${errorMessage}`);
    }
}

/**
 * 云函数主处理程序
 */
exports.handler = async (event, context) => {
    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 204, headers };
    }

    try {
        if (!CHAT_ID) {
            throw new Error("环境变量 FEISHU_CHAT_ID 未配置，无法确定发送目标群聊。");
        }

        const task = JSON.parse(event.body || '{}');
        
        // 关键验证：检查任务对象和其主键 `_id` 或 `id` 是否存在
        if (!task || !(task._id || task.id)) {
            return {
                statusCode: 400,
                headers,
                body: JSON.stringify({ success: false, message: '请求体中缺少有效的任务数据 (必须包含 _id 或 id)。' })
            };
        }
        
        const taskId = (task._id || task.id).toString();

        // 1. 获取认证 Token
        const accessToken = await getTenantAccessToken();

        // 2. 根据任务数据构建消息卡片
        const cardJson = {
            "config": {
                "wide_screen_mode": true
            },
            "header": {
                "template": "red",
                "title": {
                    "tag": "plain_text",
                    "content": `🚨 新任务提醒: ${task.title}`
                }
            },
            "elements": [{
                    "tag": "div",
                    "fields": [{
                        "is_short": true,
                        "text": {
                            "tag": "lark_md",
                            "content": `**所属项目:**\n${task.projectName || '系统全局任务'}`
                        }
                    }, {
                        "is_short": true,
                        "text": {
                            "tag": "lark_md",
                            "content": `**任务类型:**\n${task.title || '未分类'}`
                        }
                    }]
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": `**详细内容:**\n${task.description}`
                    }
                },
                {
                    "tag": "action",
                    "actions": [{
                        "tag": "button",
                        "text": {
                            "tag": "plain_text",
                            "content": "✅ 标记为已处理"
                        },
                        "type": "primary",
                        "value": {
                            "action": "complete_task",
                            "task_id": taskId
                        }
                    }, {
                        "tag": "button",
                        "text": {
                            "tag": "plain_text",
                            "content": "前往处理"
                        },
                        "type": "default",
                        // 请将此URL替换为您的产品任务中心的实际链接
                        "url": "https://www.feishu.cn/base/home" 
                    }]
                }
            ]
        };

        // 3. 调用飞书API发送消息
        const feishuApiUrl = `https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id`;
        const response = await axios.post(feishuApiUrl, {
            receive_id: CHAT_ID,
            msg_type: 'interactive',
            content: JSON.stringify(cardJson)
        }, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json; charset=utf-8',
            }
        });

        if (response.data.code !== 0) {
            console.error("飞书API发送消息失败:", response.data);
            throw new Error(`飞书API错误: ${response.data.msg}`);
        }

        console.log(`成功向 Chat ID ${CHAT_ID} 发送任务 ${taskId} 的通知卡片。`);

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: true, message: '通知发送成功' }),
        };

    } catch (error) {
        console.error('feishu-notifier 运行时发生错误:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: error.message }),
        };
    }
};

