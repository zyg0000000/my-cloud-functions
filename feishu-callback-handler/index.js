/**
 * @file feishu-callback-handler/index.js
 * @version 2.1.0 (Debug & Connectivity Test Version)
 * @description
 * - [核心调试] 这是一个临时的调试版本，用于终极验证飞书回调的连通性。
 * - [简化逻辑] 暂时移除了所有数据库连接、异步处理和卡片更新的复杂逻辑。
 * - [核心功能] 函数现在只做一件事：
 * 1. 接收到任何POST请求。
 * 2. 立即在日志中打印 "Connectivity test successful!"。
 * 3. 立即返回一个HTTP 200的成功响应。
 * - [目的] 这个版本旨在100%排除我们后端代码逻辑出错的可能性，将问题定位在网络或飞书配置上。
 */
const crypto = require('crypto');

exports.handler = async (event, context) => {
    // 立即在日志中打印收到的所有信息，这是最重要的调试步骤
    console.log('[Connectivity Test] Received request. Event:', JSON.stringify(event, null, 2));

    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    };

    // 1. 响应CORS预检请求
    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 204, headers };
    }

    // 2. 响应飞书的URL验证挑战 (challenge)
    try {
        const body = JSON.parse(event.body || '{}');
        if (body.challenge) {
            console.log('[Connectivity Test] Responding to Feishu URL challenge.');
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({ challenge: body.challenge }),
            };
        }
    } catch (e) {
        // 如果JSON解析失败，也记录下来
        console.error('[Connectivity Test] Failed to parse event body:', e);
    }
    
    // 3. 对于任何其他POST请求（例如按钮点击），只记录日志并立即返回成功
    // 这样做可以确保我们绝对不会因为代码执行超时而导致飞书报错
    console.log('[Connectivity Test] Received a non-challenge POST request. Responding with 200 OK.');
    
    return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ "message": "Request received successfully." }),
    };
};

