/**
 * @file syncFromFeishu.js
 * @version 4.0 - Upgraded Handler
 * @description [架构升级] 统一的飞书数据处理API入口。
 * - [升级] 适配新的 handleFeishuRequest 调度器，支持获取 schemas 等新操作。
 */
const { handleFeishuRequest } = require('./utils.js');

exports.handler = async (event, context) => {
    // 为CORS和JSON响应设置标准头
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS', // 允许 GET 请求
        'Content-Type': 'application/json'
    };

    // 处理CORS预检请求
    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 204, headers, body: '' };
    }

    try {
        // 根据请求方法确定如何解析 body
        let body;
        if (event.httpMethod === 'POST') {
            body = JSON.parse(event.body || '{}');
        } else if (event.httpMethod === 'GET') {
            // 对于GET请求，我们将查询参数作为 payload
            body = {
                dataType: event.queryStringParameters.dataType,
                payload: event.queryStringParameters
            };
        } else {
            throw new AppError(`Unsupported HTTP method: ${event.httpMethod}`, 405);
        }
        
        // 将请求体直接传递给总调度函数处理
        const result = await handleFeishuRequest(body);

        // 返回一个成功的HTTP响应
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                data: result 
            }),
        };

    } catch (error) {
        console.error('An error occurred in the syncFromFeishu handler:', error);

        const statusCode = error.statusCode || 500;
        return {
            statusCode: statusCode,
            headers,
            body: JSON.stringify({
                success: false,
                message: `An error occurred: ${error.message}`
            }),
        };
    }
};

