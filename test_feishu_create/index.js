/**
 * @file feishu_sheets_api_test.js
 * @description 一个独立的、用于测试飞书表格创建、复制与信息获取功能的云函数。
 * - [重要更新] 新增 "full_test" 诊断操作，用于生成提报给官方的日志。
 */

const axios = require('axios');

const FEISHU_API_BASE_URL = 'https://open.feishu.cn';
const APP_ID = process.env.FEISHU_APP_ID;
const APP_SECRET = process.env.FEISHU_APP_SECRET;

let tenantAccessToken = null;
let tokenExpiresAt = 0;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function getTenantAccessToken() {
    if (Date.now() < tokenExpiresAt && tenantAccessToken) {
        return tenantAccessToken;
    }
    if (!APP_ID || !APP_SECRET) throw new Error('环境变量 FEISHU_APP_ID 或 FEISHU_APP_SECRET 未设置。');
    try {
        const response = await axios.post(
            `${FEISHU_API_BASE_URL}/open-apis/auth/v3/tenant_access_token/internal`,
            { app_id: APP_ID, app_secret: APP_SECRET },
            { headers: { 'Content-Type': 'application/json; charset=utf-8' } }
        );
        if (response.data.code !== 0) throw new Error(`获取 tenant_access_token 失败: ${response.data.msg}`);
        tenantAccessToken = response.data.tenant_access_token;
        tokenExpiresAt = Date.now() + (response.data.expire - 300) * 1000;
        console.log("成功获取新的 tenant_access_token。");
        return tenantAccessToken;
    } catch (error) {
        const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
        console.error("getTenantAccessToken error:", errorMessage);
        throw new Error(`获取 tenant_access_token 发生网络或API错误: ${errorMessage}`);
    }
}

async function getSpreadsheetInfo(accessToken, spreadsheetToken) {
    console.log(`[诊断] 正在获取表格元信息，Token: ${spreadsheetToken}...`);
    const response = await axios.get(
        `${FEISHU_API_BASE_URL}/open-apis/sheets/v3/spreadsheets/${spreadsheetToken}/metainfo`,
        { headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' } }
    );
    if (response.data.code !== 0) throw new Error(`获取表格元信息API调用失败: ${response.data.msg} (Code: ${response.data.code})`);
    console.log("[诊断] 表格元信息获取成功。");
    return response.data.data;
}

async function copySpreadsheet(accessToken, sourceToken, destinationFolderToken) {
    console.log(`[诊断] 正在复制表格，源Token: ${sourceToken}...`);
    const requestBody = { folder_token: destinationFolderToken };
    const response = await axios.post(
        `${FEISHU_API_BASE_URL}/open-apis/sheets/v3/spreadsheets/${sourceToken}/copy`,
        requestBody,
        { headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' } }
    );
    if (response.data.code !== 0) throw new Error(`复制表格API调用失败: ${response.data.msg} (Code: ${response.data.code})`);
    console.log("[诊断] 表格复制成功。");
    return response.data.data.spreadsheet;
}

async function createSpreadsheet(accessToken, title, folderToken) {
    console.log(`[诊断] 正在创建表格，标题: "${title}"...`);
    const requestBody = { title, folder_token: folderToken };
    const response = await axios.post(
        `${FEISHU_API_BASE_URL}/open-apis/sheets/v3/spreadsheets`,
        requestBody,
        { headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json; charset=utf-8' } }
    );
    if (response.data.code !== 0) throw new Error(`创建表格API调用失败: ${response.data.msg} (Code: ${response.data.code})`);
    console.log("[诊断] 表格创建成功。");
    return response.data.data.spreadsheet;
}

exports.handler = async (event) => {
    const headers = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'POST, OPTIONS', 'Content-Type': 'application/json' };
    if (event.httpMethod === 'OPTIONS') return { statusCode: 204, headers, body: '' };

    try {
        const body = JSON.parse(event.body || '{}');
        const { action, title, spreadsheetToken, folderToken } = body;
        const accessToken = await getTenantAccessToken();
        let responseData, message;

        switch (action) {
            case 'create':
                if (!title) throw new Error("创建操作需要 'title' 参数。");
                responseData = await createSpreadsheet(accessToken, title, folderToken);
                message = "表格创建成功!";
                break;
            case 'copy':
                if (!spreadsheetToken) throw new Error("复制操作需要 'spreadsheetToken' 参数。");
                responseData = await copySpreadsheet(accessToken, spreadsheetToken, folderToken);
                message = "表格复制成功!";
                break;
            case 'get_info':
                if (!spreadsheetToken) throw new Error("获取信息操作需要 'spreadsheetToken' 参数。");
                responseData = await getSpreadsheetInfo(accessToken, spreadsheetToken);
                message = "成功获取表格信息!";
                break;
            case 'full_test':
                console.log("--- 开始完整诊断测试 ---");
                const createdSheet = await createSpreadsheet(accessToken, "自动化诊断表格", folderToken);
                console.log(`步骤1: 创建成功, Token: ${createdSheet.spreadsheet_token}`);
                
                console.log("步骤2: 等待5秒，以排除任何可能的同步延迟...");
                await sleep(5000);
                
                let infoData, copyData;
                try {
                    infoData = await getSpreadsheetInfo(accessToken, createdSheet.spreadsheet_token);
                    console.log("步骤3: 获取信息成功。");
                } catch (e) {
                    console.error("步骤3: 获取信息失败!", e.message);
                    throw new Error(`诊断失败于[获取信息]阶段: ${e.message}`);
                }
                
                try {
                    copyData = await copySpreadsheet(accessToken, createdSheet.spreadsheet_token, folderToken);
                    console.log("步骤4: 复制成功。");
                } catch (e) {
                    console.error("步骤4: 复制失败!", e.message);
                    throw new Error(`诊断失败于[复制]阶段: ${e.message}`);
                }
                
                console.log("--- 完整诊断测试成功 ---");
                responseData = { created: createdSheet, info: infoData, copied: copyData };
                message = "完整诊断测试成功通过！";
                break;
            default:
                throw new Error(`无效的 action: '${action}'。`);
        }
        return { statusCode: 200, headers, body: JSON.stringify({ success: true, message, data: responseData }) };
    } catch (error) {
        console.error('云函数执行出错:', error);
        let detailedMessage = error.message;
        if (error.response) {
            console.error('飞书 API 错误响应体:', JSON.stringify(error.response.data, null, 2));
            const apiError = error.response.data;
            detailedMessage = `飞书 API 返回错误: ${apiError.msg || JSON.stringify(apiError)} (Code: ${apiError.code}, HTTP Status: ${error.response.status})`;
        }
        return { statusCode: 500, headers, body: JSON.stringify({ success: false, message: `执行出错: ${detailedMessage}` }) };
    }
};

