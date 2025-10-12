/**
 * @file utils.js
 * @version 10.1 - Perfectly Integrated Edition
 * @description
 * - [完美整合版] 此版本为解决所有已知问题、并完整包含所有历史功能的最终生产版本。
 * - [功能补齐] 完整恢复了 v8.2/v8.7 中的 handleTalentImport 和 performProjectSync 数据导入功能。
 * - [核心保留] 保留了 v10.x 版本的所有优点：强大的公式引擎、正确的数字格式上传、稳定的文件操作流程。
 * - [日志增强] 融合了 v8.7 版本中最详尽的日志记录。
 */
const axios = require('axios');
const { MongoClient, ObjectId } = require('mongodb');

// --- 安全配置 ---
const APP_ID = process.env.FEISHU_APP_ID;
const APP_SECRET = process.env.FEISHU_APP_SECRET;
const DB_NAME = 'kol_data';
const MONGO_URI = process.env.MONGO_URI;
// 用于转移所有权的用户ID，应配置为 user_id
const FEISHU_OWNER_ID = process.env.FEISHU_OWNER_ID;
// 用于分享编辑权限的用户ID列表 (open_id)
const FEISHU_SHARE_USER_IDS = process.env.FEISHU_SHARE_USER_IDS;


// --- 模块级缓存 ---
let tenantAccessToken = null;
let tokenExpiresAt = 0;
let dbClient = null;

// --- 数据结构“总菜单”定义 ---
const DATA_SCHEMAS = {
    talents: { displayName: "达人信息", fields: [ { path: "nickname", displayName: "达人昵称" }, { path: "xingtuId", displayName: "星图ID" }, { path: "uid", displayName: "UID" }, { path: "latestPrice", displayName: "最新价格", isSpecial: true }, ] },
    projects: { displayName: "项目信息", fields: [ { path: "name", displayName: "项目名称" }, { path: "qianchuanId", displayName: "仟传项目编号" }, ] },
    collaborations: { displayName: "合作信息", fields: [ { path: "taskId", displayName: "任务ID (星图)" }, { path: "videoId", displayName: "视频ID (平台)" }, { path: "orderType", displayName: "订单类型" }, { path: "status", displayName: "合作状态" }, { path: "amount", displayName: "合作金额" }, { path: "publishDate", displayName: "实际发布日期" }, ] },
    "automation-tasks": { displayName: "自动化任务", fields: [ { path: "result.data.预期CPM", displayName: "预期CPM" }, { path: "result.data.完播率", displayName: "完播率" }, { path: "result.data.爆文率", displayName: "爆文率" }, { path: "result.data.个人视频播放量均值", displayName: "个人视频播放量均值" }, { path: "result.data.星图频播放量均值", displayName: "星图视频播放量均值" }, { path: "result.data.用户画像总结", displayName: "用户画像总结" }, { path: "result.screenshots.0.url", displayName: "截图1 (达人价格)", isImage: true }, { path: "result.screenshots.1.url", displayName: "截图2 (星图视频)", isImage: true }, { path: "result.screenshots.2.url", displayName: "截图3 (男女比例)", isImage: true }, { path: "result.screenshots.3.url", displayName: "截图4 (年龄分布)", isImage: true }, { path: "result.screenshots.4.url", displayName: "截图5 (城市等级)", isImage: true }, { path: "result.screenshots.5.url", displayName: "截图6 (八大人群)", isImage: true }, { path: "result.screenshots.6.url", displayName: "截图7 (设备截图)", isImage: true }, ] }
};

// --- 辅助函数 ---
class AppError extends Error {
    constructor(message, statusCode) { super(message); this.statusCode = statusCode; }
}

async function getDbConnection() {
    if (dbClient && dbClient.topology && dbClient.topology.isConnected()) return dbClient;
    if (!MONGO_URI) throw new AppError('MONGO_URI environment variable is not set.', 500);
    dbClient = new MongoClient(MONGO_URI);
    await dbClient.connect();
    return dbClient;
}

async function getTenantAccessToken() {
    if (Date.now() < tokenExpiresAt && tenantAccessToken) return tenantAccessToken;
    if (!APP_ID || !APP_SECRET) throw new AppError('FEISHU_APP_ID/APP_SECRET environment variables are not set.', 500);
    const response = await axios.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal', { app_id: APP_ID, app_secret: APP_SECRET });
    if (response.data.code !== 0) throw new AppError(`Failed to get tenant access token: ${response.data.msg}`, 500);
    tenantAccessToken = response.data.tenant_access_token;
    tokenExpiresAt = Date.now() + (response.data.expire - 300) * 1000;
    return tenantAccessToken;
}

function getSpreadsheetTokenFromUrl(url) {
    if (!url || typeof url !== 'string') return null;
    if (!url.includes('/')) return url;
    try {
        const pathParts = new URL(url).pathname.split('/');
        const driveTypeIndex = pathParts.findIndex(part => ['sheets', 'folder'].includes(part));
        if (driveTypeIndex > -1 && pathParts.length > driveTypeIndex + 1) {
            return pathParts[driveTypeIndex + 1];
        }
    } catch (error) {
        console.warn(`Could not parse URL: ${url}`, error);
    }
    return url;
}

function columnIndexToLetter(index) {
    let letter = '';
    while (index >= 0) {
        letter = String.fromCharCode(index % 26 + 65) + letter;
        index = Math.floor(index / 26) - 1;
    }
    return letter;
}

// --- 计算引擎核心 ---
function parseToNumberForEval(value) {
    if (value === null || value === undefined) return 0;
    if (typeof value === 'number') return value;
    if (typeof value !== 'string') return 0;
    let numStr = value.replace(/,/g, '').trim();
    if (numStr.endsWith('%')) {
        const num = parseFloat(numStr);
        return isNaN(num) ? 0 : num / 100;
    }
    if (numStr.toLowerCase().endsWith('w') || numStr.includes('万')) {
        const num = parseFloat(numStr.replace(/w|万/gi, ''));
        return isNaN(num) ? 0 : num * 10000;
    }
    const num = parseFloat(numStr);
    return isNaN(num) ? 0 : num;
}

function evaluateFormula(formula, dataContext) {
    try {
        const FORMULA_FUNCTIONS = {
            REPLACE: (text, from, to) => {
                const sourceText = text === null || text === undefined ? '' : String(text);
                return sourceText.replace(new RegExp(from, 'g'), to);
            }
        };

        const variableRegex = /\{(.+?)\}/g;
        const isStringContext = /"|'|REPLACE\s*\(/i.test(formula);
        
        let expression = formula;

        expression = expression.replace(variableRegex, (match, varPath) => {
            const pathParts = varPath.split('.');
            const collection = pathParts[0];
            const trueContext = dataContext[collection];
            const value = pathParts.slice(1).reduce((obj, key) => (obj && obj[key] !== undefined) ? obj[key] : null, trueContext);
            
            if (value === null || value === undefined) {
                return isStringContext ? '""' : '0';
            }

            if (isStringContext) {
                return JSON.stringify(String(value));
            }
            return parseToNumberForEval(String(value));
        });

        expression = expression.replace(/REPLACE\s*\(/gi, 'FORMULA_FUNCTIONS.REPLACE(');

        if (!isStringContext && /\/\s*0(?!\.)/.test(expression)) {
            return 'N/A';
        }

        const calculate = new Function('FORMULA_FUNCTIONS', `return ${expression}`);
        const result = calculate(FORMULA_FUNCTIONS);

        return result;

    } catch (error) {
        console.error(`执行公式 "${formula}" 时出错:`, error);
        return 'N/A';
    }
}

function formatOutput(value, format) {
    if (value === 'N/A' || value === null || value === undefined) return 'N/A';
    if (format === 'percentage') {
        const num = parseToNumberForEval(value);
        if (isNaN(num)) return 'N/A';
        return `${(num * 100).toFixed(2)}%`;
    }
    const numberMatch = format.match(/number\((\d+)\)/);
    if (numberMatch) {
        const num = parseToNumberForEval(value);
        if (isNaN(num)) return 'N/A';
        return num.toFixed(parseInt(numberMatch[1], 10));
    }
    return String(value);
}

// --- 飞书API辅助函数 ---
async function writeImageToCell(token, spreadsheetToken, range, imageUrl, imageName = 'image.png') {
    if (!imageUrl || !imageUrl.startsWith('http')) {
        console.log(`--> [图片] 无效的图片链接，跳过写入: ${imageUrl}`);
        return;
    }
    try {
        console.log(`--> [图片] 正在从 ${imageUrl} 下载图片...`);
        const imageResponse = await axios.get(imageUrl, { responseType: 'arraybuffer' });
        const imageBuffer = Buffer.from(imageResponse.data, 'binary');
        const imageBase64 = imageBuffer.toString('base64');
        const payload = { range, image: imageBase64, name: imageName };
        
        console.log(`--> [图片] 准备写入图片到 ${range}...`);
        const writeResponse = await axios.post(
          `https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${spreadsheetToken}/values_image`,
          payload,
          { headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' } }
        );

        if (writeResponse.data.code !== 0) {
            console.error(`--> [图片] 写入图片到 ${range} 失败:`, writeResponse.data.msg);
        } else {
            console.log(`--> [图片] 成功写入图片到 ${range}`);
        }
    } catch (error) {
        const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
        console.error(`--> [图片] 处理图片 ${imageUrl} 时发生严重错误: ${errorMessage}`);
    }
}

async function readFeishuSheet(spreadsheetToken, token, range) {
    const sheetsResponse = await axios.get(`https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/${spreadsheetToken}/sheets/query`, { headers: { 'Authorization': `Bearer ${token}` } });
    if (sheetsResponse.data.code !== 0) throw new AppError(`Failed to get sheets info: ${sheetsResponse.data.msg}`, 500);
    const firstSheetId = sheetsResponse.data.data.sheets[0].sheet_id;
    const finalRange = range || `${firstSheetId}!A1:ZZ2000`;
    const urlEncodedRange = encodeURIComponent(finalRange.startsWith(firstSheetId) ? finalRange : `${firstSheetId}!${finalRange}`);
    const valuesResponse = await axios.get(`https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${spreadsheetToken}/values/${urlEncodedRange}?valueRenderOption=ToString`, { headers: { 'Authorization': `Bearer ${token}` } });
    if (valuesResponse.data.code !== 0) throw new AppError(`Failed to read sheet values: ${valuesResponse.data.msg}`, 500);
    return valuesResponse.data.data.valueRange.values;
}

async function transferOwner(fileToken, token) {
    if (!FEISHU_OWNER_ID) { console.log("--> [权限] 未配置 FEISHU_OWNER_ID, 无法转移所有权。"); return false; }
    console.log(`--> [权限] 准备将文件所有权转移给用户: ${FEISHU_OWNER_ID}`);
    try {
        await axios.post(`https://open.feishu.cn/open-apis/drive/v1/permissions/${fileToken}/members/transfer_owner`, { member_type: 'userid', member_id: FEISHU_OWNER_ID }, { headers: { 'Authorization': `Bearer ${token}` }, params: { type: 'sheet', need_notification: true, remove_old_owner: false, stay_put: false, old_owner_perm: 'full_access' } });
        console.log(`--> [权限] 成功将所有权转移给用户: ${FEISHU_OWNER_ID}`);
        return true;
    } catch (error) {
        const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
        console.error(`--> [权限] 转移所有权失败: ${errorMessage}`);
        return false;
    }
}

async function grantEditPermissions(fileToken, token) {
    if (!FEISHU_SHARE_USER_IDS) { console.log("--> [权限] 未配置 FEISHU_SHARE_USER_IDS, 跳过分享编辑权限。"); return; }
    const userIds = FEISHU_SHARE_USER_IDS.split(',').map(id => id.trim()).filter(id => id);
    if (userIds.length === 0) return;
    console.log(`--> [权限] 准备将表格编辑权限分享给 ${userIds.length} 位用户...`);
    for (const userId of userIds) {
        try {
            await axios.post(`https://open.feishu.cn/open-apis/drive/v1/permissions/${fileToken}/members`, { member_type: 'user', member_id: userId, perm: 'edit' }, { headers: { 'Authorization': `Bearer ${token}` }, params: { type: 'sheet' } });
            console.log(`--> [权限] 成功将编辑权限授予用户: ${userId}`);
        } catch (error) {
            const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
            console.error(`--> [权限] 为用户 ${userId} 授予权限失败: ${errorMessage}`);
        }
    }
}

async function moveFileToFolder(fileToken, fileType, folderToken, token) {
    if (!folderToken) {
        console.log("--> [移动] 未提供目标文件夹Token，跳过移动操作。");
        return;
    }
    console.log(`--> [移动] 准备将文件 ${fileToken} 移动到文件夹 ${folderToken}...`);
    try {
        const response = await axios.post(
            `https://open.feishu.cn/open-apis/drive/v1/files/${fileToken}/move`,
            { type: fileType, folder_token: folderToken },
            { headers: { 'Authorization': `Bearer ${token}` } }
        );
        if (response.data.code === 0) {
            console.log(`--> [移动] 成功将文件移动到目标文件夹。`);
        } else {
            console.error(`--> [移动] 移动文件失败: ${response.data.msg}`, JSON.stringify(response.data, null, 2));
        }
    } catch (error) {
        const errorMessage = error.response ? JSON.stringify(error.response.data) : error.message;
        console.error(`--> [移动] 移动文件时发生严重网络或服务器错误: ${errorMessage}`);
    }
}

// --- 业务逻辑：导出功能 ---
async function getMappingSchemas() { return { schemas: DATA_SCHEMAS }; }

async function getSheetHeaders(payload) {
    const { spreadsheetToken } = payload;
    if (!spreadsheetToken) throw new AppError('Missing spreadsheetToken.', 400);
    const token = await getTenantAccessToken();
    const sheetsResponse = await axios.get(`https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/${spreadsheetToken}/sheets/query`, { headers: { 'Authorization': `Bearer ${token}` } });
    if (sheetsResponse.data.code !== 0) throw new AppError(`Failed to get sheets info: ${sheetsResponse.data.msg}`, 500);
    const firstSheetId = sheetsResponse.data.data.sheets[0].sheet_id;
    const urlEncodedRange = encodeURIComponent(`${firstSheetId}!A1:ZZ1`);
    const valuesResponse = await axios.get(`https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${spreadsheetToken}/values/${urlEncodedRange}?valueRenderOption=ToString`, { headers: { 'Authorization': `Bearer ${token}` } });
    if (valuesResponse.data.code !== 0) throw new AppError(`Failed to read sheet values: ${valuesResponse.data.msg}`, 500);
    return { headers: (valuesResponse.data.data.valueRange.values[0] || []).filter(h => h) };
}

async function generateAutomationSheet(payload) {
    const { primaryCollection, mappingTemplate, taskIds, destinationFolderToken, projectName } = payload;
    
    console.log("======== [START] generateAutomationSheet ========");
    console.log("收到的初始参数:", JSON.stringify(payload, null, 2));

    if (!primaryCollection || !mappingTemplate || !taskIds || !taskIds.length) {
        throw new AppError('Missing required parameters.', 400);
    }
    const token = await getTenantAccessToken();
    const db = (await getDbConnection()).db(DB_NAME);
    
    console.log("\n--- [步骤 1] 复制模板表格 ---");
    const templateToken = getSpreadsheetTokenFromUrl(mappingTemplate.spreadsheetToken);
    if (!templateToken) throw new AppError('无法从模板中解析出有效的Token。', 400);
    const newFileName = `${projectName || '未知项目'} - ${mappingTemplate.name}`.replace(/[\/\\:*?"<>|]/g, '');
    const copyPayload = { name: newFileName, type: 'sheet', folder_token: "" };
    console.log("--> 将在模板文件所在位置创建副本...");
    const copyResponse = await axios.post(`https://open.feishu.cn/open-apis/drive/v1/files/${templateToken}/copy`, copyPayload, { headers: { 'Authorization': `Bearer ${token}` } });
    if (copyResponse.data.code !== 0) { 
        console.error("--> [错误] 复制文件API返回失败:", JSON.stringify(copyResponse.data, null, 2));
        throw new AppError(`复制飞书表格失败: ${copyResponse.data.msg}`, 500); 
    }
    const newFile = copyResponse.data.data.file;
    const newSpreadsheetToken = newFile.token;
    console.log(`--> 成功! 新文件名: "${newFileName}", 新Token: ${newSpreadsheetToken}`);

    console.log("\n--- [步骤 2] 从数据库聚合数据 ---");
    const objectIdTaskIds = taskIds.map(id => new ObjectId(id));
    const tasks = await db.collection('automation-tasks').find({ _id: { $in: objectIdTaskIds } }, { projection: { 'metadata.collaborationId': 1, _id: 0 } }).toArray();
    const collaborationIds = [...new Set(tasks.map(t => t.metadata?.collaborationId).filter(Boolean))];
    let results = [];
    if (collaborationIds.length > 0) {
        let pipeline = [
            { $match: { id: { $in: collaborationIds } } },
            { $lookup: { from: 'talents', localField: 'talentId', foreignField: 'id', as: 'talent' } }, { $unwind: { path: '$talent', preserveNullAndEmptyArrays: true } },
            { $lookup: { from: 'projects', localField: 'projectId', foreignField: 'id', as: 'project' } }, { $unwind: { path: '$project', preserveNullAndEmptyArrays: true } },
            { $lookup: { from: 'automation-tasks', localField: 'id', foreignField: 'metadata.collaborationId', as: 'task' } }, { $unwind: { path: '$task', preserveNullAndEmptyArrays: true } }
        ];
        results = await db.collection('collaborations').aggregate(pipeline).toArray();
        results.forEach(doc => {
            if(doc.talent && Array.isArray(doc.talent.prices) && doc.talent.prices.length > 0) {
                doc.talent.latestPrice = doc.talent.prices[doc.talent.prices.length - 1].price;
            }
        });
    }
    console.log(`--> 成功! 数据聚合完成, 共找到 ${results.length} 条记录。`);
    
    console.log("\n--- [步骤 3] 写入数据行 ---");
    if (results.length > 0) {
        const dataToWrite = [], imageWriteQueue = [], START_ROW = 2;
        const contextData = results.map(doc => ({
            talents: doc.talent,
            projects: doc.project,
            'automation-tasks': doc.task,
            collaborations: doc
        }));

        for (let i = 0; i < contextData.length; i++) {
            const context = contextData[i];
            const rowData = [];
            for (let j = 0; j < mappingTemplate.feishuSheetHeaders.length; j++) {
                const feishuHeader = mappingTemplate.feishuSheetHeaders[j];
                const rule = mappingTemplate.mappingRules[feishuHeader];
                let finalValue;

                if (typeof rule === 'string') {
                    const pathParts = rule.split('.');
                    const collection = pathParts[0]; 
                    const trueContext = context[collection]; 
                    finalValue = pathParts.slice(1).reduce((obj, key) => (obj && obj[key] !== undefined) ? obj[key] : null, trueContext);
                } else if (typeof rule === 'object' && rule !== null && rule.formula) {
                    const rawResult = evaluateFormula(rule.formula, context);
                    if (rule.output && (rule.output === 'percentage' || rule.output.startsWith('number'))) {
                        const numericResult = parseFloat(rawResult);
                        finalValue = isNaN(numericResult) ? null : numericResult;
                    } else {
                        finalValue = rule.output ? formatOutput(rawResult, rule.output) : rawResult;
                    }
                }
                
                const isImageField = (typeof rule === 'string' && rule.includes('screenshots'));
                if (isImageField && typeof finalValue === 'string' && finalValue.startsWith('http')) {
                    rowData.push(null); // Use null for image cells to keep them blank
                    imageWriteQueue.push({ range: `${columnIndexToLetter(j)}${START_ROW + i}`, url: finalValue, name: `${feishuHeader}.png` });
                } else {
                    rowData.push(finalValue);
                }
            }
            dataToWrite.push(rowData);
        }
        
        const metaInfoResponse = await axios.get(`https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${newSpreadsheetToken}/metainfo`, { headers: { 'Authorization': `Bearer ${token}` } });
        const firstSheetId = metaInfoResponse.data.data.sheets[0].sheetId;

        if (dataToWrite.length > 0) {
            const textRange = `${firstSheetId}!A${START_ROW}:${columnIndexToLetter(dataToWrite[0].length - 1)}${START_ROW + dataToWrite.length - 1}`;
            await axios.put( `https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${newSpreadsheetToken}/values`, { valueRange: { range: textRange, values: dataToWrite } }, { headers: { 'Authorization': `Bearer ${token}` } } );
        }
        for (const imageJob of imageWriteQueue) {
            const imageRange = `${firstSheetId}!${imageJob.range}:${imageJob.range}`;
            await writeImageToCell(token, newSpreadsheetToken, imageRange, imageJob.url, imageJob.name);
        }
    }
    
    console.log("\n--- [步骤 4] 移动文件到指定文件夹 ---");
    const parsedFolderToken = getSpreadsheetTokenFromUrl(destinationFolderToken);
    await moveFileToFolder(newSpreadsheetToken, 'sheet', parsedFolderToken, token);

    console.log("\n--- [步骤 5] 处理文件权限 ---");
    const ownerTransferred = await transferOwner(newSpreadsheetToken, token);
    if (!ownerTransferred) { await grantEditPermissions(newSpreadsheetToken, token); }
    
    console.log("\n======== [END] generateAutomationSheet ========");
    return { 
        message: "飞书表格已生成并成功处理！", 
        sheetUrl: newFile.url,
        fileName: newFileName,
        sheetToken: newSpreadsheetToken
    };
}

// --- [功能补齐] 业务逻辑：导入功能 ---

// [源自 v8.2]
function parseFlexibleNumber(value, isPercentage = false) {
    if (value === null || value === undefined) return 0;
    if (typeof value === 'number') {
        if (isPercentage && value > 1) return value / 100;
        return value;
    }
    if (typeof value !== 'string') return 0;
    let numStr = value.replace(/,/g, '').trim();
    let num = 0;
    if (isPercentage && numStr.includes('%')) {
        numStr = numStr.replace(/%/g, '');
        num = parseFloat(numStr) / 100;
    } else {
        if (numStr.toLowerCase().includes('w') || numStr.includes('万')) {
            numStr = numStr.replace(/w|万/gi, '');
            num = parseFloat(numStr) * 10000;
        } else {
            num = parseFloat(numStr);
            if (isPercentage && num > 1) {
                num = num / 100;
            }
        }
    }
    return isNaN(num) ? 0 : num;
}

// [源自 v8.2]
async function handleTalentImport(spreadsheetToken) {
    console.log(`[导入] 开始从表格 ${spreadsheetToken} 导入达人数据...`);
    const token = await getTenantAccessToken();
    const rows = await readFeishuSheet(spreadsheetToken, token);
    if (!rows || rows.length < 2) return { data: [] };
    const header = rows[0];
    const dataRows = rows.slice(1);
    const processedData = [];
    const headerMap = new Map(header.map((col, i) => [col, i]));
    for (const row of dataRows) {
        const getValue = (colName, isPercentage = false) => {
            const index = headerMap.get(colName);
            return (index !== undefined && row[index] !== null && row[index] !== '') ? parseFlexibleNumber(row[index], isPercentage) : 0;
        };
        const xingtuIdIndex = headerMap.get('达人id');
        const xingtuId = (xingtuIdIndex !== undefined && row[xingtuIdIndex]) ? String(row[xingtuIdIndex]).trim() : null;
        if (!xingtuId) continue;
        const talentData = { xingtuId, performanceData: {} };
        talentData.performanceData.cpm60s = getValue('预期cpm');
        talentData.performanceData.maleAudienceRatio = getValue('男性粉丝占比', true);
        talentData.performanceData.femaleAudienceRatio = getValue('女性粉丝占比', true);
        const ratio18_40 = getValue('18-23岁粉丝比例', true) + getValue('24-30岁粉丝比例', true) + getValue('31-40岁粉丝比例', true);
        const ratio40_plus = getValue('41-50岁粉丝比例', true) + getValue('50岁以上粉丝比例', true);
        if (ratio18_40 > 0) talentData.performanceData.audience_18_40_ratio = ratio18_40;
        if (ratio40_plus > 0) talentData.performanceData.audience_40_plus_ratio = ratio40_plus;
        processedData.push(talentData);
    }
    console.log(`[导入] 成功处理 ${processedData.length} 条达人记录。`);
    return { data: processedData, message: `Successfully read ${processedData.length} records from Feishu Sheet.` };
}

// [源自 v8.7]
async function performProjectSync(spreadsheetToken, dataType) {
    console.log(`[导入] 开始从表格 ${spreadsheetToken} 同步项目数据 (类型: ${dataType})...`);
    const client = await getDbConnection();
    const db = client.db(DB_NAME);
    const token = await getTenantAccessToken();
    const rows = await readFeishuSheet(spreadsheetToken, token);
    if (!rows || rows.length < 2) return { processedRows: 0, created: 0, updated: 0 };
    const header = rows[0];
    const dataRows = rows.slice(1);
    const collaborationsCollection = db.collection('collaborations');
    const worksCollection = db.collection('works');
    let createdCount = 0;
    let updatedCount = 0;
    const DATA_MAPPING = {
        '星图任务ID': { dbField: 'collaborationId', type: 'lookup' }, '视频ID': { dbField: 'platformWorkId', type: 'string' }, '视频实际发布时间': { dbField: 'publishedAt', type: 'date' },
        '数据最后更新时间': { dbField: 'statsUpdatedAt', type: 'date' }, '播放量': { dbField: 'totalViews', type: 'number' }, '点赞量': { dbField: 'likeCount', type: 'number' },
        '评论量': { dbField: 'commentCount', type: 'number' }, '分享量': { dbField: 'shareCount', type: 'number' }, '组件曝光量': { dbField: 'componentImpressionCount', type: 'number' },
        '组件点击量': { dbField: 'componentClickCount', type: 'number' }, '视频完播率': { dbField: 'completionRate', type: 'percentage' }, '分频次触达人数-1次': { dbField: 'reachByFrequency.freq1', type: 'number' },
        '分频次触达人数-2次': { dbField: 'reachByFrequency.freq2', type: 'number' }, '分频次触达人数-3次': { dbField: 'reachByFrequency.freq3', type: 'number' }, '分频次触达人数-4次': { dbField: 'reachByFrequency.freq4', type: 'number' },
        '分频次触达人数-5次': { dbField: 'reachByFrequency.freq5', type: 'number' }, '分频次触达人数-6次': { dbField: 'reachByFrequency.freq6', type: 'number' }, '分频次触达人数-7次及以上': { dbField: 'reachByFrequency.freq7plus', type: 'number' },
    };
    const starQuestIdColumnName = '星图任务ID';
    const starQuestIdIndex = header.indexOf(starQuestIdColumnName);
    if (starQuestIdIndex === -1) throw new AppError(`"${starQuestIdColumnName}" column not found in the sheet header.`, 400);
    for (const row of dataRows) {
        const starQuestId = row[starQuestIdIndex];
        if (!starQuestId) continue;
        const starQuestIdStr = String(starQuestId).trim();
        const collaboration = await collaborationsCollection.findOne({ "taskId": starQuestIdStr });
        if (collaboration) {
            const updatePayload = {};
            const prefix = dataType;
            header.forEach((colName, index) => {
                const mapping = DATA_MAPPING[colName];
                if (!mapping || colName === starQuestIdColumnName) return;
                let value = row[index];
                if (value === null || value === undefined || String(value).trim() === '') return;
                try {
                    if (mapping.type === 'number') value = parseFloat(value) || 0;
                    else if (mapping.type === 'date') value = new Date(value);
                    else if (mapping.type === 'percentage') value = parseFlexibleNumber(value, true);
                } catch (e) { console.warn(`Could not convert value for ${colName}: ${value}. Skipping field.`); return; }
                if (mapping.dbField.includes('.')) {
                    const [parent, child] = mapping.dbField.split('.');
                    const prefixedParent = `${prefix}_${parent}`;
                    if (!updatePayload[prefixedParent]) updatePayload[prefixedParent] = {};
                    updatePayload[prefixedParent][child] = value;
                } else {
                    updatePayload[`${prefix}_${mapping.dbField}`] = value;
                }
            });
            const existingWork = await worksCollection.findOne({ collaborationId: collaboration.id });
            if (Object.keys(updatePayload).length > 0) {
                if (existingWork) {
                    await worksCollection.updateOne({ _id: existingWork._id }, { $set: { ...updatePayload, updatedAt: new Date() } });
                    updatedCount++;
                } else {
                    const newWorkDoc = { ...updatePayload, id: `work_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`, collaborationId: collaboration.id, projectId: collaboration.projectId, talentId: collaboration.talentId, sourceType: 'COLLABORATION', createdAt: new Date(), updatedAt: new Date(), };
                    await worksCollection.insertOne(newWorkDoc);
                    createdCount++;
                }
            }
        }
    }
    console.log(`[导入] 项目同步完成。处理行数: ${dataRows.length}, 新建: ${createdCount}, 更新: ${updatedCount}`);
    return { processedRows: dataRows.length, created: createdCount, updated: updatedCount };
}


// --- 总调度函数 ---
async function handleFeishuRequest(requestBody) {
    const { dataType, payload, ...legacyParams } = requestBody;
    if (!dataType) throw new AppError('Missing required parameter: dataType.', 400);

    const extractToken = (data) => {
        if (!data) return null;
        const tokenSource = data.spreadsheetToken || data.feishuUrl;
        return getSpreadsheetTokenFromUrl(tokenSource);
    };
    
    switch (dataType) {
        case 'getMappingSchemas':
            return await getMappingSchemas();
        case 'getSheetHeaders':
            return await getSheetHeaders(payload);
        case 'generateAutomationReport':
            return await generateAutomationSheet(payload);
        case 'talentPerformance':
        case 't7':
        case 't21': {
            const token = extractToken({ ...legacyParams, ...payload });
            if (!token) throw new AppError(`Missing spreadsheetToken or a valid feishuUrl for ${dataType}.`, 400);
            if (dataType === 'talentPerformance') return await handleTalentImport(token);
            return await performProjectSync(token, dataType);
        }
        default:
            throw new AppError(`Invalid dataType "${dataType}".`, 400);
    }
}

module.exports = { handleFeishuRequest };

