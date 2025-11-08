/**
 * @file utils.js
 * @version 11.4.3 - Percentage Field Fix
 * @description
 * - [BUG 修复] 修复百分比字段值被错误除以100的问题（只有包含%符号时才除以100）
 * - [兼容性] 保留 V11.4.2 的统计逻辑修复和多价格类型支持
 * - [兼容性] 保留 V11.4.1 的字段映射修复和lastUpdated功能
 * - [兼容性] 保留 V11.4 的数据库批量更新功能
 * - [兼容性] 保留 V11.3 的 Null Safety Fix 和 parseFlexibleNumber 函数
 * - [兼容性] 保留 V11.2 的 Sheet Generation Fix
 * - [兼容性] 保留 V11.1 的 manualDailyUpdate 日志优化
 * - [兼容性] 保留 V11.0 的 manualDailyUpdate 核心功能
 */
const axios = require('axios');
const { MongoClient, ObjectId } = require('mongodb');

// --- 安全配置 ---
const APP_ID = process.env.FEISHU_APP_ID;
const APP_SECRET = process.env.FEISHU_APP_SECRET;
const DB_NAME = 'kol_data';
const MONGO_URI = process.env.MONGO_URI;
const FEISHU_OWNER_ID = process.env.FEISHU_OWNER_ID;
const FEISHU_SHARE_USER_IDS = process.env.FEISHU_SHARE_USER_IDS;

// --- 数据库集合名称常量 ---
const COLLABORATIONS_COLLECTION = 'collaborations';
const WORKS_COLLECTION = 'works';
const PROJECTS_COLLECTION = 'projects';
const TALENTS_COLLECTION = 'talents';
const MAPPING_TEMPLATES_COLLECTION = 'mapping_templates';
const AUTOMATION_TASKS_COLLECTION = 'automation-tasks';

// --- 模块级缓存 ---
let tenantAccessToken = null;
let tokenExpiresAt = 0;
let dbClient = null;

// --- 数据结构定义 ---
const DATA_SCHEMAS = {
    talents: { displayName: "达人信息", fields: [ { path: "nickname", displayName: "达人昵称" }, { path: "xingtuId", displayName: "星图ID" }, { path: "uid", displayName: "UID" }, { path: "latestPrice", displayName: "最新价格", isSpecial: true }, ] },
    projects: { displayName: "项目信息", fields: [ { path: "name", displayName: "项目名称" }, { path: "qianchuanId", displayName: "仟传项目编号" }, ] },
    collaborations: { displayName: "合作信息", fields: [ { path: "taskId", displayName: "任务ID (星图)" }, { path: "videoId", displayName: "视频ID (平台)" }, { path: "orderType", displayName: "订单类型" }, { path: "status", displayName: "合作状态" }, { path: "amount", displayName: "合作金额" }, { path: "publishDate", displayName: "实际发布日期" }, ] },
    "automation-tasks": { displayName: "自动化任务", fields: [ { path: "result.data.预期CPM", displayName: "预期CPM" }, { path: "result.data.完播率", displayName: "完播率" }, { path: "result.data.爆文率", displayName: "爆文率" }, { path: "result.data.个人视频播放量均值", displayName: "个人视频播放量均值" }, { path: "result.data.星图频播放量均值", displayName: "星图视频播放量均值" }, { path: "result.data.用户画像总结", displayName: "用户画像总结" }, { path: "result.screenshots.0.url", displayName: "截图1 (达人价格)", isImage: true }, { path: "result.screenshots.1.url", displayName: "截图2 (星图视频)", isImage: true }, { path: "result.screenshots.2.url", displayName: "截图3 (男女比例)", isImage: true }, { path: "result.screenshots.4.url", displayName: "截图4 (年龄分布)", isImage: true }, { path: "result.screenshots.5.url", displayName: "截图5 (城市等级)", isImage: true }, { path: "result.screenshots.6.url", displayName: "截图6 (八大人群)", isImage: true }, { path: "result.screenshots.7.url", displayName: "截图7 (设备截图)", isImage: true }, ] }
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
        const driveTypeIndex = pathParts.findIndex(part => ['sheets', 'folder', 'spreadsheet'].includes(part));
        if (driveTypeIndex > -1 && pathParts.length > driveTypeIndex + 1) {
            return pathParts[driveTypeIndex + 1];
        }
    } catch (error) {
        console.warn(`Could not parse URL: ${url}`, error);
    }
    console.warn(`Could not extract token from URL: ${url}. Returning the input string.`);
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

/**
 * [V11.3 新增] 解析灵活的数字格式（支持百分比、千位分隔符、万单位等）
 */
function parseFlexibleNumber(value, isPercentage = false) {
    if (value === null || value === undefined) return 0;
    if (typeof value === 'number') return value;
    if (typeof value !== 'string') return 0;
    
    let numStr = value.replace(/,/g, '').trim();
    
    if (isPercentage || numStr.endsWith('%')) {
        const num = parseFloat(numStr.replace('%', ''));
        // Only divide by 100 if there's an actual % symbol in the original string
        return isNaN(num) ? 0 : (numStr.endsWith('%') ? num / 100 : num);
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

    console.log(`--> [飞书读取] 目标表格: ${spreadsheetToken}, 范围: ${decodeURIComponent(urlEncodedRange)}`);
    const valuesResponse = await axios.get(
        `https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${spreadsheetToken}/values/${urlEncodedRange}?valueRenderOption=ToString`,
        { headers: { 'Authorization': `Bearer ${token}` } }
    );
    if (valuesResponse.data.code !== 0) throw new AppError(`Failed to read sheet values: ${valuesResponse.data.msg}`, 500);
    console.log(`--> [飞书读取] 成功读取 ${valuesResponse.data.data.valueRange.values?.length || 0} 行数据。`);
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
    const headers = await readFeishuSheet(getSpreadsheetTokenFromUrl(spreadsheetToken), token, 'A1:ZZ1');
    return { headers: (headers[0] || []).filter(h => h) };
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
    const tasks = await db.collection(AUTOMATION_TASKS_COLLECTION).find({ _id: { $in: objectIdTaskIds } }, { projection: { 'metadata.collaborationId': 1, _id: 1 } }).toArray();
    const collaborationIds = [...new Set(tasks.map(t => t.metadata?.collaborationId).filter(Boolean))];
    let results = [];
    if (collaborationIds.length > 0) {
        let pipeline = [
            { $match: { id: { $in: collaborationIds } } },
            { $lookup: { from: TALENTS_COLLECTION, localField: 'talentId', foreignField: 'id', as: 'talent' } }, { $unwind: { path: '$talent', preserveNullAndEmptyArrays: true } },
            { $lookup: { from: PROJECTS_COLLECTION, localField: 'projectId', foreignField: 'id', as: 'project' } }, { $unwind: { path: '$project', preserveNullAndEmptyArrays: true } },
            
             {
               $lookup: {
                 from: AUTOMATION_TASKS_COLLECTION,
                 let: { collabId: "$id" },
                 pipeline: [
                   { $match:
                      { $expr:
                         { $and:
                            [
                              { $eq: ["$metadata.collaborationId", "$$collabId"] },
                              { $in: ["$_id", objectIdTaskIds] }
                            ]
                         }
                      }
                   },
                   { $limit: 1 }
                 ],
                 as: "task"
               }
            },
            { $unwind: { path: '$task', preserveNullAndEmptyArrays: true } }
        ];
        results = await db.collection(COLLABORATIONS_COLLECTION).aggregate(pipeline).toArray();
        results.forEach(doc => {
            if(doc.talent && Array.isArray(doc.talent.prices) && doc.talent.prices.length > 0) {
                const sortedPrices = [...doc.talent.prices].sort((a, b) => (b.year - a.year) || (b.month - a.month));
                const latestPriceEntry = sortedPrices.find(p => p.status === 'confirmed') || sortedPrices[0];
                if (latestPriceEntry) {
                    doc.talent.latestPrice = latestPriceEntry.price;
                }
            }
        });
    }
    console.log(`--> 成功! 数据聚合完成, 共找到 ${results.length} 条有效记录。`);

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
                let finalValue = null;

                if (typeof rule === 'string') {
                    const pathParts = rule.split('.');
                    if (pathParts.length > 1) {
                        const collection = pathParts[0];
                        const trueContext = context[collection];
                        finalValue = pathParts.slice(1).reduce((obj, key) => (obj && obj[key] !== undefined) ? obj[key] : null, trueContext);
                    }
                } else if (typeof rule === 'object' && rule !== null && rule.formula) {
                    const rawResult = evaluateFormula(rule.formula, context);
                    finalValue = rule.output ? formatOutput(rawResult, rule.output) : rawResult;
                }
                
                const isImageField = (typeof rule === 'string' && rule.includes('screenshots'));
                if (isImageField && typeof finalValue === 'string' && finalValue.startsWith('http')) {
                    rowData.push(null);
                    imageWriteQueue.push({ range: `${columnIndexToLetter(j)}${START_ROW + i}`, url: finalValue, name: `${feishuHeader}.png` });
                } else {
                    rowData.push(finalValue === null || finalValue === undefined ? null : finalValue);
                }
            }
            dataToWrite.push(rowData);
        }

        const metaInfoResponse = await axios.get(`https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${newSpreadsheetToken}/metainfo`, { headers: { 'Authorization': `Bearer ${token}` } });
        const firstSheetId = metaInfoResponse.data.data.sheets[0].sheetId;

        if (dataToWrite.length > 0) {
            const textRange = `${firstSheetId}!A${START_ROW}:${columnIndexToLetter(dataToWrite[0].length - 1)}${START_ROW + dataToWrite.length - 1}`;
            console.log(`--> [写入文本] 目标范围: ${textRange}, 行数: ${dataToWrite.length}`);
            try {
                await axios.put(
                    `https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${newSpreadsheetToken}/values`,
                    { valueRange: { range: textRange, values: dataToWrite } },
                    { headers: { 'Authorization': `Bearer ${token}` } }
                );
                console.log(`--> [写入文本] 成功写入 ${dataToWrite.length} 行数据。`);
            } catch(writeError) {
                 console.error(`--> [写入文本] 写入失败: ${writeError.response?.data?.msg || writeError.message}`);
                 throw new AppError(`写入飞书表格失败: ${writeError.response?.data?.msg || writeError.message}`, 500);
            }
        }

        if (imageWriteQueue.length > 0) {
             console.log(`--> [写入图片] 准备写入 ${imageWriteQueue.length} 张图片...`);
            for (const imageJob of imageWriteQueue) {
                const imageRange = `${firstSheetId}!${imageJob.range}:${imageJob.range}`;
                await writeImageToCell(token, newSpreadsheetToken, imageRange, imageJob.url, imageJob.name);
            }
             console.log(`--> [写入图片] 图片写入完成。`);
        }
    }
    
    console.log("\n--- [步骤 4] 移动文件到指定文件夹 ---");
    const parsedFolderToken = getSpreadsheetTokenFromUrl(destinationFolderToken);
    await moveFileToFolder(newSpreadsheetToken, 'sheet', parsedFolderToken, token);

    console.log("\n--- [步骤 5] 处理文件权限 ---");
    const ownerTransferred = await transferOwner(newSpreadsheetToken, token);
    if (!ownerTransferred) {
        await grantEditPermissions(newSpreadsheetToken, token);
    }
    
    console.log("\n======== [END] generateAutomationSheet ========");
    return { 
        message: "飞书表格已生成并成功处理！", 
        sheetUrl: newFile.url,
        fileName: newFileName,
        sheetToken: newSpreadsheetToken
    };
}


// --- 业务逻辑：导入功能 ---

/**
 * [V11.4.2 新增] 处理达人数据导入 - 支持多价格类型
 */
async function handleTalentImport(spreadsheetToken) {
    console.log(`[导入] 开始从表格 ${spreadsheetToken} 导入达人数据...`);
    const token = await getTenantAccessToken();
    const rows = await readFeishuSheet(spreadsheetToken, token);
    if (!rows || rows.length < 2) return { data: [], updated: 0, failed: 0 };
    
    const header = rows[0];
    const dataRows = rows.slice(1);
    const processedData = [];

    // 获取数据库连接
    const client = await getDbConnection();
    const db = client.db(DB_NAME);
    const talentsCollection = db.collection(TALENTS_COLLECTION);

    // 统计计数器
    let stats = {
        totalRows: dataRows.length,
        processed: 0,
        skipped: 0,
        updated: 0,
        failed: 0,
        priceUpdatedTalents: 0,  // [V11.4.2 新增] 价格更新的达人数
        priceRecordsAdded: 0,     // [V11.4.2 新增] 添加的价格记录数
        skippedReasons: {
            missingId: 0,
            emptyRow: 0
        }
    };

    console.log(`[导入] 表头列数: ${header.length}`);
    console.log(`[导入] 表头列名: ${header.filter(h => h).join(', ')}`);

    const headerMap = new Map(
        header
            .map((col, i) => {
                const colName = (col && typeof col === 'string') ? col.trim() : '';
                return [colName, i];
            })
            .filter(([colName]) => colName !== '')
    );

    const xingtuIdCol1 = headerMap.get('达人id');
    const xingtuIdCol2 = headerMap.get('星图ID');
    console.log(`[导入] "达人id"列索引: ${xingtuIdCol1}, "星图ID"列索引: ${xingtuIdCol2}`);
    
    if (xingtuIdCol1 === undefined && xingtuIdCol2 === undefined) {
        throw new AppError('飞书表格中找不到"达人id"或"星图ID"列，请检查表头。', 400);
    }

    console.log('[调试] 前3行数据示例:');
    for (let i = 0; i < Math.min(3, dataRows.length); i++) {
        const row = dataRows[i];
        const idIndex = xingtuIdCol1 ?? xingtuIdCol2;
        const idValue = row[idIndex];
        console.log(`  行${i+1}: 达人id="${idValue}" (类型: ${typeof idValue}, 长度: ${idValue ? String(idValue).length : 0})`);
    }

    // [V11.4.2 新增] 获取当前年月用于价格记录
    const currentYear = new Date().getFullYear();
    const currentMonth = new Date().getMonth() + 1;

    // 收集所有需要更新的数据
    for (const row of dataRows) {
        const getValue = (colName, isPercentage = false) => {
            const index = headerMap.get(colName);
            return (index !== undefined && row[index] !== null && row[index] !== '') ? parseFlexibleNumber(row[index], isPercentage) : 0;
        };

        const xingtuIdIndex = headerMap.get('达人id') ?? headerMap.get('星图ID');
        const xingtuId = (xingtuIdIndex !== undefined && row[xingtuIdIndex]) ? String(row[xingtuIdIndex]).trim() : null;

        if (!xingtuId) {
            stats.skipped++;
            stats.skippedReasons.missingId++;
            continue;
        }

        const talentData = { xingtuId, performanceData: {} };

        // [V11.4.1 修复] 完整的字段映射（飞书列名 -> 数据库字段名）
        const mappings = [
            { key: 'cpm60s', header: '预期cpm' },
            { key: 'maleAudienceRatio', header: '男性粉丝占比', isPercentage: true },
            { key: 'femaleAudienceRatio', header: '女性粉丝占比', isPercentage: true },
            { key: 'ratio_18_23', header: '18-23岁粉丝比例', isPercentage: true },
            { key: 'ratio_24_30', header: '24-30岁粉丝比例', isPercentage: true },
            { key: 'ratio_31_40', header: '31-40岁粉丝比例', isPercentage: true },
            { key: 'ratio_41_50', header: '41-50岁粉丝比例', isPercentage: true },
            { key: 'ratio_50_plus', header: '50岁以上粉丝比例', isPercentage: true },
            // [V11.4.1 修复] 8个人群包字段 - 添加"粉丝比例"后缀
            { key: 'ratio_town_middle_aged', header: '小镇中老年粉丝比例', isPercentage: true },
            { key: 'ratio_senior_middle_class', header: '资深中产粉丝比例', isPercentage: true },
            { key: 'ratio_z_era', header: 'Z时代粉丝比例', isPercentage: true },
            { key: 'ratio_urban_silver', header: '都市银发粉丝比例', isPercentage: true },
            { key: 'ratio_town_youth', header: '小镇青年粉丝比例', isPercentage: true },
            { key: 'ratio_exquisite_mom', header: '精致妈妈粉丝比例', isPercentage: true },
            { key: 'ratio_new_white_collar', header: '新锐白领粉丝比例', isPercentage: true },
            { key: 'ratio_urban_blue_collar', header: '都市蓝领粉丝比例', isPercentage: true },
        ];

        mappings.forEach(m => {
             const value = getValue(m.header, m.isPercentage);
             if (value !== 0 || (headerMap.has(m.header) && row[headerMap.get(m.header)] !== null)) {
                 talentData.performanceData[m.key] = value;
             }
        });

        const ratio18_40 = (talentData.performanceData.ratio_18_23 || 0) + (talentData.performanceData.ratio_24_30 || 0) + (talentData.performanceData.ratio_31_40 || 0);
        const ratio40_plus = (talentData.performanceData.ratio_41_50 || 0) + (talentData.performanceData.ratio_50_plus || 0);

        if (ratio18_40 > 0) talentData.performanceData.audience_18_40_ratio = ratio18_40;
        if (ratio40_plus > 0) talentData.performanceData.audience_40_plus_ratio = ratio40_plus;

        // [V11.4.2 新增] 解析3个价格字段
        const priceFields = [
            { header: '抖音60+s短视频报价', type: '60s_plus' },
            { header: '抖音20-60s短视频报价', type: '20_to_60s' },
            { header: '抖音1-20s短视频报价', type: '1_to_20s' }
        ];

        const newPrices = [];
        for (const field of priceFields) {
            const priceValue = getValue(field.header, false);
            if (priceValue > 0) {
                newPrices.push({
                    year: currentYear,
                    month: currentMonth,
                    type: field.type,
                    price: priceValue,
                    status: 'confirmed'
                });
            }
        }

        talentData.newPrices = newPrices;  // 临时存储新价格

        processedData.push(talentData);
        stats.processed++;
    }
    
    // [V11.4.2 修复] 使用点表示法批量更新数据库，添加 lastUpdated 时间戳 + 价格更新
    console.log(`[导入] 开始批量更新数据库，共 ${processedData.length} 条记录...`);
    
    if (processedData.length > 0) {
        const bulkOps = [];
        const currentTime = new Date();
        
        for (const talent of processedData) {
            const updateFields = {};
            
            // [V11.4.1] 使用点表示法更新 performanceData 的各个字段
            for (const [key, value] of Object.entries(talent.performanceData)) {
                updateFields[`performanceData.${key}`] = value;
            }
            
            // [V11.4.1] 添加 lastUpdated 时间戳到 performanceData
            updateFields['performanceData.lastUpdated'] = currentTime;
            updateFields['updatedAt'] = currentTime;
            
            bulkOps.push({
                updateOne: {
                    filter: { xingtuId: talent.xingtuId },
                    update: { $set: updateFields },
                    upsert: false
                }
            });
            
            // [V11.4.2 新增] 如果有价格需要更新
            if (talent.newPrices && talent.newPrices.length > 0) {
                // 先删除当前年月的所有3种类型的价格记录
                bulkOps.push({
                    updateOne: {
                        filter: { xingtuId: talent.xingtuId },
                        update: {
                            $pull: {
                                prices: {
                                    year: currentYear,
                                    month: currentMonth,
                                    type: { $in: ['60s_plus', '20_to_60s', '1_to_20s'] }
                                }
                            }
                        },
                        upsert: false
                    }
                });
                
                // 再添加新价格
                bulkOps.push({
                    updateOne: {
                        filter: { xingtuId: talent.xingtuId },
                        update: {
                            $push: {
                                prices: { $each: talent.newPrices }
                            }
                        },
                        upsert: false
                    }
                });
                
                stats.priceUpdatedTalents++;
                stats.priceRecordsAdded += talent.newPrices.length;
            }
        }
        
        try {
            const bulkResult = await talentsCollection.bulkWrite(bulkOps, { ordered: false });
            
            // [V11.4.2 修复] 简化统计逻辑 - 方案A
            // 如果 bulkWrite 执行成功，则认为所有达人都成功更新了
            stats.updated = processedData.length;
            stats.failed = 0;
            
            console.log(`[导入] 数据库批量更新完成：`);
            console.log(`  - 总操作数: ${bulkOps.length}`);
            console.log(`  - Matched: ${bulkResult.matchedCount}`);
            console.log(`  - Modified: ${bulkResult.modifiedCount}`);
        } catch (error) {
            console.error(`[导入] 数据库批量更新失败:`, error);
            // 如果批量更新失败，所有达人都标记为失败
            stats.failed = processedData.length;
            stats.updated = 0;
            throw new AppError(`批量更新数据库失败: ${error.message}`, 500);
        }
    }
    
    console.log(`[导入] ========== 导入统计 ==========`);
    console.log(`[导入] 总行数: ${stats.totalRows}`);
    console.log(`[导入] 解析成功: ${stats.processed} 条`);
    console.log(`[导入] 成功更新: ${stats.updated} 条达人`);
    console.log(`[导入] 更新失败: ${stats.failed} 条达人`);
    console.log(`[导入] 跳过: ${stats.skipped} 条`);
    console.log(`[导入]   - 缺少达人id: ${stats.skippedReasons.missingId} 条`);
    // [V11.4.2] 价格更新统计
    if (stats.priceUpdatedTalents > 0) {
        console.log(`[导入] 价格更新: ${stats.priceUpdatedTalents} 位达人，共 ${stats.priceRecordsAdded} 条价格`);
    }
    console.log(`[导入] ==================================`);
    
    return { 
        data: processedData, 
        updated: stats.updated,
        failed: stats.failed,
        priceUpdated: stats.priceUpdatedTalents,
        message: `成功更新 ${stats.updated} 条达人记录${stats.failed > 0 ? `，失败 ${stats.failed} 条` : ''}。${stats.priceUpdatedTalents > 0 ? `价格更新 ${stats.priceUpdatedTalents} 位达人。` : ''}` 
    };
}

async function performProjectSync(spreadsheetToken, dataType) {
    console.log(`[导入] 开始从表格 ${spreadsheetToken} 同步项目数据 (类型: ${dataType})...`);
    const client = await getDbConnection();
    const db = client.db(DB_NAME);
    const token = await getTenantAccessToken();
    const rows = await readFeishuSheet(spreadsheetToken, token);
    if (!rows || rows.length < 2) return { processedRows: 0, created: 0, updated: 0 };
    const header = rows[0];
    const dataRows = rows.slice(1);
    const collaborationsCollection = db.collection(COLLABORATIONS_COLLECTION);
    const worksCollection = db.collection(WORKS_COLLECTION);
    const projectsCollection = db.collection(PROJECTS_COLLECTION);

    let createdCount = 0;
    let updatedCount = 0;
    let skippedCount = 0;

    if (dataType === 'manualDailyUpdate') {
        const COL_TASK_ID = '星图任务ID';
        const COL_TIMESTAMP = '数据最后更新时间';
        const COL_VIEWS = '播放量';

        const taskIdIndex = header.indexOf(COL_TASK_ID);
        const timestampIndex = header.indexOf(COL_TIMESTAMP);
        const viewsIndex = header.indexOf(COL_VIEWS);

        if (taskIdIndex === -1 || timestampIndex === -1 || viewsIndex === -1) {
            throw new AppError(`飞书表格缺少必要的列: ${COL_TASK_ID}, ${COL_TIMESTAMP}, 或 ${COL_VIEWS}`, 400);
        }

        const bulkOps = [];
        const collabProjectMap = new Map();

        for (const row of dataRows) {
            const taskId = row[taskIdIndex] ? String(row[taskIdIndex]).trim() : null;
            const timestampStr = row[timestampIndex];
            const viewsStr = row[viewsIndex];

            if (!taskId) {
                skippedCount++;
                continue;
            }

            if (!timestampStr || viewsStr === null || viewsStr === undefined) {
                console.warn(`[导入 manualDailyUpdate] 跳过行，缺少 timestamp 或 views (TaskID: ${taskId})`);
                skippedCount++;
                continue;
            }

            let dateStr;
            try {
                dateStr = new Date(timestampStr).toISOString().split('T')[0];
                if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) throw new Error("Invalid Date Format");
            } catch (e) {
                console.warn(`[导入 manualDailyUpdate] 跳过行，无法提取日期 (TaskID: ${taskId})`);
                skippedCount++;
                continue;
            }

            const totalViews = parseInt(String(viewsStr).replace(/,/g, ''), 10);
            if (isNaN(totalViews)) {
                console.warn(`[导入 manualDailyUpdate] 跳过行，无法解析播放量 (TaskID: ${taskId})`);
                skippedCount++;
                continue;
            }

            let collab = collabProjectMap.get(taskId);
            if (!collab) {
                collab = await collaborationsCollection.findOne({ taskId: taskId });
                if (collab) {
                     const project = await projectsCollection.findOne({ id: collab.projectId });
                     collab.projectDiscount = project ? (parseFloat(project.discount) || 1.0) : 1.0;
                     collabProjectMap.set(taskId, collab);
                }
            }

            if (!collab) {
                console.warn(`[导入 manualDailyUpdate] 未找到合作记录 (TaskID: ${taskId})`);
                skippedCount++;
                continue;
            }

            const amount = parseFloat(collab.amount) || 0;
            const income = amount * collab.projectDiscount * 1.05;
            const cpm = income > 0 && totalViews > 0 ? (income / totalViews) * 1000 : 0;

             const pullOp = {
                updateOne: {
                    filter: { collaborationId: collab.id },
                    update: { $pull: { dailyStats: { date: dateStr } } }
                }
            };
            const pushOp = {
                updateOne: {
                    filter: { collaborationId: collab.id },
                    update: {
                        $push: {
                            dailyStats: {
                                $each: [{ date: dateStr, totalViews: totalViews, cpm: cpm, cpmChange: null, solution: '' }],
                                $sort: { date: 1 }
                            }
                        },
                         $setOnInsert: {
                            id: `work_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
                            projectId: collab.projectId,
                            talentId: collab.talentId,
                            sourceType: 'COLLABORATION',
                            createdAt: new Date()
                        },
                        $set: { updatedAt: new Date() }
                    },
                    upsert: true
                }
            };
            bulkOps.push(pullOp, pushOp);
        }

        if (bulkOps.length > 0) {
            const bulkResult = await worksCollection.bulkWrite(bulkOps, { ordered: false });
            updatedCount = bulkOps.length / 2; 
            console.log(`[导入 manualDailyUpdate] BulkWrite 完成. Matched: ${bulkResult.matchedCount}, Modified: ${bulkResult.modifiedCount}, Upserted: ${bulkResult.upsertedCount}`);

            const collabIdsToUpdate = [...new Set(bulkOps.filter(op => op.updateOne.filter.collaborationId).map(op => op.updateOne.filter.collaborationId))];
            const updatedWorks = await worksCollection.find({ collaborationId: { $in: collabIdsToUpdate } }).toArray();
            const cpmChangeBulkOps = [];
            for (const work of updatedWorks) {
                if (!work.dailyStats || work.dailyStats.length < 2) continue;
                for (let i = 1; i < work.dailyStats.length; i++) {
                    const currentStat = work.dailyStats[i];
                    const prevStat = work.dailyStats[i-1];
                    const cpmChange = (prevStat.cpm !== null && currentStat.cpm !== null) ? currentStat.cpm - prevStat.cpm : null;
                    if (currentStat.cpmChange !== cpmChange) {
                        cpmChangeBulkOps.push({
                            updateOne: {
                                filter: { _id: work._id, "dailyStats.date": currentStat.date },
                                update: { $set: { "dailyStats.$.cpmChange": cpmChange } }
                            }
                        });
                    }
                }
            }
             if (cpmChangeBulkOps.length > 0) {
                 await worksCollection.bulkWrite(cpmChangeBulkOps, { ordered: false });
                 console.log(`[导入 manualDailyUpdate] 完成 cpmChange 的计算和更新 (${cpmChangeBulkOps.length} updates).`);
             }
        }
        console.log(`[导入 manualDailyUpdate] 手动日报同步完成。处理行数: ${dataRows.length}, 更新/新增: ${updatedCount}, 跳过: ${skippedCount}`);
        return { processedRows: dataRows.length, created: 0, updated: updatedCount };

    }
    else {
        const DATA_MAPPING = {
            '星图任务ID': { dbField: 'collaborationId', type: 'lookup' }, '视频ID': { dbField: 'platformWorkId', type: 'string' }, '视频实际发布时间': { dbField: 'publishedAt', type: 'date' },
            '数据最后更新时间': { dbField: 'statsUpdatedAt', type: 'date' }, '播放量': { dbField: 'totalViews', type: 'number' }, '点赞量': { dbField: 'likeCount', type: 'number' },
            '评论量': { dbField: 'commentCount', type: 'number' }, '分享量': { dbField: 'shareCount', type: 'number' }, '组件曝光量': { dbField: 'componentImpressionCount', type: 'number' },
            '组件点击量': { dbField: 'componentClickCount', type: 'number' }, '视频完播率': { dbField: 'completionRate', type: 'number' }, '分频次触达人数-1次': { dbField: 'reachByFrequency.freq1', type: 'number' },
            '分频次触达人数-2次': { dbField: 'reachByFrequency.freq2', type: 'number' }, '分频次触达人数-3次': { dbField: 'reachByFrequency.freq3', type: 'number' }, '分频次触达人数-4次': { dbField: 'reachByFrequency.freq4', type: 'number' },
            '分频次触达人数-5次': { dbField: 'reachByFrequency.freq5', type: 'number' }, '分频次触达人数-6次': { dbField: 'reachByFrequency.freq6', type: 'number' }, '分频次触达人数-7次及以上': { dbField: 'reachByFrequency.freq7plus', type: 'number' },
        };
        const starQuestIdColumnName = '星图任务ID';
        const starQuestIdIndex = header.indexOf(starQuestIdColumnName);
        if (starQuestIdIndex === -1) throw new AppError(`"${starQuestIdColumnName}" column not found in the sheet header.`, 400);

        for (const row of dataRows) {
            const starQuestId = row[starQuestIdIndex];
            if (!starQuestId) { skippedCount++; continue; }
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
                        if (mapping.type === 'number') value = parseFloat(String(value).replace(/,/g, '')) || 0;
                        else if (mapping.type === 'date') value = new Date(value);
                        else if (mapping.type === 'percentage') value = parseFlexibleNumber(value, true);
                        else value = String(value);
                    } catch (e) {
                        console.warn(`[导入 ${dataType}] 无法转换值 ${colName}: ${row[index]}`);
                        return;
                    }

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
                        const newWorkDoc = {
                            ...updatePayload,
                            id: `work_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
                            collaborationId: collaboration.id,
                            projectId: collaboration.projectId,
                            talentId: collaboration.talentId,
                            sourceType: 'COLLABORATION',
                            createdAt: new Date(),
                            updatedAt: new Date(),
                        };
                        await worksCollection.insertOne(newWorkDoc);
                        createdCount++;
                    }
                } else {
                     skippedCount++;
                }
            } else {
                 skippedCount++;
                 if (skippedCount <= 5) {
                     console.warn(`[导入 ${dataType}] 未找到合作记录 taskId: ${starQuestIdStr}`);
                 }
            }
        }
        console.log(`[导入 ${dataType}] 项目同步完成。处理: ${dataRows.length}行, 新建: ${createdCount}, 更新: ${updatedCount}, 跳过: ${skippedCount}`);
        return { processedRows: dataRows.length, created: createdCount, updated: updatedCount };
    }
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
            const headersToken = extractToken(payload);
            if (!headersToken) throw new AppError('Missing spreadsheetToken or feishuUrl for getSheetHeaders.', 400);
            return await getSheetHeaders({ spreadsheetToken: headersToken });
        case 'generateAutomationReport':
             if (!payload || !payload.mappingTemplate || !payload.taskIds) {
                 throw new AppError('Invalid payload structure for generateAutomationReport.', 400);
             }
            return await generateAutomationSheet(payload);
        case 'talentPerformance':
        case 't7':
        case 't21':
        case 'manualDailyUpdate':
        {
            const token = extractToken({ ...legacyParams, ...payload });
            if (!token) throw new AppError(`Missing spreadsheetToken or a valid feishuUrl for ${dataType}.`, 400);

            if (dataType === 'talentPerformance') {
                const result = await handleTalentImport(token);
                return result;
            } else {
                const result = await performProjectSync(token, dataType);
                return result;
            }
        }
        default:
            throw new AppError(`Invalid dataType "${dataType}". Supported types are: getMappingSchemas, getSheetHeaders, generateAutomationReport, talentPerformance, t7, t21, manualDailyUpdate.`, 400);
    }
}

module.exports = { handleFeishuRequest };