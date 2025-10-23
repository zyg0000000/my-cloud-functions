/**
 * @file utils.js
 * @version 11.0 - Manual Daily Update Support
 * @description
 * - [核心功能] 新增了对 dataType 'manualDailyUpdate' 的处理逻辑，用于手动同步超过14天的项目日报数据。
 * - [独立逻辑] 'manualDailyUpdate' 分支不依赖全局 DATA_MAPPING，直接按固定列名读取数据，提取日期，并更新 works 集合中的 dailyStats 数组。
 * - [CPM 计算] 在更新 dailyStats 时，会根据关联的 collaboration 和 project 信息重新计算 CPM 和 CPM 变化。
 * - [兼容性] 保留并兼容了 handleTalentImport 和 performProjectSync (用于 t7/t21) 的原有功能。
 * - [日志增强] 为手动更新逻辑添加了详细的日志记录。
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

// --- 新增: 数据库集合名称常量 ---
const COLLABORATIONS_COLLECTION = 'collaborations';
const WORKS_COLLECTION = 'works';
const PROJECTS_COLLECTION = 'projects'; // 用于获取 projectDiscount
const TALENTS_COLLECTION = 'talents'; // 用于 handleTalentImport
const MAPPING_TEMPLATES_COLLECTION = 'mapping_templates'; // 用于 generateAutomationSheet
const AUTOMATION_TASKS_COLLECTION = 'automation-tasks'; // 用于 generateAutomationSheet

// --- 模块级缓存 ---
let tenantAccessToken = null;
let tokenExpiresAt = 0;
let dbClient = null;

// --- 数据结构“总菜单”定义 (保持不变，仅用于 generateAutomationSheet 和 getMappingSchemas) ---
const DATA_SCHEMAS = {
    talents: { displayName: "达人信息", fields: [ { path: "nickname", displayName: "达人昵称" }, { path: "xingtuId", displayName: "星图ID" }, { path: "uid", displayName: "UID" }, { path: "latestPrice", displayName: "最新价格", isSpecial: true }, ] },
    projects: { displayName: "项目信息", fields: [ { path: "name", displayName: "项目名称" }, { path: "qianchuanId", displayName: "仟传项目编号" }, ] },
    collaborations: { displayName: "合作信息", fields: [ { path: "taskId", displayName: "任务ID (星图)" }, { path: "videoId", displayName: "视频ID (平台)" }, { path: "orderType", displayName: "订单类型" }, { path: "status", displayName: "合作状态" }, { path: "amount", displayName: "合作金额" }, { path: "publishDate", displayName: "实际发布日期" }, ] },
    "automation-tasks": { displayName: "自动化任务", fields: [ { path: "result.data.预期CPM", displayName: "预期CPM" }, { path: "result.data.完播率", displayName: "完播率" }, { path: "result.data.爆文率", displayName: "爆文率" }, { path: "result.data.个人视频播放量均值", displayName: "个人视频播放量均值" }, { path: "result.data.星图频播放量均值", displayName: "星图视频播放量均值" }, { path: "result.data.用户画像总结", displayName: "用户画像总结" }, { path: "result.screenshots.0.url", displayName: "截图1 (达人价格)", isImage: true }, { path: "result.screenshots.1.url", displayName: "截图2 (星图视频)", isImage: true }, { path: "result.screenshots.2.url", displayName: "截图3 (男女比例)", isImage: true }, { path: "result.screenshots.3.url", displayName: "截图4 (年龄分布)", isImage: true }, { path: "result.screenshots.4.url", displayName: "截图5 (城市等级)", isImage: true }, { path: "result.screenshots.5.url", displayName: "截图6 (八大人群)", isImage: true }, { path: "result.screenshots.6.url", displayName: "截图7 (设备截图)", isImage: true }, ] }
};

// --- 辅助函数 (保持不变) ---
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
    if (!url.includes('/')) return url; // Assume it's already a token if no slash
    try {
        const pathParts = new URL(url).pathname.split('/');
        // Find 'sheets' or 'folder', then take the next part
        const driveTypeIndex = pathParts.findIndex(part => ['sheets', 'folder', 'spreadsheet'].includes(part)); // Added 'spreadsheet'
        if (driveTypeIndex > -1 && pathParts.length > driveTypeIndex + 1) {
            return pathParts[driveTypeIndex + 1];
        }
    } catch (error) {
        console.warn(`Could not parse URL: ${url}`, error);
    }
    // Fallback if URL parsing fails or no token found
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

// --- 计算引擎核心 (保持不变) ---
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
            return 'N/A'; // Avoid division by zero for numeric contexts
        }

        // Use Function constructor for safe evaluation
        const calculate = new Function('FORMULA_FUNCTIONS', `return ${expression}`);
        const result = calculate(FORMULA_FUNCTIONS);

        return result;

    } catch (error) {
        console.error(`执行公式 "${formula}" 时出错:`, error);
        return 'N/A'; // Return 'N/A' on error
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
    // Default format
    return String(value);
}

// --- 飞书API辅助函数 (保持不变) ---
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
    // 1. Get the first sheet ID
    const sheetsResponse = await axios.get(`https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/${spreadsheetToken}/sheets/query`, { headers: { 'Authorization': `Bearer ${token}` } });
    if (sheetsResponse.data.code !== 0) throw new AppError(`Failed to get sheets info: ${sheetsResponse.data.msg}`, 500);
    const firstSheetId = sheetsResponse.data.data.sheets[0].sheet_id;

    // 2. Construct the range (default to A1:ZZ2000 if none provided)
    const finalRange = range || `${firstSheetId}!A1:ZZ2000`;
    const urlEncodedRange = encodeURIComponent(finalRange.startsWith(firstSheetId) ? finalRange : `${firstSheetId}!${finalRange}`);

    // 3. Read the values
    console.log(`--> [飞书读取] 目标表格: ${spreadsheetToken}, 范围: ${decodeURIComponent(urlEncodedRange)}`);
    const valuesResponse = await axios.get(
        `https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${spreadsheetToken}/values/${urlEncodedRange}?valueRenderOption=ToString`,
        { headers: { 'Authorization': `Bearer ${token}` } }
    );
    if (valuesResponse.data.code !== 0) throw new AppError(`Failed to read sheet values: ${valuesResponse.data.msg}`, 500);
    console.log(`--> [飞书读取] 成功读取 ${valuesResponse.data.data.valueRange.values?.length || 0} 行数据。`);
    return valuesResponse.data.data.valueRange.values;
}

// ... 其他飞书API辅助函数 (transferOwner, grantEditPermissions, moveFileToFolder) 保持不变 ...
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

// --- 业务逻辑：导出功能 (保持不变) ---
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
    // ... (generateAutomationSheet 逻辑保持不变) ...
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
    const copyPayload = { name: newFileName, type: 'sheet', folder_token: "" }; // Initially create in root
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
            // Associate the correct task back based on collaborationId
            { $addFields: { taskObjectId: { $toObjectId: "" } } }, // Placeholder for ObjectId matching
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
                              { $in: ["$_id", objectIdTaskIds] } // Ensure we only link tasks requested
                            ]
                         }
                      }
                   },
                   { $limit: 1 } // Only need one matching task per collaboration
                 ],
                 as: "task"
               }
            },
            { $unwind: { path: '$task', preserveNullAndEmptyArrays: true } }
        ];
        results = await db.collection(COLLABORATIONS_COLLECTION).aggregate(pipeline).toArray();
        // Add latest price logic if needed
        results.forEach(doc => {
            if(doc.talent && Array.isArray(doc.talent.prices) && doc.talent.prices.length > 0) {
                 // Sort prices to find the latest confirmed or provisional
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
        // Map results to the data structure needed for formula evaluation
        const contextData = results.map(doc => ({
            talents: doc.talent,
            projects: doc.project,
            'automation-tasks': doc.task, // Use the correct key as defined in DATA_SCHEMAS
            collaborations: doc
        }));

        for (let i = 0; i < contextData.length; i++) {
            const context = contextData[i];
            const rowData = [];
            for (let j = 0; j < mappingTemplate.feishuSheetHeaders.length; j++) {
                const feishuHeader = mappingTemplate.feishuSheetHeaders[j];
                const rule = mappingTemplate.mappingRules[feishuHeader];
                let finalValue = null; // Default to null for empty cells

                if (typeof rule === 'string') { // Direct Mapping
                    const pathParts = rule.split('.');
                    if (pathParts.length > 1) {
                        const collection = pathParts[0];
                        const trueContext = context[collection];
                        finalValue = pathParts.slice(1).reduce((obj, key) => (obj && obj[key] !== undefined) ? obj[key] : null, trueContext);
                    }
                } else if (typeof rule === 'object' && rule !== null && rule.formula) { // Formula Calculation
                    const rawResult = evaluateFormula(rule.formula, context);
                    finalValue = rule.output ? formatOutput(rawResult, rule.output) : rawResult;
                }

                // Check if the source indicates an image and the value is a valid URL
                const isImageField = (typeof rule === 'string' && rule.includes('screenshots'));
                if (isImageField && typeof finalValue === 'string' && finalValue.startsWith('http')) {
                    rowData.push(null); // Keep cell blank for images initially
                    imageWriteQueue.push({ range: `${columnIndexToLetter(j)}${START_ROW + i}`, url: finalValue, name: `${feishuHeader}.png` });
                } else {
                    // Ensure N/A or actual value is pushed
                    rowData.push(finalValue === null || finalValue === undefined ? null : finalValue);
                }
            }
            dataToWrite.push(rowData);
        }

        // Get the first sheet ID for writing
        const metaInfoResponse = await axios.get(`https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/${newSpreadsheetToken}/metainfo`, { headers: { 'Authorization': `Bearer ${token}` } });
        const firstSheetId = metaInfoResponse.data.data.sheets[0].sheetId;

        // Write text/numeric data
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

        // Write images sequentially to avoid rate limiting issues
        if (imageWriteQueue.length > 0) {
             console.log(`--> [写入图片] 准备写入 ${imageWriteQueue.length} 张图片...`);
            for (const imageJob of imageWriteQueue) {
                const imageRange = `${firstSheetId}!${imageJob.range}:${imageJob.range}`; // Range for a single cell
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
    // Only grant edit permissions if owner transfer failed or was skipped
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


// --- 业务逻辑：导入功能 (保持不变) ---
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
            // Handle cases where percentage might be entered as 50 instead of 0.5 or 50%
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
    if (!rows || rows.length < 2) return { data: [] }; // Need header + at least one data row
    const header = rows[0];
    const dataRows = rows.slice(1);
    const processedData = [];

    // Map headers to indices for robust column lookup
    const headerMap = new Map(header.map((col, i) => [col.trim(), i]));

    for (const row of dataRows) {
        const getValue = (colName, isPercentage = false) => {
            const index = headerMap.get(colName);
            return (index !== undefined && row[index] !== null && row[index] !== '') ? parseFlexibleNumber(row[index], isPercentage) : 0;
        };

        const xingtuIdIndex = headerMap.get('达人id') ?? headerMap.get('星图ID'); // Support variations
        const xingtuId = (xingtuIdIndex !== undefined && row[xingtuIdIndex]) ? String(row[xingtuIdIndex]).trim() : null;

        if (!xingtuId) {
            console.warn("[导入] 跳过一行，因为缺少 达人id 或 星图ID。");
            continue; // Skip row if mandatory ID is missing
        }

        const talentData = { xingtuId, performanceData: {} };

        // Define mappings - use ?? to check both Chinese and potentially English headers
        const mappings = [
            { key: 'cpm60s', header: '预期cpm' },
            { key: 'maleAudienceRatio', header: '男性粉丝占比', isPercentage: true },
            { key: 'femaleAudienceRatio', header: '女性粉丝占比', isPercentage: true },
            { key: 'ratio_18_23', header: '18-23岁粉丝比例', isPercentage: true },
            { key: 'ratio_24_30', header: '24-30岁粉丝比例', isPercentage: true },
            { key: 'ratio_31_40', header: '31-40岁粉丝比例', isPercentage: true },
            { key: 'ratio_41_50', header: '41-50岁粉丝比例', isPercentage: true },
            { key: 'ratio_50_plus', header: '50岁以上粉丝比例', isPercentage: true },
            // Add more mappings as needed
        ];

        mappings.forEach(m => {
             const value = getValue(m.header, m.isPercentage);
             // Only add the field if the value is not zero (or was successfully parsed)
             if (value !== 0 || (headerMap.has(m.header) && row[headerMap.get(m.header)] !== null)) {
                 talentData.performanceData[m.key] = value;
             }
        });

        // Calculate combined ratios after individual ones are processed
        const ratio18_40 = (talentData.performanceData.ratio_18_23 || 0) + (talentData.performanceData.ratio_24_30 || 0) + (talentData.performanceData.ratio_31_40 || 0);
        const ratio40_plus = (talentData.performanceData.ratio_41_50 || 0) + (talentData.performanceData.ratio_50_plus || 0);

        if (ratio18_40 > 0) talentData.performanceData.audience_18_40_ratio = ratio18_40;
        if (ratio40_plus > 0) talentData.performanceData.audience_40_plus_ratio = ratio40_plus;

        processedData.push(talentData);
    }
    console.log(`[导入] 成功处理 ${processedData.length} 条达人记录。`);
    return { data: processedData, message: `Successfully read ${processedData.length} records from Feishu Sheet.` };
}

// [源自 v8.7, 包含 v11.0 的 manualDailyUpdate 逻辑]
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
    const projectsCollection = db.collection(PROJECTS_COLLECTION); // Needed for manualDailyUpdate CPM calc

    let createdCount = 0;
    let updatedCount = 0;
    let skippedCount = 0;

    // --- [新增 v11.0] 手动日报更新逻辑 ---
    if (dataType === 'manualDailyUpdate') {
        // 1. 定义所需列名和索引
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
        const collabProjectMap = new Map(); // Cache projectId for collaborations

        for (const row of dataRows) {
            const taskId = row[taskIdIndex] ? String(row[taskIdIndex]).trim() : null;
            
            // [V11.1 修复] 如果 taskId 为空，说明这很可能是一个空行，直接静默跳过，不再打印日志。
            if (!taskId) {
                skippedCount++;
                continue;
            }

            // [V11.1 修复] 如果 taskId 存在，但其他数据缺失，才打印警告。
            const timestampStr = row[timestampIndex];
            const viewsStr = row[viewsIndex];
            if (!timestampStr || viewsStr === null || viewsStr === undefined) {
                console.warn(`[导入 manualDailyUpdate] 跳过行 ${taskId}，缺少 timestamp 或 views: ${JSON.stringify(row)}`);
                skippedCount++;
                continue;
            }

            // 2. 提取日期 (YYYY-MM-DD)
            let dateStr;
            try {
                // 尝试解析完整日期时间戳，然后格式化
                dateStr = new Date(timestampStr).toISOString().split('T')[0];
                if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) throw new Error("Invalid Date Format");
            } catch (e) {
                console.warn(`[导入 manualDailyUpdate] 跳过行，无法从 '${timestampStr}' 提取有效日期 YYYY-MM-DD: ${JSON.stringify(row)}`);
                skippedCount++;
                continue;
            }

            const totalViews = parseInt(String(viewsStr).replace(/,/g, ''), 10);
            if (isNaN(totalViews)) {
                console.warn(`[导入 manualDailyUpdate] 跳过行 ${taskId}，无法解析播放量 '${viewsStr}': ${JSON.stringify(row)}`);
                skippedCount++;
                continue;
            }

            // 3. 查找 collaboration 和 project (带缓存)
            let collab = collabProjectMap.get(taskId);
            if (!collab) {
                collab = await collaborationsCollection.findOne({ taskId: taskId });
                if (collab) {
                     const project = await projectsCollection.findOne({ id: collab.projectId });
                     collab.projectDiscount = project ? (parseFloat(project.discount) || 1.0) : 1.0; // Store discount with collab
                     collabProjectMap.set(taskId, collab);
                }
            }

            if (!collab) {
                console.warn(`[导入 manualDailyUpdate] 跳过行，未找到 taskId '${taskId}' 对应的合作记录: ${JSON.stringify(row)}`);
                skippedCount++;
                continue;
            }

            // 4. 计算 CPM
            const amount = parseFloat(collab.amount) || 0;
            const income = amount * collab.projectDiscount * 1.05;
            const cpm = income > 0 && totalViews > 0 ? (income / totalViews) * 1000 : 0;

            // 5. 准备 $pull 和 $push 操作
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
                                $each: [{ date: dateStr, totalViews: totalViews, cpm: cpm, cpmChange: null, solution: '' }], // cpmChange 先设为 null
                                $sort: { date: 1 } // 保持日期有序
                            }
                        },
                         $setOnInsert: { // Ensure work exists if pushing
                            id: `work_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
                            projectId: collab.projectId,
                            talentId: collab.talentId,
                            sourceType: 'COLLABORATION',
                            createdAt: new Date()
                        },
                        $set: { updatedAt: new Date() } // Always update timestamp
                    },
                    upsert: true // Create work if not exists
                }
            };
            bulkOps.push(pullOp, pushOp);
        }

        // 6. 执行批量写入
        if (bulkOps.length > 0) {
            const bulkResult = await worksCollection.bulkWrite(bulkOps, { ordered: false });
            updatedCount = bulkResult.modifiedCount + bulkResult.upsertedCount; // Count updates and potential upserts
            console.log(`[导入 manualDailyUpdate] BulkWrite 完成. Matched: ${bulkResult.matchedCount}, Modified: ${bulkResult.modifiedCount}, Upserted: ${bulkResult.upsertedCount}`);

            // 7. [后处理] 计算 cpmChange (需要再次查询)
            const collabIdsToUpdate = [...new Set(bulkOps.filter(op => op.updateOne.filter.collaborationId).map(op => op.updateOne.filter.collaborationId))];
            const updatedWorks = await worksCollection.find({ collaborationId: { $in: collabIdsToUpdate } }).toArray();
            const cpmChangeBulkOps = [];
            for (const work of updatedWorks) {
                if (!work.dailyStats || work.dailyStats.length < 2) continue;
                for (let i = 1; i < work.dailyStats.length; i++) {
                    const currentStat = work.dailyStats[i];
                    const prevStat = work.dailyStats[i-1];
                    const cpmChange = (prevStat.cpm !== null && currentStat.cpm !== null) ? currentStat.cpm - prevStat.cpm : null;
                    // Only update if cpmChange is different or was null
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
        console.log(`[导入 manualDailyUpdate] 手动日报同步完成。处理行数: ${dataRows.length}, 更新/新增 works 记录: ${updatedCount}, 跳过: ${skippedCount}`);
        return { processedRows: dataRows.length, created: 0, updated: updatedCount }; // Reporting slightly differently

    }
    // --- End manualDailyUpdate ---

    // --- t7/t21 Logic (保持不变) ---
    else {
        // ... (Original performProjectSync logic for t7/t21 using DATA_MAPPING) ...
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
            if (!starQuestId) { skippedCount++; continue; }
            const starQuestIdStr = String(starQuestId).trim();
            const collaboration = await collaborationsCollection.findOne({ "taskId": starQuestIdStr });
            if (collaboration) {
                const updatePayload = {};
                const prefix = dataType; // 't7' or 't21'
                header.forEach((colName, index) => {
                    const mapping = DATA_MAPPING[colName];
                    if (!mapping || colName === starQuestIdColumnName) return; // Skip lookup key
                    let value = row[index];
                    if (value === null || value === undefined || String(value).trim() === '') return; // Skip empty cells
                    try {
                        // Perform type conversion based on mapping
                        if (mapping.type === 'number') value = parseFloat(String(value).replace(/,/g, '')) || 0;
                        else if (mapping.type === 'date') value = new Date(value);
                        else if (mapping.type === 'percentage') value = parseFlexibleNumber(value, true);
                        else value = String(value); // Default to string
                    } catch (e) {
                        console.warn(`[导入 ${dataType}] Could not convert value for ${colName}: ${row[index]}. Skipping field.`);
                        return; // Skip this field if conversion fails
                    }

                    // Handle nested fields like reachByFrequency
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

                if (Object.keys(updatePayload).length > 0) { // Only update if there's data
                    if (existingWork) {
                        await worksCollection.updateOne({ _id: existingWork._id }, { $set: { ...updatePayload, updatedAt: new Date() } });
                        updatedCount++;
                    } else {
                        // Create new work doc if not exists, ONLY if we have data to insert
                        const newWorkDoc = {
                            ...updatePayload, // Add the actual data
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
                     skippedCount++; // Count rows with ID but no updatable data as skipped
                }
            } else {
                 skippedCount++; // Count rows where collaboration wasn't found as skipped
                 console.warn(`[导入 ${dataType}] Collaboration not found for taskId: ${starQuestIdStr}. Skipping row.`);
            }
        }
        console.log(`[导入 ${dataType}] 项目同步完成。处理行数: ${dataRows.length}, 新建: ${createdCount}, 更新: ${updatedCount}, 跳过: ${skippedCount}`);
        return { processedRows: dataRows.length, created: createdCount, updated: updatedCount };
    }
}


// --- 总调度函数 (保持不变) ---
async function handleFeishuRequest(requestBody) {
    const { dataType, payload, ...legacyParams } = requestBody;
    if (!dataType) throw new AppError('Missing required parameter: dataType.', 400);

    // Helper to extract token robustly
    const extractToken = (data) => {
        if (!data) return null;
        const tokenSource = data.spreadsheetToken || data.feishuUrl; // Check both legacy and new way
        return getSpreadsheetTokenFromUrl(tokenSource); // Use the improved parser
    };

    switch (dataType) {
        case 'getMappingSchemas':
            return await getMappingSchemas();
        case 'getSheetHeaders':
            const headersToken = extractToken(payload);
            if (!headersToken) throw new AppError('Missing spreadsheetToken or feishuUrl for getSheetHeaders.', 400);
            return await getSheetHeaders({ spreadsheetToken: headersToken });
        case 'generateAutomationReport':
            // Assume payload contains the necessary structure from mapping_templates.js
             if (!payload || !payload.mappingTemplate || !payload.taskIds) {
                 throw new AppError('Invalid payload structure for generateAutomationReport.', 400);
             }
            return await generateAutomationSheet(payload);
        case 'talentPerformance': // Fall through for talent import
        case 't7':                // Fall through for T7 sync
        case 't21':               // Fall through for T21 sync
        case 'manualDailyUpdate': // Add the new type here
        {
            const token = extractToken({ ...legacyParams, ...payload }); // Check both direct params and payload object
            if (!token) throw new AppError(`Missing spreadsheetToken or a valid feishuUrl for ${dataType}.`, 400);

            if (dataType === 'talentPerformance') {
                // Ensure handleTalentImport returns the expected structure
                const result = await handleTalentImport(token);
                // The main handler expects { success: true, data: result }
                return result; // handleTalentImport already returns { data: ..., message: ...}
            } else {
                // Ensure performProjectSync returns the expected structure
                const result = await performProjectSync(token, dataType);
                 // The main handler expects { success: true, data: result }
                return result; // performProjectSync already returns { processedRows: ..., created: ..., updated: ... }
            }
        }
        default:
            throw new AppError(`Invalid dataType "${dataType}". Supported types are: getMappingSchemas, getSheetHeaders, generateAutomationReport, talentPerformance, t7, t21, manualDailyUpdate.`, 400);
    }
}

module.exports = { handleFeishuRequest };

