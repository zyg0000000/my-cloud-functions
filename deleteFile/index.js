/**
 * [生产版 v2.1 - 语法修复版]
 * 云函数：deleteFile
 * 描述：处理文件删除请求，同步删除TOS上的物理文件和项目数据库中的文件引用。
 * --- v2.1 更新日志 ---
 * - [核心BUG修复] 修正了 require('https) 语句中缺失的单引号，解决了导致部署失败的语法错误。
 * --- v2.0 更新日志 ---
 * - [核心功能] 新增了完整的数据库操作逻辑。
 * - [实现方式] 本函数现在会通过HTTP请求，调用现有的 /projects 和 /update-project 接口来完成数据库的读写。
 */
const https = require('https');
const { TosClient } = require('@volcengine/tos-sdk');

// --- 从环境变量中获取配置 (部署时必须设置) ---
const TOS_ACCESS_KEY_ID = process.env.TOS_ACCESS_KEY_ID;
const TOS_SECRET_ACCESS_KEY = process.env.TOS_SECRET_ACCESS_KEY;
const TOS_ENDPOINT = process.env.TOS_ENDPOINT;
const TOS_REGION = process.env.TOS_REGION;
const TOS_BUCKET_NAME = process.env.TOS_BUCKET_NAME;

// --- 内部API配置 ---
// 该URL是从前端代码 order_list.js 中获取的，必须与您的项目API网关地址一致
const API_BASE_URL = 'https://sd2pl0r2pkvfku8btbid0.apigateway-cn-shanghai.volceapi.com';

/**
 * 一个Node.js环境下的API请求辅助函数
 * @param {string} path API路径 (例如 '/projects')
 * @param {string} method 'GET' 或 'PUT'
 * @param {object} [data=null] 要在请求体中发送的数据
 * @returns {Promise<object>} 解析后的JSON响应
 */
function apiRequest(path, method, data = null) {
    return new Promise((resolve, reject) => {
        const url = new URL(path, API_BASE_URL);
        const options = {
            method: method,
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            timeout: 10000, // 10秒超时
        };

        const req = https.request(url, options, (res) => {
            let body = '';
            res.on('data', (chunk) => { body += chunk; });
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    try {
                        // 确保body不为空再解析
                        resolve(body ? JSON.parse(body) : {});
                    } catch (e) {
                        console.error("API响应JSON解析失败:", body);
                        reject(new Error("API响应JSON解析失败"));
                    }
                } else {
                    reject(new Error(`内部API请求失败, 状态码: ${res.statusCode}, 响应: ${body}`));
                }
            });
        });

        req.on('error', (e) => reject(new Error(`内部API请求网络错误: ${e.message}`)));
        req.on('timeout', () => {
            req.destroy();
            reject(new Error('内部API请求超时'));
        });

        if (data) {
            req.write(JSON.stringify(data));
        }
        req.end();
    });
}

// ... (getKeyFromUrl 函数保持不变)
function getKeyFromUrl(url) {
    try {
        const parsedUrl = new URL(url);
        if (parsedUrl.hostname === `${TOS_BUCKET_NAME}.${TOS_ENDPOINT}`) {
            return parsedUrl.pathname.substring(1);
        }
        if (parsedUrl.pathname.endsWith('/preview-file') && parsedUrl.searchParams.has('fileKey')) {
            const fileKey = parsedUrl.searchParams.get('fileKey');
            if (fileKey) return fileKey;
        }
        console.warn(`URL格式无法识别: ${url}`);
        return null;
    } catch (error) {
        console.error("从URL解析文件名时发生异常:", url, error);
        return null;
    }
}


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
    
    if (event.httpMethod !== 'POST') {
         return { statusCode: 405, headers, body: JSON.stringify({ message: "Method Not Allowed" }) };
    }

    try {
        const body = JSON.parse(event.body || '{}');
        const { projectId, fileUrl } = body;

        if (!projectId || !fileUrl) {
            return {
                statusCode: 400, headers,
                body: JSON.stringify({ success: false, message: '请求体中缺少必要的字段 (projectId, fileUrl)。' })
            };
        }

        const fileKey = getKeyFromUrl(fileUrl);
        if (!fileKey) {
             return {
                statusCode: 400, headers,
                body: JSON.stringify({ success: false, message: `提供的fileUrl格式无法被服务器识别: ${fileUrl}` })
            };
        }

        // --- 步骤 1: 从TOS删除物理文件 ---
        console.log(`[${projectId}] 准备从TOS删除文件, Key: ${fileKey}`);
        const client = new TosClient({
            accessKeyId: TOS_ACCESS_KEY_ID,
            accessKeySecret: TOS_SECRET_ACCESS_KEY,
            endpoint: TOS_ENDPOINT,
            region: TOS_REGION,
        });
        
        // 我们将TOS删除操作也放入try...catch中，以便处理文件不存在的情况
        try {
            await client.deleteObject({ bucket: TOS_BUCKET_NAME, key: fileKey, });
            console.log(`[${projectId}] 文件已从TOS成功删除。`);
        } catch (tosError) {
             if (tosError.statusCode === 404) {
                 console.log(`[${projectId}] 文件在TOS上未找到 (404 Not Found)，可能已被删除。将继续执行数据库清理。`);
             } else {
                 // 如果是其他TOS错误，则抛出让外层catch捕获
                 throw tosError;
             }
        }

        // --- 步骤 2: 从数据库中移除文件引用 (通过API调用实现) ---
        console.log(`[${projectId}] 准备更新数据库，移除文件引用: ${fileUrl}`);
        
        // 2.1 获取项目当前的文件列表
        const projectDataResponse = await apiRequest(`/projects?projectId=${projectId}`, 'GET');
        const currentFiles = projectDataResponse?.data?.projectFiles;

        if (!Array.isArray(currentFiles)) {
             console.warn(`[${projectId}] 无法获取到项目的文件列表，数据库更新操作已跳过。`);
        } else {
            const updatedFiles = currentFiles.filter(file => file.url !== fileUrl);
            // 2.3 只有当文件列表确实发生变化时才调用更新接口
            if (updatedFiles.length < currentFiles.length) {
                 await apiRequest('/update-project', 'PUT', {
                    id: projectId,
                    projectFiles: updatedFiles
                });
                console.log(`[${projectId}] 数据库更新成功。`);
            } else {
                console.log(`[${projectId}] 文件引用已在数据库中不存在，无需更新。`);
            }
        }

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: true, message: '文件删除流程处理完成。' }),
        };

    } catch (error) {
        console.error(`[${'N/A'}] 删除文件时发生严重错误:`, error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }),
        };
    }
};

