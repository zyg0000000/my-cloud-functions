/**
 * [生产版 v3.0 - PDF代理最终版]
 * 云函数：previewFile
 * 描述：作为安全代理，从TOS获取PDF文件并直接流式传输给客户端，以实现可靠的内联预览。
 * --- v3.0 更新日志 ---
 * - [架构最终化] 采用后端代理流式传输方案，彻底解决TOS预览问题。
 * - [健壮性] 增加了完整的错误处理，能正确响应 "文件未找到" 等情况。
 * - [性能] 直接使用TOS SDK返回的 content (Stream) 对象进行传输，高效处理文件。
 * ---------------------
 */

const { TosClient } = require('@volcengine/tos-sdk');
const util = require('util');

// --- 环境变量与客户端初始化 ---
const TOS_ACCESS_KEY_ID = process.env.TOS_ACCESS_KEY_ID;
const TOS_SECRET_ACCESS_KEY = process.env.TOS_SECRET_ACCESS_KEY;
const TOS_ENDPOINT = process.env.TOS_ENDPOINT;
const TOS_REGION = process.env.TOS_REGION;
const TOS_BUCKET_NAME = process.env.TOS_BUCKET_NAME;

let tosClient;
function getTosClient() {
    if (tosClient) return tosClient;
    if (TOS_ACCESS_KEY_ID && TOS_SECRET_ACCESS_KEY && TOS_ENDPOINT && TOS_REGION && TOS_BUCKET_NAME) {
        tosClient = new TosClient({
            accessKeyId: TOS_ACCESS_KEY_ID,
            accessKeySecret: TOS_SECRET_ACCESS_KEY,
            endpoint: TOS_ENDPOINT,
            region: TOS_REGION,
        });
        return tosClient;
    }
    throw new Error("TOS 服务配置不完整，无法初始化。");
}

exports.handler = async (event, context) => {
    let fileKey = event.queryStringParameters ? event.queryStringParameters.fileKey : null;

    if (!fileKey) {
        return { 
            statusCode: 400, 
            headers: {'Content-Type': 'application/json'}, 
            body: JSON.stringify({ success: false, message: "请求缺少必要的 fileKey 参数。" }) 
        };
    }
    
    try {
        fileKey = decodeURIComponent(fileKey);
        const client = getTosClient();

        const response = await client.getObjectV2({
            bucket: TOS_BUCKET_NAME,
            key: fileKey,
        });

        // 基于我们最终的调试结论，文件流在 response.data.content
        const fileStream = response.data.content;
        
        // 验证我们是否得到了一个可读流
        if (!fileStream || typeof fileStream.pipe !== 'function') {
             console.error("--- v3.0 严重错误 ---", `TOS响应的 response.data.content 不是一个有效的Stream。Key: ${fileKey}`);
             throw new Error("TOS返回了非预期的响应结构。");
        }

        // 将Stream转换为Base64编码的字符串以通过API网关传输
        const chunks = [];
        for await (const chunk of fileStream) {
            chunks.push(chunk);
        }
        const fileBuffer = Buffer.concat(chunks);

        return {
            statusCode: 200,
            headers: {
                // 明确告知浏览器这是一个PDF文件，应尝试内联预览
                'Content-Type': 'application/pdf',
                'Content-Disposition': 'inline', 
            },
            isBase64Encoded: true,
            body: fileBuffer.toString('base64'),
        };

    } catch (error) {
        if (error && error.statusCode === 404) {
            console.warn(`文件未找到 (404): 请求的 fileKey '${fileKey}' 在TOS存储桶中不存在。`);
            return {
                statusCode: 404,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: false, message: `文件 '${fileKey}' 未找到。` })
            };
        }
        
        console.error(`处理文件预览(key: ${fileKey})时发生严重错误:`, util.inspect(error, {depth: 5}));
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                success: false, 
                message: "服务器在处理文件时遇到未知问题。", 
                error: error.message || "未知错误"
            })
        };
    }
};

