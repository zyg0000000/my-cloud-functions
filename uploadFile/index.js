/**
 * [生产版 v1.5 - 架构优化回滚版]
 * 云函数：uploadFile
 * 描述：统一文件上传云函数，用于将文件上传至TOS。
 * --- v1.5 更新日志 ---
 * - [架构优化] 回滚了 v1.4 的预签名URL生成逻辑。
 * - 本函数现在只负责上传文件，并返回一个标准的、永久的公网访问URL。
 * - 预览功能将由 getProjects 等读取接口在响应时动态生成，实现“存储”与“访问”的关注点分离。
 * ---------------------
 * 触发器：API 网关, 通过 POST /upload-file 路径调用。
 */
const { TosClient } = require('@volcengine/tos-sdk');

// 从环境变量中获取配置
const TOS_ACCESS_KEY_ID = process.env.TOS_ACCESS_KEY_ID;
const TOS_SECRET_ACCESS_KEY = process.env.TOS_SECRET_ACCESS_KEY;
const TOS_ENDPOINT = process.env.TOS_ENDPOINT;
const TOS_REGION = process.env.TOS_REGION;
const TOS_BUCKET_NAME = process.env.TOS_BUCKET_NAME;

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

  try {
    let inputData = {};
    if (event.body) {
        try {
            inputData = JSON.parse(event.body);
        } catch(e) { /* ignore */ }
    }
    if (Object.keys(inputData).length === 0 && event.queryStringParameters) {
        inputData = event.queryStringParameters;
    }

    const { fileName, fileData } = inputData;

    if (!fileName || !fileData) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ success: false, message: '请求体中缺少必要的字段 (fileName, fileData)。' })
      };
    }

    let buffer;
    let mimeType = 'application/octet-stream';

    if (fileData.startsWith('data:')) {
        const parts = fileData.split(';base64,');
        if (parts.length === 2) {
            const mimeTypePart = parts[0].split(':')[1];
            const base64Data = parts[1];
            
            if (mimeTypePart) {
                mimeType = mimeTypePart;
            }
            buffer = Buffer.from(base64Data, 'base64');
        } else {
            buffer = Buffer.from(fileData, 'base64');
        }
    } else {
        buffer = Buffer.from(fileData, 'base64');
    }

    const client = new TosClient({
        accessKeyId: TOS_ACCESS_KEY_ID,
        accessKeySecret: TOS_SECRET_ACCESS_KEY,
        endpoint: TOS_ENDPOINT,
        region: TOS_REGION,
    });

    const extensionIndex = fileName.lastIndexOf('.');
    const extension = extensionIndex > -1 ? fileName.substring(extensionIndex) : '.png';
    const uniqueFileName = `${Date.now()}_${Math.random().toString(36).substring(2, 9)}${extension}`;
    
    await client.putObject({
        bucket: TOS_BUCKET_NAME,
        key: uniqueFileName,
        body: buffer,
        headers: {
            'Content-Type': mimeType
        }
    });

    // [架构优化] 返回标准的、永久的公网URL
    const fileUrl = `https://${TOS_BUCKET_NAME}.${TOS_ENDPOINT}/${uniqueFileName}`;

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: '文件上传成功',
        data: {
          fileName: uniqueFileName,
          url: fileUrl // 返回永久URL
        }
      }),
    };

  } catch (error) {
    console.error('处理请求时发生错误:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ success: false, message: '服务器内部错误', error: error.message }),
    };
  }
};

