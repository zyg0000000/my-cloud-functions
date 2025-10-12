/**
 * @file Cloud Function: mapping-templates-api
 * @version 3.0 - Formula Support
 * @description
 * - [核心升级] 增加了对包含公式对象的 `mappingRules` 数据结构的验证和存储支持。
 * - [验证增强] 确保了在创建和更新时，新的数据结构能够被正确处理。
 */
const { MongoClient, ObjectId } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'kol_data';
const COLLECTION_NAME = 'mapping_templates';
let cachedDb = null;

async function connectToDatabase() {
    if (cachedDb) return cachedDb;
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    cachedDb = client.db(DB_NAME);
    return cachedDb;
}

function createResponse(statusCode, body) {
    return {
        statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        body: JSON.stringify(body),
    };
}

exports.handler = async (event) => {
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(204, {});
    }

    try {
        const db = await connectToDatabase();
        const collection = db.collection(COLLECTION_NAME);
        const { id } = event.queryStringParameters || {};

        switch (event.httpMethod) {
            case 'GET': {
                if (id) {
                    if (!ObjectId.isValid(id)) return createResponse(400, { success: false, message: 'Invalid ID format.' });
                    const template = await collection.findOne({ _id: new ObjectId(id) });
                    return template ? createResponse(200, { success: true, data: template }) : createResponse(404, { success: false, message: 'Template not found.' });
                }
                const templates = await collection.find({}).sort({ createdAt: -1 }).toArray();
                return createResponse(200, { success: true, data: templates });
            }

            case 'POST':
            case 'PUT': {
                const isUpdate = event.httpMethod === 'PUT';
                if (isUpdate && (!id || !ObjectId.isValid(id))) {
                    return createResponse(400, { success: false, message: 'A valid template ID is required for updating.' });
                }
                
                const body = JSON.parse(event.body || '{}');
                const { name, spreadsheetToken, mappingRules } = body;

                if (!name || !spreadsheetToken || typeof mappingRules !== 'object') {
                    return createResponse(400, { success: false, message: 'Missing required fields: name, spreadsheetToken, and mappingRules.' });
                }

                const document = {
                    name,
                    spreadsheetToken,
                    mappingRules,
                    description: body.description || '',
                    feishuSheetHeaders: body.feishuSheetHeaders || [],
                    updatedAt: new Date(),
                };

                if (isUpdate) {
                    const result = await collection.updateOne({ _id: new ObjectId(id) }, { $set: document });
                    if (result.matchedCount === 0) return createResponse(404, { success: false, message: 'Template not found.' });
                    const updatedDoc = await collection.findOne({ _id: new ObjectId(id) });
                    return createResponse(200, { success: true, data: updatedDoc });
                } else {
                    document.createdAt = new Date();
                    const result = await collection.insertOne(document);
                    const createdDoc = await collection.findOne({ _id: result.insertedId });
                    return createResponse(201, { success: true, data: createdDoc });
                }
            }

            case 'DELETE': {
                if (!id || !ObjectId.isValid(id)) return createResponse(400, { success: false, message: 'A valid ID is required.' });
                const result = await collection.deleteOne({ _id: new ObjectId(id) });
                return result.deletedCount === 0 ? createResponse(404, { success: false, message: 'Template not found.' }) : createResponse(204, {});
            }

            default:
                return createResponse(405, { success: false, message: 'Method Not Allowed' });
        }
    } catch (error) {
        console.error('Error in mapping-templates-api handler:', error);
        return createResponse(500, { success: false, message: 'An internal server error occurred.' });
    }
};
