/**
 * @file Cloud Function: automation-workflows
 * @version 2.1 - Composite Workflow Support
 * @description Manages CRUD operations for automation workflows.
 * --- UPDATE (v2.1) ---
 * - [功能增强] 在 POST 和 PUT 方法中，正式加入了对 `composite` 类型的支持。
 * - 这使得前端可以创建包含多种原子能力（如截图和数据抓取）的组合工作流。
 */
const { MongoClient, ObjectId } = require('mongodb');

// Environment variables should be configured in the cloud function settings
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'kol_data';
const COLLECTION_NAME = 'automation-workflows';

let cachedDb = null;

/**
 * Connects to the MongoDB database.
 * @returns {Promise<Db>} A promise that resolves to the database instance.
 */
async function connectToDatabase() {
    if (cachedDb) {
        return cachedDb;
    }
    const client = new MongoClient(MONGO_URI, { connectTimeoutMS: 5000, serverSelectionTimeoutMS: 5000 });
    try {
        await client.connect();
        const db = client.db(DB_NAME);
        cachedDb = db;
        console.log("Successfully connected to MongoDB.");
        return db;
    } catch (err) {
        console.error("Failed to connect to MongoDB", err);
        throw new Error("Unable to connect to the database.");
    }
}

/**
 * The main handler for the cloud function, compatible with API Gateway trigger.
 * @param {object} event - The event object from the API gateway.
 * @param {object} context - The context object from the runtime.
 * @returns {Promise<object>} A promise that resolves to the HTTP response object.
 */
module.exports.handler = async (event, context) => {
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Content-Type': 'application/json'
    };

    if (event.httpMethod === 'OPTIONS') {
        return {
            statusCode: 204,
            headers,
            body: ''
        };
    }

    try {
        const db = await connectToDatabase();
        const collection = db.collection(COLLECTION_NAME);
        const body = event.body ? JSON.parse(event.body) : {};
        
        // --- MODIFICATION: Extract ID from query params for DELETE/PUT ---
        const workflowIdFromQuery = event.queryStringParameters?.id;

        switch (event.httpMethod) {
            case 'GET': {
                const workflows = await collection.find({}).sort({ createdAt: -1 }).toArray();
                return {
                    statusCode: 200,
                    headers,
                    body: JSON.stringify({ success: true, data: workflows })
                };
            }

            case 'POST': {
                if (!body.name || !body.steps || !Array.isArray(body.steps)) {
                    return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: 'Missing required fields: name and steps array.' }) };
                }
                const newWorkflow = {
                    name: body.name,
                    description: body.description || '',
                    type: body.type || 'screenshot', // Allows 'screenshot', 'data_scraping', or 'composite'
                    steps: body.steps,
                    createdAt: new Date(),
                    updatedAt: new Date()
                };
                const createResult = await collection.insertOne(newWorkflow);
                return {
                    statusCode: 201,
                    headers,
                    body: JSON.stringify({ success: true, data: { insertedId: createResult.insertedId } })
                };
            }
            
            case 'PUT': {
                // Use ID from query parameter if available, fallback to body
                const workflowIdToUpdate = workflowIdFromQuery || body._id;
                if (!workflowIdToUpdate) {
                    return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: 'Workflow ID is required for updates.' }) };
                }
                if (!ObjectId.isValid(workflowIdToUpdate)) {
                     return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: 'Invalid Workflow ID format.' }) };
                }

                delete body._id; // Ensure _id is not in the $set operator
                const updateData = { ...body, updatedAt: new Date() };
                
                const updateResult = await collection.updateOne(
                    { _id: new ObjectId(workflowIdToUpdate) },
                    { $set: updateData }
                );

                if (updateResult.matchedCount === 0) {
                    return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: 'Workflow not found.' }) };
                }
                return { statusCode: 200, headers, body: JSON.stringify({ success: true, data: { updatedId: workflowIdToUpdate } }) };
            }

            case 'DELETE': {
                 if (!workflowIdFromQuery) {
                    return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: 'Workflow ID is required in query parameter for deletion.' }) };
                }
                 if (!ObjectId.isValid(workflowIdFromQuery)) {
                     return { statusCode: 400, headers, body: JSON.stringify({ success: false, message: 'Invalid Workflow ID format.' }) };
                }

                const deleteResult = await collection.deleteOne({ _id: new ObjectId(workflowIdFromQuery) });

                if (deleteResult.deletedCount === 0) {
                    return { statusCode: 404, headers, body: JSON.stringify({ success: false, message: 'Workflow not found.' }) };
                }
                return { statusCode: 204, headers, body: '' }; // Use 204 for successful deletion
            }

            default:
                return {
                    statusCode: 405,
                    headers,
                    body: JSON.stringify({ success: false, message: `Method Not Allowed: ${event.httpMethod}` })
                };
        }
    } catch (error) {
        console.error('Error processing request:', error);
        const responseBody = {
            success: false,
            message: 'An internal server error occurred.',
            error: error.message
        };
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify(responseBody)
        };
    }
};
