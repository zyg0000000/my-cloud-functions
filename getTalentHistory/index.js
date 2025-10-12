exports.handler = async function handler(event, context) {
  console.log(`received new request, request id: %s`, context.requestId);

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ 'message': 'hello world' }),
  };
};
