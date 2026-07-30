exports.handler = async () => ({
  statusCode: 200,
  headers: {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
  },
  body: JSON.stringify({
    status: 'retired',
    retired: true,
    message: 'The legacy Pipedrive-to-Katana sync is retired.',
  }),
});
