const fastify = require("fastify")({ logger: true });

// In-memory storage
const storage = new Map();

/** CLICKHOUSE URL SELECT */
fastify.get("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send({ error: 'Key is required' });
  
  const data = storage.get(key);
  if (data === undefined) {
    return reply.code(404).send({ error: 'Not found' });
  }
  return data;
});


/** CLICKHOUSE URL INSERT */
fastify.post("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send({ error: 'Key is required' });
  
  storage.set(key, request.body);
  return { success: true };
});


/**
 * @param req {FastifyRequest}
 * @returns {Promise<Buffer>}
 */
async function getRawBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.raw.on('data', chunk => chunks.push(chunk));
    req.raw.on('end', () => resolve(Buffer.concat(chunks)));
    req.raw.on('error', reject);
  });
}

/**
 * @param req {FastifyRequest}
 * @returns {Promise<object[]>}
 */
async function octetStreamParser(req) {
  try {
    const buffer = await getRawBody(req);
    const jsonString = buffer.toString('utf8');
    if (jsonString.length <= 1) return false;
    return JSON.parse(jsonString);
    /*
    // Split the string into lines and parse each line as JSON
    const jsonObjects = jsonString
      .trim()
      .split('\n')
      .map(line => JSON.parse(line));
    return jsonObjects;
    */
  } catch (err) {
    err.statusCode = 400;
    throw err;
  }
}

// Add a custom parser for 'application/octet-stream'
fastify.addContentTypeParser('application/octet-stream', octetStreamParser);

/** RUN URL Engine */
const start = async () => {
  try {
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
    console.log(`Server is running on http://0.0.0.0:3000`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();