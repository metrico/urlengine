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
 * @returns {Promise<string>}
 */
async function getContentBody(req) {
  let body = "";
  req.raw.on("data", (data) => {
    body += data.toString();
  });
  await new Promise((resolve) => req.raw.once("end", resolve));
  return body;
}

/**
 * @param req {FastifyRequest}
 * @returns {Promise<void>}
 */
async function genericJSONParser(req) {
  try {
    var body = await getContentBody(req);
    console.log('!!!!!!!!!', body);
    // x-ndjson to json
    const response = body
      .trim()
      .split("\n")
      .map(JSON.parse)
      .map((obj) =>
        Object.entries(obj)
          .sort()
          .reduce((o, [k, v]) => ((o[k] = v), o), {})
      );
    return response;
  } catch (err) {
    err.statusCode = 400;
    throw err;
  }
}

fastify.addContentTypeParser("*", {}, async function (req, body, done) {
  return await genericJSONParser(req);
});

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