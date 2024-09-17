const fastify = require("fastify")({ logger: true });
const fs = require('fs').promises;
const path = require('path');

const DATA_DIR = path.join(__dirname, '/tmp/data');

// Ensure the data directory exists
fs.mkdir(DATA_DIR, { recursive: true }).catch(console.error);

async function readJsonFile(filePath) {
  try {
    const data = await fs.readFile(filePath, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
}

async function writeJsonFile(filePath, data) {
  await fs.writeFile(filePath, JSON.stringify(data, null, 2));
}

async function getFileStats(filePath) {
  try {
    return await fs.stat(filePath);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

/** CLICKHOUSE URL SELECT */
fastify.get("/:database", async (request, reply) => {
  try {
    const { database } = request.params;
    if (!database) return reply.code(400).send({ error: 'Database name is required' });
    
    const filePath = path.join(DATA_DIR, `${database}.json`);
    const items = await readJsonFile(filePath);
    return items;
  } catch (error) {
    request.log.error(error);
    reply.code(500).send({ error: 'Internal Server Error' });
  }
});

/** CLICKHOUSE URL HEAD */
fastify.head("/:database", async (request, reply) => {
  reply.code(200).send(); 
});

/** CLICKHOUSE URL INSERT */
fastify.post("/:database", async (request, reply) => {
  try {
    const { database } = request.params;
    if (!database) return reply.code(400).send({ error: 'Database name is required' });
    
    const filePath = path.join(DATA_DIR, `${database}.json`);
    const existingData = await readJsonFile(filePath);
    
    request.body.forEach((row) => {
      existingData.push(row); // Insert raw JSON objects from ClickHouse
    });
    
    await writeJsonFile(filePath, existingData);
    return {};
  } catch (error) {
    request.log.error(error);
    reply.code(500).send({ error: 'Internal Server Error' });
  }
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