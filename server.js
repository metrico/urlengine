const fastify = require("fastify")({ logger: true });
const fs = require("fs").promises;
const path = require("path");

const STORAGE_DIR = path.join(__dirname, "/tmp/storage");

// Ensure storage directory exists
fs.mkdir(STORAGE_DIR, { recursive: true }).catch(console.error);

async function getFilePath(key) {
  return path.join(STORAGE_DIR, `${key}.json`);
}

async function readFile(key) {
  const filePath = await getFilePath(key);
  try {
    console.log("reading: ", filePath);
    const data = await fs.readFile(filePath, "utf8");
    console.log("got data: ", data);
    return JSON.parse(data);
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function writeFile(key, data) {
  const filePath = await getFilePath(key);
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), "utf8");
}

/** CLICKHOUSE URL SELECT */
fastify.get("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send({ error: "Key is required" });

  const data = await readFile(key);
  if (data === null) {
    return reply.code(404).send({ error: "Not found" });
  }
  return data;
});

/** CLICKHOUSE URL INSERT */
fastify.post("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send({ error: "Key is required" });

  const data = Array.isArray(request.body) ? request.body : [request.body];
  if (data.length == 0) return;
  console.log("writing: ", data);
  await writeFile(key, data);
  return { success: true };
});

/**
 * @param req {FastifyRequest}
 * @returns {Promise<Buffer>}
 */
async function getRawBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.raw.on("data", (chunk) => chunks.push(chunk));
    req.raw.on("end", () => resolve(Buffer.concat(chunks)));
    req.raw.on("error", reject);
  });
}

/**
 * @param req {FastifyRequest}
 * @returns {Promise<object[]>}
 */
async function octetStreamParser(req) {
  try {
    const buffer = await getRawBody(req);
    const jsonString = buffer.toString("utf8");
    if (jsonString.trim().length === 0) {
      return [];
    }
    // Split the string into lines and parse each line as JSON
    const jsonObjects = jsonString
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    return jsonObjects;
  } catch (err) {
    req.log.error("Error parsing octet-stream:", err);
    err.statusCode = 400;
    throw err;
  }
}

// Add a custom parser for 'application/octet-stream'
fastify.addContentTypeParser("application/octet-stream", octetStreamParser);

/** RUN URL Engine */
const start = async () => {
  try {
    await fastify.listen({ port: process.env.PORT || 3000, host: "0.0.0.0" });
    console.log(`Server is running on http://0.0.0.0:3000`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
