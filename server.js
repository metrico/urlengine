const fastify = require("fastify")({ logger: true });
const fs = require("fs").promises;
const path = require("path");

const DB_DIR = path.join(__dirname, "/tmp/db");
const DUCKDB_MAGIC_HEADER = "DUCK";
const MAGIC_HEADER_LENGTH = 24;

// Ensure storage directory exists
fs.mkdir(DB_DIR, { recursive: true }).catch(console.error);

async function getFilePath(key) {
  return path.join(DB_DIR, `${key}`);
}

async function readFile(key, start, end) {
  const filePath = await getFilePath(key);
  try {
    console.log("reading: ", filePath);
    const fileHandle = await fs.open(filePath, 'r');
    
    // Check for DuckDB magic header
    const headerBuffer = Buffer.alloc(MAGIC_HEADER_LENGTH);
    await fileHandle.read(headerBuffer, 0, MAGIC_HEADER_LENGTH, 0);
    const hasMagicHeader = headerBuffer.toString('utf8', 0, 4) === DUCKDB_MAGIC_HEADER;
    
    if (start !== undefined && end !== undefined) {
      // Adjust start and end if magic header is present
      if (hasMagicHeader) {
        start += MAGIC_HEADER_LENGTH;
        end += MAGIC_HEADER_LENGTH;
      }
      const buffer = Buffer.alloc(end - start + 1);
      await fileHandle.read(buffer, 0, end - start + 1, start);
      await fileHandle.close();
      return buffer;
    } else {
      let data = await fileHandle.readFile();
      await fileHandle.close();
      // Remove magic header if present
      if (hasMagicHeader) {
        data = data.slice(MAGIC_HEADER_LENGTH);
      }
      return data;
    }
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function writeFile(key, data) {
  const filePath = await getFilePath(key);
  await fs.writeFile(filePath, data);
}

// Helper function to handle range requests
async function handleRangeRequest(request, reply, key) {
  const filePath = await getFilePath(key);
  
  try {
    const stats = await fs.stat(filePath);
    let fileSize = stats.size;
    
    // Check for DuckDB magic header
    const fileHandle = await fs.open(filePath, 'r');
    const headerBuffer = Buffer.alloc(MAGIC_HEADER_LENGTH);
    await fileHandle.read(headerBuffer, 0, MAGIC_HEADER_LENGTH, 0);
    await fileHandle.close();
    const hasMagicHeader = headerBuffer.toString('utf8', 0, 4) === DUCKDB_MAGIC_HEADER;
    
    // Adjust fileSize if magic header is present
    if (hasMagicHeader) {
      fileSize -= MAGIC_HEADER_LENGTH;
    }
    
    const rangeHeader = request.headers.range;
    if (rangeHeader) {
      const parts = rangeHeader.replace(/bytes=/, "").split("-");
      let start = parts[0] ? parseInt(parts[0], 10) : 0;
      let end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
      
      if (isNaN(start) || isNaN(end) || start >= fileSize || end >= fileSize || start > end) {
        reply.code(416).send({ error: "Range Not Satisfiable" });
        return null;
      }
      
      const chunkSize = (end - start) + 1;
      reply.header('Content-Range', `bytes ${start}-${end}/${fileSize}`);
      reply.header('Accept-Ranges', 'bytes');
      reply.header('Content-Length', chunkSize);
      reply.code(206);
      return await readFile(key, start, end);
    } else {
      reply.header('Content-Length', fileSize);
      reply.header('Accept-Ranges', 'bytes');
      return await readFile(key);
    }
  } catch (error) {
    if (error.code === "ENOENT") {
      reply.code(404).send({ error: "Not found" });
      return null;
    }
    throw error;
  }
}

/** GET file */
fastify.get("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send();
  const data = await handleRangeRequest(request, reply, key);
  if (data === null) {
    return; // 404 already sent in handleRangeRequest
  }
  reply.send(data);
});

/** POST file */
fastify.post("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send({ error: "Key is required" });
  
  const data = await request.body;
  await writeFile(key, data);
  return { success: true };
});

// Custom parser for binary data
fastify.addContentTypeParser('application/octet-stream', { parseAs: 'buffer' }, (req, body, done) => {
  done(null, body);
});

/** RUN Server */
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