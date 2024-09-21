const fastify = require("fastify")({ logger: true });
const fs = require("fs").promises;
const path = require("path");

const DB_DIR = path.join(__dirname, "/tmp/db");

// Ensure storage directory exists
fs.mkdir(DB_DIR, { recursive: true }).catch(console.error);

async function getFilePath(key) {
  return path.join(DB_DIR, `${key}`);
}

async function readFile(key, start, end) {
  const filePath = await getFilePath(key);
  try {
    const fileHandle = await fs.open(filePath, 'r');

    if (start !== undefined && end !== undefined) {
      const buffer = Buffer.alloc(end - start + 1);
      await fileHandle.read(buffer, 0, end - start + 1, start);
      await fileHandle.close();
      return buffer;
    } else {
      const data = await fileHandle.readFile();
      await fileHandle.close();
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
  await fs.writeFile(filePath, data, 'binary'); // Ensure binary write

  // Log the file size and verify contents
  const stats = await fs.stat(filePath);
  console.log(`File saved: ${filePath}, Size: ${stats.size} bytes`);

  // Read first few bytes to check for DuckDB magic header
  const buffer = await readFile(key, 0, 7); // Read first 8 bytes for the magic header
  console.log('File header:', buffer.toString('utf-8')); // Should start with "DUCKDB"
}

// Helper function to handle range requests
async function handleRangeRequest(request, reply, key) {
  const filePath = await getFilePath(key);

  try {
    const stats = await fs.stat(filePath);
    const fileSize = stats.size;
    const rangeHeader = request.headers.range;

    if (rangeHeader) {
      const parts = rangeHeader.replace(/bytes=/, "").split("-");
      const start = parts[0] ? parseInt(parts[0], 10) : 0;
      const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;

      // Validate the range
      if (isNaN(start) || isNaN(end) || start >= fileSize || end >= fileSize || start > end) {
        reply.header('Content-Range', `bytes */${fileSize}`);
        reply.code(416).send({ error: "Range Not Satisfiable" });
        return null;
      }

      const chunkSize = (end - start) + 1;
      reply.header('Content-Range', `bytes ${start}-${end}/${fileSize}`);
      reply.header('Accept-Ranges', 'bytes');
      reply.header('Content-Length', chunkSize);
      reply.code(206); // Partial content

      // Read file content with the range
      return await readFile(key, start, end);
    } else {
      // Full file request
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

// GET file
fastify.get("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send();
  
  const data = await handleRangeRequest(request, reply, key);
  if (data === null) {
    return; // 404 or 416 already sent in handleRangeRequest
  }
  
  reply.send(data);
});

// POST file (used for COPY INTO operation)
fastify.post("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send({ error: "Key is required" });

  const data = await request.body;
  await writeFile(key, data);
  return { success: true };
});

// Custom parser for binary data (required for handling DuckDB file uploads)
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
