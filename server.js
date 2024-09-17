import fastify from 'fastify';
import { save, load } from './pastila.js'; // Import your functions here

// Handle GET requests
fastify.get("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send({ error: "Key is required" });

  try {
    // Use the load function to get the data
    const result = await load(key, key); // Adjust parameters if necessary
    return { content: result };
  } catch (error) {
    return reply.code(404).send({ error: "Not found" });
  }
});

// Handle POST requests
fastify.post("/:key", async (request, reply) => {
  const { key } = request.params;
  if (!key) return reply.code(400).send({ error: "Key is required" });

  try {
    const data = Array.isArray(request.body) ? request.body : [request.body];
    if (data.length === 0) return reply.code(400).send({ error: "No data provided" });

    // Use the save function to save the data
    const savedId = await save(data, key, key, false); // Adjust parameters if necessary
    return { success: true, id: savedId };
  } catch (error) {
    return reply.code(500).send({ error: "Error saving data" });
  }
});

// Helper function to parse application/octet-stream
async function getRawBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.raw.on("data", (chunk) => chunks.push(chunk));
    req.raw.on("end", () => resolve(Buffer.concat(chunks)));
    req.raw.on("error", reject);
  });
}

async function octetStreamParser(req) {
  try {
    const buffer = await getRawBody(req);
    const jsonString = buffer.toString("utf8");
    if (jsonString.trim().length === 0) {
      return [];
    }
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

// Start the Fastify server
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
