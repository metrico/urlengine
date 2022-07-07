/** CLICKHOUSE URL Table Engine handler */
/** BONUS: Stateful saves in deta */

const fastify = require("fastify")({ logger: true });
var memory = []; // our fake memory storage

const { Deta } = require("deta");
const deta = Deta(process.env.DETA_TOKEN || false);
const db = deta.Base("shared");

/** CLICKHOUSE URL SELECT */
fastify.get("/", async (request, reply) => {
  const { items } = await db.fetch();
  return items;
  // return memory;
});

/** Get state from Deta */
fastify.get("/:detabase", async (request, reply) => {
  const { detabase } = request.params || "shared";
  const db = deta.Base(detabase);
  const { items } = await db.fetch();
  return items;
});

/** CLICKHOUSE URL INSERT */
fastify.post("/", async (request, reply) => {
  request.body.forEach((row) => {
    // memory.push({ key: row.key, value: parseInt(row.value) });
    db.put({ key: row.key, value: parseInt(row.value) });
  });
  return {};
});

fastify.post("/memory", async (request, reply) => {
  request.body.forEach((row) => {
    memory.push({ key: row.key, value: parseInt(row.value) });
  });
  return {};
});

/** Save state in Deta **/
fastify.post("/:detabase", async (request, reply) => {
  const { detabase } = request.params || "shared";
  const db = deta.Base(detabase);
  request.body.forEach((row) => {
    db.put({ key: row.key, value: parseInt(row.value) });
  });
  return {};
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
    await fastify.listen(3000);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};
start();
