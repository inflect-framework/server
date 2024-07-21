const express = require("express");
const cors = require("cors");
const bodyParser = require("body-parser");
const db = require("./db");
const generateTestEvent = require("./utils/generateTestEvent");
const applyProcessors = require("./utils/applyProcessors");
const getAndInsertTopicsAndSchemas = require("./utils/getAndInsertTopicsAndSchemas");
const getSchemaByName = require("./utils/getSchema");

const app = express();
app.use(cors());
app.use(express.static("../client/public"));
app.use(bodyParser.json());

const port = process.env.SERVER_PORT || 3010;

app.get("/", (req, res) => {
  res.status(200).send("public/index.html");
  getAndInsertTopicsAndSchemas();
});

const createPipelineQuery = `
WITH source AS (
  SELECT id FROM topics WHERE topic_name = $1
),
target AS (
  SELECT id FROM topics WHERE topic_name = $2
),
incoming_schema AS (
  SELECT id FROM schemas WHERE schema_name = $3
),
outgoing_schema AS (
  SELECT id FROM schemas WHERE schema_name = $4
)
INSERT INTO pipelines (name, source_topic_id, target_topic_id, incoming_schema_id, outgoing_schema_id, steps, is_active)
SELECT $5, source.id, target.id, incoming_schema.id, outgoing_schema.id, $6::jsonb, true
FROM source, target, incoming_schema, outgoing_schema
RETURNING id;
`;

app.post("/create_pipeline", async (req, res) => {
  const body = req.body;
  const outgoingSchema = body.outgoingSchema.name;
  const processors = body.steps.map((obj) => obj.id);
  let dlqs = Array(body.steps.length).fill(null);
  dlqs.push(body.outgoingSchema.redirectTopic);
  const { name, sourceTopic, targetTopic, incomingSchema } = body;

  const steps = { processors: processors, dlqs: dlqs };

  try {
    const params = [
      sourceTopic,
      targetTopic,
      incomingSchema,
      outgoingSchema,
      name,
      JSON.stringify(steps),
    ];
    const result = await db.query(createPipelineQuery, params);
    res.status(200).send(result.rows[0]);
  } catch (error) {
    console.log(error);
    res.status(500).send(error);
  }
});

const updateActiveStateQuery = `
UPDATE pipelines
SET is_active = $1
WHERE id = $2;
`;

app.put("/pipeline/:id", async (req, res) => {
  const newActiveState = req.body.isActive;
  const pipelineId = +req.params.id;
  try {
    await db.query(updateActiveStateQuery, [newActiveState, pipelineId]);
    res.status(200).send({ success: true });
  } catch (error) {
    res.status(500).send(error);
  }
});

const getPipelinesQuery = `
SELECT 
    p.name, 
    st.topic_name as source_topic, 
    tt.topic_name as target_topic,
    is_active,
    p.id,
    p.steps,
    ischema.schema_name as incoming_schema,
    oschema.schema_name as outgoing_schema
FROM 
    pipelines p
JOIN 
    topics st ON p.source_topic_id = st.id
JOIN 
    topics tt ON p.target_topic_id = tt.id
JOIN 
    schemas ischema ON p.incoming_schema_id = ischema.id
JOIN 
    schemas oschema ON p.outgoing_schema_id = oschema.id;
`;

app.get("/pipelines", async (req, res) => {
  try {
    const allPipelines = await db.query(getPipelinesQuery);
    res.status(200).send(allPipelines.rows);
    getAndInsertTopicsAndSchemas();
  } catch (error) {
    console.error("Error getting pipelines:", error);
    res.status(500).send({
      message: "Failed to get pipelines",
      error: error.message,
      stack: error.stack,
    });
  }
});

const getTopicsQuery = `
SELECT topic_name FROM topics;
`;

const getSchemasQuery = `
SELECT schema_name FROM schemas;
`;

app.get("/topics_schemas", async (req, res) => {
  try {
    const topics = await db.query(getTopicsQuery);
    const schemas = await db.query(getSchemasQuery);
    const topicsArray = topics.rows.map((row) => row.topic_name);
    const schemasArray = schemas.rows.map((row) => row.schema_name);
    const result = { topics: topicsArray, schemas: schemasArray };
    res.status(200).send(result);
  } catch (error) {
    res.status(500, error);
  }
});

app.post("/test_event", async (req, res) => {
  const schema = req.body.schema;
  const format = req.body.format;
  const registrySchema = await getSchemaByName(schema);
  try {
    const event = await generateTestEvent(format, registrySchema);
    res.status(200).send(event);
  } catch (error) {
    console.error(error);
    res.status(500).send(error);
  }
});

app.post("/test_pipeline", async (req, res) => {
  const format = req.body.format;
  const steps = req.body.steps.map((obj) => obj.processor_name);
  const event = JSON.parse(req.body.event);

  if (!event || !steps || !format) {
    return res.status(400).send({
      error: "Invalid input. Event and steps with processors are required.",
    });
  }

  try {
    const transformedMessage = await applyProcessors(event, steps);

    res.status(200).send({
      status: "success",
      transformedMessage,
    });
  } catch (error) {
    console.error("Error processing event:", error);
    res.status(500).send({ error: "Failed to process event." });
  }
});

const getProcessorsQuery = `
SELECT * from processors;
`;
app.get("/processors", async (req, res) => {
  try {
    const result = await db.query(getProcessorsQuery);
    res.status(200).send(result.rows);
  } catch (error) {
    console.error(error);
    res.status(500).send(error);
  }
});

app.listen(port, () => {
  console.log(`listening on port ${port}`);
});
