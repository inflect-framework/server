const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const db = require('./db');
const generateTestEvent = require('./utils/generateTestEvent');
const applyProcessors = require('./utils/applyProcessors');
const getAndInsertTopicsAndSchemas = require('./utils/getAndInsertTopicsAndSchemas');
const getSchemaByName = require('./utils/getSchema');
const e = require('express');

const app = express();
app.use(cors());
app.use(express.static('../client/public'));
app.use(bodyParser.json());

const port = process.env.SERVER_PORT || 3010;

app.get('/', (req, res) => {
  res.status(200).send('public/index.html');
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

app.post('/create_pipeline', async (req, res) => {
  // const body = req.body;
  // const outgoingSchema = body.outgoingSchema;
  const processors = req.body.steps.processors;
  const dlqs = req.body.steps.dlqs || [];
  // dlqs.push(body.outgoingSchema.redirectTopic);
  const { 
    name, 
    source_topic, 
    target_topic, 
    incoming_schema, 
    outgoing_schema 
  } = req.body;

  const steps = { processors: processors, dlqs: dlqs };
  const client = await db.getClient();
  try {
    const params = [
      source_topic,
      target_topic,
      incoming_schema,
      outgoing_schema,
      name,
      JSON.stringify(steps),
    ];
    const result = await client.query(createPipelineQuery, params);
    res.status(200).send(result.rows[0]);
  } catch (error) {
    console.error(error);
    res.status(500).send(error);
  } finally {
    client.release();
  }
});

const updatePipelinesQuery = `
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
UPDATE pipelines
SET 
  name = $5, 
  source_topic_id = source.id, 
  target_topic_id = target.id, 
  incoming_schema_id = incoming_schema.id, 
  outgoing_schema_id = outgoing_schema.id, 
  steps = $6::jsonb, 
  is_active = $7
FROM 
  source, target, incoming_schema, outgoing_schema
WHERE 
  pipelines.id = $8;
`;

app.put('/pipeline', async (req, res) => {
  const pipelineId = req.body.id;
  const pipelineName = req.body.name;
  const sourceTopicName = req.body.source_topic;
  const targetTopicName = req.body.target_topic;
  const incomingSchema = req.body.incoming_schema;
  const outgoingSchema = req.body.outgoing_schema;
  const newActiveState = req.body.is_active;
  const steps = req.body.steps;
  const redirectTopic = req.body.redirect_topic;

  if (!steps.hasOwnProperty('dlqs')) {
    steps.dlqs = Array(steps.processors.length + 1).fill(null);
    steps.dlqs[steps.dlqs.length - 1] = redirectTopic;
  } else if (steps.dlqs.length !== steps.processors.length + 1) {
    steps.dlqs = [...steps.dlqs, redirectTopic];
  } else if (steps.dlqs[steps.dlqs.length - 1] !== redirectTopic) {
    steps.dlqs[steps.dlqs.length - 1] = redirectTopic;
  }

  const client = await db.getClient();
  try {
    await client.query(updatePipelinesQuery, [
      sourceTopicName,
      targetTopicName,
      incomingSchema,
      outgoingSchema,
      pipelineName,
      steps,
      newActiveState,
      pipelineId,
    ]);
    res.status(200).send({ success: true });
  } catch (error) {
    console.error('Error updating pipeline:', error);
    res.status(500).send(error);
  } finally {
    client.release();
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

app.get('/pipelines', async (req, res) => {
  const client = await db.getClient();
  try {
    const allPipelines = await client.query(getPipelinesQuery);
    res.status(200).send(allPipelines.rows);
    getAndInsertTopicsAndSchemas();
  } catch (error) {
    console.error('Error getting pipelines:', error);
    res.status(500).send({
      message: 'Failed to get pipelines',
      error: error.message,
      stack: error.stack,
    });
  } finally {
    client.release();
  }
});

const getTopicsQuery = `
SELECT topic_name FROM topics;
`;

const getSchemasQuery = `
SELECT schema_name FROM schemas;
`;

app.get('/topics_schemas', async (req, res) => {
  const client = await db.getClient();
  try {
    const topics = await client.query(getTopicsQuery);
    const schemas = await client.query(getSchemasQuery);
    const topicsArray = topics.rows.map((row) => row.topic_name);
    const schemasArray = schemas.rows.map((row) => row.schema_name);
    const result = { topics: topicsArray, schemas: schemasArray };
    res.status(200).send(result);
  } catch (error) {
    res.status(500, error);
  } finally {
    client.release();
  }
});

app.post('/test_event', async (req, res) => {
  const schema = req.body.schema;
  const format = req.body.format;
  const registrySchema = await getSchemaByName(schema);
  const protoName = registrySchema.hasOwnProperty('syntax') ? registrySchema.root : null;
  try {
    if (registrySchema === undefined) throw new Error('Schema not returned from registry');
    const event = await generateTestEvent(format, registrySchema, protoName);
    res.status(200).send(event);
  } catch (error) {
    console.error(error);
    res.status(500).send(error);
  }
});

app.post('/test_pipeline', async (req, res) => {
  const { format, steps, dlqs } = req.body;
  const event = JSON.parse(req.body.event);

  if (!event || !steps || !format || !dlqs) {
    return res.status(400).send({
      error: 'Invalid input. Event and steps with processors are required.',
    });
  }

  try {
    const transformedMessage = await applyProcessors(event, steps, dlqs);

    res.status(200).send({
      status: 'success',
      transformedMessage,
    });
  } catch (error) {
    console.error('Error processing event:', error);
    res.status(500).send({ error: 'Failed to process event.' });
  }
});

const getProcessorsQuery = `
SELECT * from processors;
`;
app.get('/processors', async (req, res) => {
  const client = await db.getClient();
  try {
    const result = await client.query(getProcessorsQuery);
    res.status(200).send(result.rows);
  } catch (error) {
    console.error(error);
    res.status(500).send(error);
  } finally {
    client.release();
  }
});

app.listen(port, () => {
  console.log(`listening on port ${port}`);
});
