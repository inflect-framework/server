const express = require('express')
const cors = require('cors')
const bodyParser = require('body-parser')
const axios = require('axios')
const processorURL = null
const db = require('./db')

const app = express()
app.use(cors());
app.use(express.static('../client/public'));
// app.use(express.json())
app.use(bodyParser.json())

const port = 3000;

app.get('/', (req, res) => {
  res.send('public/index.html')
});

const createConnectionQuery = `
WITH source AS (
  SELECT id FROM sources WHERE source_topic = $1
),
target AS (
  SELECT id FROM targets WHERE target_topic = $2
),
transformation AS (
  SELECT id FROM transformations WHERE transformation_name = $3
)
INSERT INTO connections (source_id, target_id, transformation_id, active_state)
SELECT source.id, target.id, transformation.id, true
FROM source, target, transformation
RETURNING id;
`

app.post('/create_transformation', async (req, res) => {
  const body = req.body
  const { sourceTopic, targetTopic, transformation } = body
  try {
    const params = [sourceTopic, targetTopic, transformation]
    const result = await db.query(createConnectionQuery, params)
    res.status(200).send(result.rows.at(0))
  } catch (error) {
    console.log(error)
    res.send(500, error)
  }
  // consumer(sourceTopic, targetTopic, transformation);
});

const updateActiveStateQuery = `
UPDATE connections
SET active_state = $1
WHERE connections.id = $2;
`

app.put('/connection/:id', async (req, res) => {
  const newActiveState = req.body.connectionActiveState
  const connectionId = +req.params.id
  try {
    await db.query(updateActiveStateQuery, [newActiveState, connectionId])
    res.send(200, {success: true});
  } catch (error) {
    res.status(500, error)
  }
})

const getConnectionsQuery = `
SELECT 
    tr.transformation_name, 
    s.source_topic, 
    t.target_topic,
    active_state,
    c.id
FROM 
    connections c
JOIN 
    sources s ON c.source_id = s.id
JOIN 
    targets t ON c.target_id = t.id
JOIN 
    transformations tr ON c.transformation_id = tr.id;
`

app.get('/connections', async (req, res) => {
  const allConnections = await db.query(getConnectionsQuery)
  res.send(200, allConnections.rows);
})

app.listen(port, () => {
  console.log(`listening on port ${port}`)
})