const express = require('express')
const cors = require('cors')
// const consumer = require('../stream-processor/consumer.js')
const bodyParser = require('body-parser')
const axios = require('axios')
const processorURL = null
// const fs = require('fs')
// const path = require('path')

// fs.readdir('./transformations', (err, files) => {
//   files.map(file => {
    
//   })
// })

const app = express()
app.use(cors());
app.use(express.static('../client/public'));
// app.use(express.json())
app.use(bodyParser.json())

const port = 3000;

app.get('/', (req, res) => {
  res.send('public/index.html')
});

app.post('/create_transformation', async (req, res) => {
  const body = req.body
  const [sourceTopic, targetTopic] = [body.sourceTopic, body.targetTopic];
  const transformation = 'capitalize'
  try {
    const request = await axios.post('http://localhost:4000/createTransformation', {sourceTopic, targetTopic, transformation})
    res.send(200, request.body)
  } catch (error) {
    res.send(500, error)
  }
  // consumer(sourceTopic, targetTopic, transformation);
});

app.listen(port, () => {
  console.log(`listening on port ${port}`)
})