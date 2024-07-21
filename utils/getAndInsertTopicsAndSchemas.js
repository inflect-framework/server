const { Kafka } = require("kafkajs");
const axios = require("axios");
require("dotenv").config();
const db = require("../db");

const kafka = new Kafka({
  clientId: "inflect-client",
  brokers: [process.env.BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.APIKEY,
    password: process.env.APISECRET,
  },
});

const registryUrl = process.env.REGISTRY_URL;
const registryAuth = {
  username: process.env.REGISTRY_APIKEY,
  password: process.env.REGISTRY_APISECRET,
};

const insertTopic = async (topicName) => {
  const client = await db.getClient();
  const query = `
    INSERT INTO topics (topic_name)
    VALUES ($1)
    ON CONFLICT (topic_name) DO NOTHING;
  `;
  await client.query(query, [topicName]);
};

const insertSchema = async (schemaName) => {
  const client = await db.getClient();
  const query = `
    INSERT INTO schemas (schema_name)
    VALUES ($1)
    ON CONFLICT (schema_name) DO NOTHING;
  `;
  await client.query(query, [schemaName]);
};

async function getAndInsertTopicsAndSchemas() {
  const client = await db.getClient();

  try {
    await client.connect();

    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();
    await admin.disconnect();

    for (const topic of topics) {
      await insertTopic(topic);
    }
    console.log("Kafka Topics:", topics);

    const subjectsResponse = await axios.get(`${registryUrl}/subjects`, {
      auth: registryAuth,
    });
    const subjects = subjectsResponse.data;

    for (const subject of subjects) {
      await insertSchema(subject);
    }
    console.log("Schema Registry Subjects:", subjects);

    console.log("Topics and schemas inserted successfully");
  } catch (error) {
    console.error("Error listing topics and schemas:", error);
  } finally {
    client.release();
  }
}

module.exports = getAndInsertTopicsAndSchemas;
