const axios = require("axios");
require("dotenv").config();

const registryUrl = process.env.REGISTRY_URL;
const registryAuth = {
  username: process.env.REGISTRY_APIKEY,
  password: process.env.REGISTRY_APISECRET,
};

async function getSchemaByName(schemaName) {
  try {
    const subjectsResponse = await axios.get(
      `${registryUrl}/subjects/${schemaName}/versions/latest`,
      {
        auth: registryAuth,
      }
    );

    return JSON.parse(subjectsResponse.data.schema).properties;
  } catch (error) {
    console.error("Error getting schema", error);
  }
}

module.exports = getSchemaByName;
