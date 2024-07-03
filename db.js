const { Pool } = require('pg');
const dotenv = require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

pool.on('connect', () => {
  console.log('Connected to the database');
});

module.exports = {
  query: (text, params) => pool.query(text, params),
  getClient: () => pool.connect()
};